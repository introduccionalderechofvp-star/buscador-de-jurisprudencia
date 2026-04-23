#!/usr/bin/env node
/**
 * Scraper de la Corte Suprema de Justicia de Colombia.
 *
 * Consume la API GraphQL pública de consulta de providencias:
 *   POST https://consultaprovidenciasbk.cortesuprema.gov.co/api
 *   POST https://consultaprovidenciasbk.cortesuprema.gov.co/downloadFile
 *
 * Dos modos de invocación:
 *
 *   1. Incremental (default, pensado para cron diario):
 *        node scrapers/corte-suprema.js
 *      Itera las 4 salas (Civil, Laboral, Penal, Tutelas), pagina desde la
 *      primera página en orden NEW_FIRST y se DETIENE apenas encuentra una
 *      sentencia ya descargada (el "early stop"). Como la API ordena por más
 *      nuevo primero, encontrar una ya existente significa que todo lo
 *      posterior también está descargado.
 *
 *   2. Ad-hoc (equivalente al .bat interactivo original):
 *        node scrapers/corte-suprema.js --ad-hoc \
 *             --sala Civil --query "laudo arbitral" --ano 2025 --max 78
 *      Descarga exactamente los N documentos que matchean los filtros, sin
 *      early stop. Útil para investigación dirigida.
 *
 * Flags:
 *   --sala <Civil|Laboral|Penal|Tutelas>   default: iterar las 4
 *   --query <string>                        default: "a" (comodín, trae todo)
 *   --ano <YYYY>                            default: "" (todos los años)
 *   --magistrado <nombre>                   default: "" (todos)
 *   --max <N>                               default: 10000 (ilimitado práctico)
 *   --ad-hoc                                default: false (modo incremental)
 *   --no-abort                              default: false — desactiva el
 *                                           circuit breaker por sala (sigue
 *                                           paginando aun con 20+ errores
 *                                           consecutivos). Uso diagnóstico
 *                                           para explorar salas problemáticas.
 *
 * Salida:
 *   - PDFs guardados en uploads/Sala <sala> - Corte Suprema de Justicia/<año>/
 *   - Estado persistido en scrapers/state/corte-suprema-<sala>.json
 *   - Errores en scrapers/errors/<YYYY-MM-DD>.jsonl
 *   - Resumen final por sala y totales a stdout
 */

import 'dotenv/config';
import { parseArgs } from 'node:util';
import { pathToFileURL } from 'node:url';

import { fetchJSON, fetchBuffer, getBudgetStatus } from './lib/http.js';
import { saveAtomic, cleanupTmp, safeName, buildBasenameIndex } from './lib/storage.js';
import { readState, writeState, acquireLock, releaseLock } from './lib/state.js';
import { logError } from './lib/errors.js';
import { convertDocxBufferToPdf } from './lib/docx.js';

// ─── Constantes del API ───────────────────────────────────────────────────────

const API_SEARCH   = 'https://consultaprovidenciasbk.cortesuprema.gov.co/api';
const API_DOWNLOAD = 'https://consultaprovidenciasbk.cortesuprema.gov.co/downloadFile';

const SALAS_DEFAULT = ['Civil', 'Laboral', 'Penal', 'Tutelas'];
const RESULTS_PER_PAGE = 10;                              // fijo en el API
const DELAY_BETWEEN_REQUESTS_MS = 400;                    // cortesía con la API

// ─── Utilidades ───────────────────────────────────────────────────────────────

function organoName(sala) {
  return `Sala ${sala} - Corte Suprema de Justicia`;
}

function stateName(sala) {
  return `corte-suprema-${sala.toLowerCase()}`;
}

function sleep(ms) {
  return new Promise(r => setTimeout(r, ms));
}

function buildQuery({ query, sala, start, magistrado, ano }) {
  // Mismos parámetros que usaba el script Python original.
  // Escapamos las comillas dobles en los valores para que no rompan el GraphQL.
  const esc = s => String(s ?? '').replace(/"/g, '\\"');
  return `{
  getSearchResult(searchQuery:{
    query: "${esc(query)}"
    typeOfQuery: "${esc(sala)}"
    start: ${Number(start)}
    isExact: false
    magistrate: "${esc(magistrado)}"
    year: "${esc(ano)}"
    autoSentencia: "SENTENCIA"
    order: "NEW_FIRST"
    roomTutelas: ""
    addedQueries: []
  })
  {
    searchResults {
      title
      onlinePath
      doctor
      ano
    }
    numOfResults
  }
}`;
}

async function searchPage({ query, sala, start, magistrado, ano }) {
  const data = await fetchJSON(API_SEARCH, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ query: buildQuery({ query, sala, start, magistrado, ano }) })
  });

  const result = data?.data?.getSearchResult;
  if (!result || !Array.isArray(result.searchResults)) {
    throw new Error(`Respuesta del API malformada: ${JSON.stringify(data).slice(0, 200)}`);
  }
  return result;
}

async function downloadFile(onlinePath) {
  return fetchBuffer(API_DOWNLOAD, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ path: onlinePath })
  }, { timeoutMs: 60_000 });   // archivos pueden ser varios MB, tolerancia mayor
}

/**
 * Convierte un onlinePath anidado por magistrado a su forma plana (sala+año).
 * Ejemplo:
 *   IN:  /var/www/html/Index/PENAL/2025/Dr. X/Sentencia/SP1148.docx
 *   OUT: /var/www/html/Index/PENAL/2025/SP1148.docx
 *
 * Si el path ya está plano (≤8 segmentos), lo devuelve sin cambios. Robusto:
 * no depende de conocer la sala — usa los primeros 7 segmentos estructurales
 * (/var/www/html/Index/<SALA>/<año>) + el filename final.
 */
function flattenOnlinePath(onlinePath) {
  const parts = onlinePath.split('/');
  if (parts.length <= 8) return onlinePath;
  const prefix = parts.slice(0, 7).join('/');
  const filename = parts[parts.length - 1];
  return `${prefix}/${filename}`;
}

/**
 * Wrapper de downloadFile con fallback entre path anidado y plano.
 *
 * Primero intenta el onlinePath tal cual (como lo devuelve el GraphQL de la
 * CSJ). Si falla, reintenta con la versión plana construida sobre la marcha.
 *
 * Contexto: la CSJ sirve Civil/Laboral/Tutelas con path anidado por magistrado
 * (funcionan con el onlinePath tal cual), pero Penal solo responde con el
 * path plano sin magistrado. El fallback maneja ambos casos sin que el resto
 * del código tenga que saber qué sala está procesando.
 *
 * Si el onlinePath ya estaba plano (sin subcarpetas anidadas), no hay
 * alternativa que intentar y se propaga el error original sin gastar
 * una segunda request.
 */
async function downloadWithPathFallback(onlinePath) {
  let firstError;
  try {
    return await downloadFile(onlinePath);
  } catch (e) {
    firstError = e;
  }
  const flat = flattenOnlinePath(onlinePath);
  if (flat === onlinePath) throw firstError;
  console.log(`    [path-fallback] nested falló, usando plano`);
  try {
    return await downloadFile(flat);
  } catch (secondError) {
    throw new Error(`nested: ${firstError.message} | flat: ${secondError.message}`);
  }
}

// Extensiones que sabemos procesar. Para cualquier otra, loggeamos y saltamos.
const SUPPORTED_EXTS = new Set(['pdf', 'docx', 'doc']);

// Orden de preferencia: PDF nativo primero (cero conversión), luego DOCX, luego
// DOC. Si un formato falla (404, error de conversión, etc.), probamos el siguiente.
const FORMAT_PRIORITY = ['pdf', 'docx', 'doc'];

// Umbral de errores consecutivos para abortar una sala. Protege contra escenarios
// como Sala Penal, donde la API lista PDFs que devuelven 404 sistemáticamente.
const CONSECUTIVE_ERROR_LIMIT = 20;

function extractExt(onlinePath) {
  const m = onlinePath?.match(/\.([a-z0-9]+)$/i);
  return m ? m[1].toLowerCase() : null;
}

function extractBasename(doc) {
  // Priorizar title, fallback al basename del onlinePath
  const raw = doc.title || doc.onlinePath?.split('/').pop() || '';
  return raw.replace(/\.(pdf|docx?)$/i, '').trim();
}

/**
 * Agrupa los resultados de una página por basename (sin extensión), indexando
 * por formato disponible. Permite tratar una sentencia publicada en varios
 * formatos como UN solo candidato con múltiples opciones de descarga.
 *
 * @returns {{ groups: Map<string, {pdf?, docx?, doc?, año}>, unsupported: number }}
 */
function groupResultsByBasename(searchResults) {
  const groups = new Map();
  let unsupported = 0;

  for (const doc of searchResults) {
    const ext = extractExt(doc.onlinePath);
    if (!ext || !SUPPORTED_EXTS.has(ext)) {
      const shortTitle = (doc.title || doc.onlinePath || '?').slice(0, 60);
      console.log(`    [skip .${ext || '??'}] ${shortTitle}`);
      unsupported++;
      continue;
    }

    const base = extractBasename(doc);
    if (!base) continue;

    // Usamos el basename lowercased como clave, para comparación
    // case-insensitive con el índice del disco
    const key = base.toLowerCase();
    if (!groups.has(key)) {
      groups.set(key, { base, año: String(doc.ano || new Date().getFullYear()) });
    }
    // Preferir la primera ocurrencia si el API devuelve duplicados del mismo formato
    const slot = groups.get(key);
    if (!slot[ext]) slot[ext] = doc;
  }

  return { groups, unsupported };
}

/**
 * Intenta procesar una entrada agrupada: descarga en orden de preferencia
 * (pdf → docx → doc), convierte si es necesario, y guarda como <base>.pdf.
 *
 * Estrategia ante fallos:
 *   - 404 en un formato → intenta el siguiente formato disponible
 *   - Otro error en descarga o conversión → loguea y prueba siguiente formato
 *   - Si TODOS los formatos fallan, retorna { success: false }
 *
 * @returns {Promise<{success, formatUsed?, converted?, pdfSize?, errorMessage?}>}
 */
async function processGroup({ groupKey, group, organo, stateName: scraperName }) {
  const { base, año } = group;
  const errorsPerFormat = [];

  for (const fmt of FORMAT_PRIORITY) {
    if (!group[fmt]) continue;

    try {
      const buffer = await downloadWithPathFallback(group[fmt].onlinePath);
      if (!buffer || buffer.length === 0) {
        throw new Error('buffer vacío');
      }

      let pdfBuffer;
      let converted = false;
      if (fmt === 'pdf') {
        pdfBuffer = buffer;
      } else {
        // .docx o .doc → convertir a PDF con LibreOffice
        pdfBuffer = await convertDocxBufferToPdf(buffer, fmt);
        converted = true;
      }

      const finalFilename = safeName(base + '.pdf');
      saveAtomic(organo, año, finalFilename, pdfBuffer);
      return {
        success: true,
        formatUsed: fmt,
        converted,
        pdfSize: pdfBuffer.length,
        filename: finalFilename,
        año
      };
    } catch (e) {
      errorsPerFormat.push({ fmt, error: e.message });
      logError(scraperName, {
        phase: fmt === 'pdf' ? 'download' : 'download+convert',
        doc_title: group[fmt].title,
        doc_path:  group[fmt].onlinePath,
        doc_ano:   group[fmt].ano,
        format:    fmt,
        error:     e.message
      });
      // Si el error es 404 y hay un formato alternativo, seguimos con él
      // silenciosamente. Para cualquier otro error, también reintentamos con
      // el siguiente formato (más defensivo que abortar).
      continue;
    }
  }

  // ── Fallback especulativo a .doc ───────────────────────────────────────────
  //
  // Motivación: observación empírica (confirmada por el usuario con la UI de
  // la CSJ) de que algunas salas tienen sentencias accesibles por URL .doc
  // aunque la API solo liste .docx en búsquedas genéricas. En particular
  // Sala Penal: sus .docx listados devuelven 404, pero construir la URL con
  // extensión .doc en el mismo path sí funciona.
  //
  // .doc es el ÚLTIMO recurso — solo si pdf y docx (listados) fallaron, y si
  // .doc mismo no estaba listado. Esto respeta la preferencia: pdf primero,
  // docx si pdf falla, doc listado si docx falla, y .doc especulativo solo
  // si ni siquiera el .doc estaba listado y nada más funcionó.
  //
  // Se loggea con la etiqueta `doc*` (asterisco = fallback especulativo).
  if (!group.doc && (group.docx || group.pdf)) {
    const sourceDoc   = group.docx || group.pdf;
    const specPath    = sourceDoc.onlinePath.replace(/\.(pdf|docx)$/i, '.doc');

    try {
      const buffer = await downloadWithPathFallback(specPath);
      if (!buffer || buffer.length === 0) {
        throw new Error('buffer vacío');
      }
      const pdfBuffer = await convertDocxBufferToPdf(buffer, 'doc');
      const finalFilename = safeName(base + '.pdf');
      saveAtomic(organo, año, finalFilename, pdfBuffer);
      return {
        success: true,
        formatUsed: 'doc*',       // asterisco indica fallback especulativo
        converted: true,
        pdfSize: pdfBuffer.length,
        filename: finalFilename,
        año
      };
    } catch (e) {
      errorsPerFormat.push({ fmt: 'doc*', error: e.message });
      logError(scraperName, {
        phase:       'speculative-doc',
        doc_title:   sourceDoc.title,
        doc_path:    specPath,
        doc_ano:     sourceDoc.ano,
        format:      'doc*',
        error:       e.message
      });
    }
  }

  return {
    success: false,
    errors: errorsPerFormat,
    errorMessage: errorsPerFormat.map(e => `${e.fmt}:${e.error}`).join(' | ')
  };
}

// ─── Lógica principal por sala ────────────────────────────────────────────────

async function scrapeSala({ sala, query, ano, magistrado, max, incremental, noAbort = false }) {
  const organo = organoName(sala);
  const name   = stateName(sala);

  const lock = acquireLock(name);
  if (!lock.acquired) {
    console.error(`[${sala}] Ya hay otra instancia corriendo (PID ${lock.heldByPid}). Salto.`);
    return { sala, skipped: true, reason: 'lock_held' };
  }

  // Limpieza de .tmp huérfanos de corridas anteriores que murieron
  const tmpRemoved = cleanupTmp(organo);
  if (tmpRemoved > 0) {
    console.log(`[${sala}] Limpiados ${tmpRemoved} archivos .tmp huérfanos.`);
  }

  // Índice de basenames (sin extensión, lowercase) ya presentes en disco bajo
  // el organo — detecta .pdf, .docx y .doc. Permite dedup cross-format y
  // tolerante a estructuras legacy nested.
  //
  // CLAVE: este índice es un SNAPSHOT INMUTABLE del estado del disco al
  // arrancar la corrida. NO se actualiza durante el scraping. Sirve solo
  // para detectar sentencias PRE-EXISTENTES (que disparan early-stop).
  const existingBeforeRun = buildBasenameIndex(organo);
  console.log(`[${sala}] Índice de archivos existentes: ${existingBeforeRun.size} basenames ya en disco.`);

  // Set aparte para tracking de lo que descargamos en ESTA corrida. Dedup
  // contra duplicados devueltos por la API en la misma corrida, pero NO
  // dispara early-stop (los duplicados de la API no significan que "ya
  // alcanzamos el corpus anterior").
  const downloadedThisRun = new Set();
  const failedThisRun = new Set();

  const prevState = readState(name) || {
    sala,
    docs_historicos: 0,
    ultima_corrida_exitosa: null,
    ultima_corrida_descargados: 0
  };

  console.log(`\n─── ${organo} ───`);
  console.log(`  Modo: ${incremental ? 'incremental (early-stop al primer ya-existente)' : 'ad-hoc'}`);
  console.log(`  Query: "${query}" · Año: "${ano || 'todos'}" · Magistrado: "${magistrado || 'todos'}" · Max: ${max}`);
  if (prevState.ultima_corrida_exitosa) {
    console.log(`  Última corrida exitosa: ${prevState.ultima_corrida_exitosa} (${prevState.ultima_corrida_descargados} docs)`);
  }

  let start              = 0;
  let total              = Infinity;
  let downloaded         = 0;
  let alreadyExists      = 0;
  let errors             = 0;
  let unsupportedSkipped = 0;
  let dupInRun           = 0;
  let earlyStop          = false;
  let abortedBySala      = false;
  let consecutiveErrors  = 0;

  try {
    while (downloaded + alreadyExists < max && start < total && !earlyStop && !abortedBySala) {
      let page;
      try {
        page = await searchPage({ query, sala, start, magistrado, ano });
      } catch (e) {
        logError(name, {
          phase: 'search',
          page: start / RESULTS_PER_PAGE + 1,
          error: e.message
        });
        console.error(`  [search err] página ${start/RESULTS_PER_PAGE + 1}: ${e.message}`);
        break;   // si falla la búsqueda de una página, no tiene sentido seguir
      }

      total = page.numOfResults;
      const pageNum = start / RESULTS_PER_PAGE + 1;
      console.log(`  Página ${pageNum}: ${page.searchResults.length} resultados (total API: ${total})`);

      // Agrupa los resultados de la página por basename, unificando pdf/docx/doc
      const { groups, unsupported } = groupResultsByBasename(page.searchResults);
      unsupportedSkipped += unsupported;

      for (const [groupKey, group] of groups) {
        if (downloaded + alreadyExists >= max) break;

        // 1. ¿Ya existía antes de esta corrida? → dispara early-stop (incremental)
        if (existingBeforeRun.has(groupKey)) {
          if (!downloadedThisRun.has(groupKey)) {
            alreadyExists++;
            downloadedThisRun.add(groupKey);
          } else {
            dupInRun++;
            continue;
          }
          if (incremental) {
            console.log(`    [existe] ${group.base} → early-stop activado`);
            earlyStop = true;
            break;
          }
          console.log(`    [skip]   ya existe: ${group.base}`);
          continue;
        }

        // 2. ¿Ya lo descargamos o falló en ESTA misma corrida? → skip silencioso
        if (downloadedThisRun.has(groupKey) || failedThisRun.has(groupKey)) {
          dupInRun++;
          continue;
        }

        // 3. Intentar descargar/convertir en orden de preferencia
        const result = await processGroup({
          groupKey,
          group,
          organo,
          stateName: name
        });

        if (result.success) {
          downloaded++;
          downloadedThisRun.add(groupKey);
          consecutiveErrors = 0;   // éxito rompe la racha

          const label = result.converted
            ? `[ok ${result.formatUsed}→pdf]`
            : `[ok ${result.formatUsed}]`;
          console.log(`    ${label.padEnd(15)} ${result.filename}  (${(result.pdfSize/1024).toFixed(1)} KB, año ${result.año})`);
        } else {
          errors++;
          consecutiveErrors++;
          failedThisRun.add(groupKey);
          console.log(`    [err]    ${group.base}: ${result.errorMessage}`);

          // Circuit breaker por sala: muchos errores consecutivos sugieren
          // que la sala entera está rota (ej. PDFs 404 sistemáticos).
          // Deshabilitado si noAbort=true (modo diagnóstico).
          if (consecutiveErrors >= CONSECUTIVE_ERROR_LIMIT && !noAbort) {
            console.log(`  [abort sala] ${consecutiveErrors} errores consecutivos — abortando ${sala}`);
            abortedBySala = true;
            break;
          }
          if (consecutiveErrors === CONSECUTIVE_ERROR_LIMIT && noAbort) {
            console.log(`  [no-abort] ${consecutiveErrors} errores consecutivos — continuando por flag --no-abort`);
          }
        }

        await sleep(DELAY_BETWEEN_REQUESTS_MS);
      }

      start += RESULTS_PER_PAGE;
      await sleep(DELAY_BETWEEN_REQUESTS_MS);
    }

    // Persistir estado tras la corrida exitosa
    writeState(name, {
      sala,
      docs_historicos: prevState.docs_historicos + downloaded,
      ultima_corrida_exitosa: new Date().toISOString(),
      ultima_corrida_descargados: downloaded,
      ultima_corrida_ya_existian: alreadyExists,
      ultima_corrida_errores: errors,
      ultima_corrida_unsupported: unsupportedSkipped,
      ultima_corrida_dup_in_run: dupInRun,
      ultima_corrida_early_stop: earlyStop,
      ultima_corrida_aborted_by_sala: abortedBySala
    });

    const suffixes = [];
    if (unsupportedSkipped > 0) suffixes.push(`${unsupportedSkipped} no-sop`);
    if (dupInRun > 0)           suffixes.push(`${dupInRun} dup-API`);
    if (earlyStop)              suffixes.push('early-stop');
    if (abortedBySala)          suffixes.push('ABORT-sala');
    const suffix = suffixes.length ? ' · ' + suffixes.join(' · ') : '';

    console.log(`\n  ═══ ${sala}: ${downloaded} descargados · ${alreadyExists} ya existían · ${errors} errores${suffix} ═══`);
    return {
      sala,
      downloaded,
      alreadyExists,
      errors,
      unsupportedSkipped,
      dupInRun,
      earlyStop,
      abortedBySala
    };
  } finally {
    releaseLock(name);
  }
}

// ─── API programática ─────────────────────────────────────────────────────────

/**
 * Corre el scraper de CSJ sobre una o más salas.
 *
 * Esta función es la API programática del módulo — el orquestador
 * (scrapers/index.js) la invoca directamente. Para uso CLI, ver runCLI abajo.
 *
 * @param {object} options
 * @param {string[]} [options.salas]       — salas a procesar. Default: las 4
 * @param {string}   [options.query='a']   — query del API
 * @param {string}   [options.ano='']      — filtro de año ("" = todos)
 * @param {string}   [options.magistrado=''] — filtro de magistrado
 * @param {number}   [options.max=10000]   — tope de docs a considerar por sala
 * @param {boolean}  [options.incremental=true] — early-stop al primer ya-existente
 *
 * @returns {Promise<{summaries, totals, elapsedMs}>} resultados agregados
 */
export async function run({
  salas = SALAS_DEFAULT,
  query = 'a',
  ano = '',
  magistrado = '',
  max = 10_000,
  incremental = true,
  noAbort = false
} = {}) {
  // Validación temprana
  for (const s of salas) {
    if (!SALAS_DEFAULT.includes(s)) {
      throw new Error(`Sala "${s}" no válida. Opciones: ${SALAS_DEFAULT.join(', ')}`);
    }
  }

  const startTime = Date.now();
  console.log('═══════════════════════════════════════════════════════════════');
  console.log('  Scraper — Corte Suprema de Justicia');
  console.log('═══════════════════════════════════════════════════════════════');
  console.log(`  Salas: ${salas.join(', ')}`);
  console.log(`  Modo:  ${incremental ? 'incremental' : 'ad-hoc'}${noAbort ? '  [NO-ABORT: circuit breaker por sala desactivado]' : ''}`);
  console.log(`  Max por sala: ${max}`);
  const budget = getBudgetStatus();
  console.log(`  Presupuesto HTTP hoy: ${budget.requests}/${budget.limit}`);

  const summaries = [];
  for (const sala of salas) {
    const summary = await scrapeSala({ sala, query, ano, magistrado, max, incremental, noAbort });
    summaries.push(summary);
  }

  const totals = summaries.reduce((acc, s) => ({
    downloaded:         acc.downloaded         + (s.downloaded         || 0),
    alreadyExists:      acc.alreadyExists      + (s.alreadyExists      || 0),
    errors:             acc.errors             + (s.errors             || 0),
    unsupportedSkipped: acc.unsupportedSkipped + (s.unsupportedSkipped || 0),
    dupInRun:           acc.dupInRun           + (s.dupInRun           || 0)
  }), { downloaded: 0, alreadyExists: 0, errors: 0, unsupportedSkipped: 0, dupInRun: 0 });

  return { summaries, totals, elapsedMs: Date.now() - startTime };
}

/**
 * Imprime el resumen final a stdout. Se usa tanto desde el CLI como desde el
 * orquestador cuando quiere el resumen por-scraper.
 */
export function printSummary({ summaries, totals, elapsedMs }) {
  const elapsedMin = (elapsedMs / 1000 / 60).toFixed(1);

  console.log('\n═══════════════════════════════════════════════════════════════');
  console.log('  RESUMEN FINAL — Corte Suprema de Justicia');
  console.log('═══════════════════════════════════════════════════════════════');
  for (const s of summaries) {
    if (s.skipped) {
      console.log(`  ${s.sala.padEnd(10)}  SALTADA (${s.reason})`);
    } else {
      const flags = [];
      if (s.unsupportedSkipped > 0) flags.push(`no-sop:${s.unsupportedSkipped}`);
      if (s.dupInRun > 0)           flags.push(`dup:${s.dupInRun}`);
      if (s.earlyStop)              flags.push('early-stop');
      if (s.abortedBySala)          flags.push('ABORT');
      const flagStr = flags.length ? '  ' + flags.join(' ') : '';
      console.log(
        `  ${s.sala.padEnd(10)}  descargados: ${String(s.downloaded).padStart(4)}  ` +
        `ya existían: ${String(s.alreadyExists).padStart(4)}  ` +
        `errores: ${String(s.errors).padStart(3)}` +
        flagStr
      );
    }
  }
  console.log('  ─────────────────────────────────────────────────────────────');
  const totalFlags = [];
  if (totals.unsupportedSkipped > 0) totalFlags.push(`no-sop:${totals.unsupportedSkipped}`);
  if (totals.dupInRun > 0)           totalFlags.push(`dup:${totals.dupInRun}`);
  const totalFlagStr = totalFlags.length ? '  ' + totalFlags.join(' ') : '';
  console.log(`  TOTAL       descargados: ${String(totals.downloaded).padStart(4)}  ` +
              `ya existían: ${String(totals.alreadyExists).padStart(4)}  ` +
              `errores: ${String(totals.errors).padStart(3)}` +
              totalFlagStr);
  console.log(`  Tiempo: ${elapsedMin} min`);
}

// ─── Entry point CLI ──────────────────────────────────────────────────────────

async function runCLI() {
  const { values } = parseArgs({
    options: {
      sala:        { type: 'string' },
      query:       { type: 'string', default: 'a' },
      ano:         { type: 'string', default: '' },
      magistrado:  { type: 'string', default: '' },
      max:         { type: 'string', default: '10000' },
      'ad-hoc':    { type: 'boolean', default: false },
      'no-abort':  { type: 'boolean', default: false }
    }
  });

  const result = await run({
    salas:       values.sala ? [values.sala] : SALAS_DEFAULT,
    query:       values.query,
    ano:         values.ano,
    magistrado:  values.magistrado,
    max:         Number(values.max),
    incremental: !values['ad-hoc'],
    noAbort:     values['no-abort']
  });

  printSummary(result);

  // Exit code según resultado:
  //   0 — todo ok (incluso si no se descargó nada nuevo)
  //   1 — errores de red o API, pero algo se descargó
  //   2 — fallo total (ningún scraper pudo correr)
  const { summaries, totals } = result;
  const allSkipped = summaries.every(s => s.skipped);
  if (allSkipped) process.exit(2);
  if (totals.errors > 0 && totals.downloaded === 0) process.exit(1);
  process.exit(0);
}

// Solo corre el CLI si este archivo se invoca directamente (no cuando se
// importa como módulo desde el orquestador).
if (import.meta.url === pathToFileURL(process.argv[1]).href) {
  runCLI().catch(e => {
    console.error('\nERROR FATAL:', e.message);
    console.error(e.stack);
    process.exit(1);
  });
}
