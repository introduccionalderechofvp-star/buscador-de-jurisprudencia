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
 *
 * Salida:
 *   - PDFs guardados en uploads/Sala <sala> - Corte Suprema de Justicia/<año>/
 *   - Estado persistido en scrapers/state/corte-suprema-<sala>.json
 *   - Errores en scrapers/errors/<YYYY-MM-DD>.jsonl
 *   - Resumen final por sala y totales a stdout
 */

import 'dotenv/config';
import { parseArgs } from 'node:util';

import { fetchJSON, fetchBuffer, getBudgetStatus } from './lib/http.js';
import { saveAtomic, cleanupTmp, safeName, buildBasenameIndex } from './lib/storage.js';
import { readState, writeState, acquireLock, releaseLock } from './lib/state.js';
import { logError } from './lib/errors.js';

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

async function downloadPdf(onlinePath) {
  return fetchBuffer(API_DOWNLOAD, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ path: onlinePath })
  }, { timeoutMs: 60_000 });   // PDFs pueden ser más grandes, tolerancia mayor
}

// ─── Lógica principal por sala ────────────────────────────────────────────────

async function scrapeSala({ sala, query, ano, magistrado, max, incremental }) {
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

  // Índice de archivos ya presentes (en cualquier nivel bajo el organo). Esto
  // permite que el scraper reconozca archivos bajados históricamente aunque
  // vivan en subcarpetas legacy distintas a la ruta plana que el scraper usa
  // para archivos nuevos (ej. <organo>/<año>/<file>.pdf).
  const existingNames = buildBasenameIndex(organo);
  console.log(`[${sala}] Índice de archivos existentes: ${existingNames.size} PDFs ya en disco.`);

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

  let start         = 0;
  let total         = Infinity;
  let downloaded    = 0;
  let alreadyExists = 0;
  let errors        = 0;
  let earlyStop     = false;

  try {
    while (downloaded + alreadyExists < max && start < total && !earlyStop) {
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

      for (const doc of page.searchResults) {
        if (downloaded + alreadyExists >= max) break;

        // Solo PDFs (la API puede devolver otros formatos mezclados)
        if (!doc.onlinePath?.toLowerCase().endsWith('.pdf')) continue;

        // Normalizar filename: garantizar extensión .pdf, sin caracteres inválidos
        const rawTitle = doc.title || doc.onlinePath.split('/').pop();
        const filename = safeName(rawTitle.toLowerCase().endsWith('.pdf') ? rawTitle : rawTitle + '.pdf');
        const año      = String(doc.ano || new Date().getFullYear());

        // Dedup: ¿existe un archivo con este basename en CUALQUIER nivel bajo
        // el organo? (tolera estructuras de carpeta legacy además de la ruta
        // plana que usamos para archivos nuevos)
        if (existingNames.has(filename)) {
          alreadyExists++;
          if (incremental) {
            console.log(`    [existe] ${filename} → early-stop activado`);
            earlyStop = true;
            break;
          }
          console.log(`    [skip]   ya existe: ${filename}`);
          continue;
        }

        // Descargar
        try {
          const buffer = await downloadPdf(doc.onlinePath);
          if (!buffer || buffer.length === 0) {
            throw new Error('buffer vacío');
          }
          saveAtomic(organo, año, filename, buffer);
          existingNames.add(filename);     // actualizar índice en vivo
          downloaded++;
          console.log(`    [ok]     ${filename}  (${(buffer.length/1024).toFixed(1)} KB, año ${año})`);
        } catch (e) {
          errors++;
          logError(name, {
            phase: 'download',
            doc_title: doc.title,
            doc_path: doc.onlinePath,
            doc_ano: doc.ano,
            error: e.message
          });
          console.log(`    [err]    ${filename}: ${e.message}`);
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
      ultima_corrida_early_stop: earlyStop
    });

    console.log(`\n  ═══ ${sala}: ${downloaded} descargados · ${alreadyExists} ya existían · ${errors} errores${earlyStop ? ' · early-stop' : ''} ═══`);
    return { sala, downloaded, alreadyExists, errors, earlyStop };
  } finally {
    releaseLock(name);
  }
}

// ─── Entry point ──────────────────────────────────────────────────────────────

async function main() {
  const { values } = parseArgs({
    options: {
      sala:       { type: 'string' },
      query:      { type: 'string', default: 'a' },
      ano:        { type: 'string', default: '' },
      magistrado: { type: 'string', default: '' },
      max:        { type: 'string', default: '10000' },
      'ad-hoc':   { type: 'boolean', default: false }
    }
  });

  const max         = Number(values.max);
  const incremental = !values['ad-hoc'];
  const salas       = values.sala ? [values.sala] : SALAS_DEFAULT;

  // Validación temprana
  for (const s of salas) {
    if (!SALAS_DEFAULT.includes(s)) {
      console.error(`ERROR: Sala "${s}" no válida. Opciones: ${SALAS_DEFAULT.join(', ')}`);
      process.exit(1);
    }
  }

  const startTime = Date.now();
  console.log('═══════════════════════════════════════════════════════════════');
  console.log('  Scraper — Corte Suprema de Justicia');
  console.log('═══════════════════════════════════════════════════════════════');
  console.log(`  Salas: ${salas.join(', ')}`);
  console.log(`  Modo:  ${incremental ? 'incremental' : 'ad-hoc'}`);
  console.log(`  Max por sala: ${max}`);
  const budget = getBudgetStatus();
  console.log(`  Presupuesto HTTP hoy: ${budget.requests}/${budget.limit}`);

  const summaries = [];
  for (const sala of salas) {
    const summary = await scrapeSala({
      sala,
      query: values.query,
      ano:   values.ano,
      magistrado: values.magistrado,
      max,
      incremental
    });
    summaries.push(summary);
  }

  // Resumen final
  const totals = summaries.reduce((acc, s) => ({
    downloaded: acc.downloaded + (s.downloaded || 0),
    alreadyExists: acc.alreadyExists + (s.alreadyExists || 0),
    errors: acc.errors + (s.errors || 0)
  }), { downloaded: 0, alreadyExists: 0, errors: 0 });

  const elapsed = ((Date.now() - startTime) / 1000 / 60).toFixed(1);

  console.log('\n═══════════════════════════════════════════════════════════════');
  console.log('  RESUMEN FINAL');
  console.log('═══════════════════════════════════════════════════════════════');
  for (const s of summaries) {
    if (s.skipped) {
      console.log(`  ${s.sala.padEnd(10)}  SALTADA (${s.reason})`);
    } else {
      console.log(
        `  ${s.sala.padEnd(10)}  descargados: ${String(s.downloaded).padStart(4)}  ` +
        `ya existían: ${String(s.alreadyExists).padStart(4)}  ` +
        `errores: ${String(s.errors).padStart(3)}` +
        (s.earlyStop ? '  [early-stop]' : '')
      );
    }
  }
  console.log('  ─────────────────────────────────────────────────────────────');
  console.log(`  TOTAL       descargados: ${String(totals.downloaded).padStart(4)}  ` +
              `ya existían: ${String(totals.alreadyExists).padStart(4)}  ` +
              `errores: ${String(totals.errors).padStart(3)}`);
  console.log(`  Tiempo: ${elapsed} min`);

  // Exit code según resultado:
  //   0 — todo ok (incluso si no se descargó nada nuevo)
  //   1 — errores de red o API, pero algo se descargó
  //   2 — fallo total (ningún scraper pudo correr)
  const allSkipped = summaries.every(s => s.skipped);
  if (allSkipped) process.exit(2);
  if (totals.errors > 0 && totals.downloaded === 0) process.exit(1);
  process.exit(0);
}

main().catch(e => {
  console.error('\nERROR FATAL:', e.message);
  console.error(e.stack);
  process.exit(1);
});
