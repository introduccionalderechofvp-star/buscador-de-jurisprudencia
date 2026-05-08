#!/usr/bin/env node
/**
 * corte-suprema-tutelas-md.js
 *
 * Scraper especializado para Sala Tutelas de la CSJ que genera .md DIRECTO,
 * sin pasar por PDF intermedio.
 *
 * Por qué un scraper aparte (no se mezcla con corte-suprema.js):
 *   - Corpus de Tutelas: 193k sentencias × 500KB/PDF = ~95 GB. No viable.
 *   - Como .md: ~10 GB. Esto solo funciona si la conversión es en RAM,
 *     mientras se descarga, sin tocar disco con el PDF.
 *   - Las otras salas SÍ se quieren conservar como PDF (para poder leer el
 *     original con sello, firma, etc.) — el scraper general queda intacto.
 *
 * Pipeline por archivo (en RAM, sin .pdf intermedio):
 *   .pdf  → pdf-parse → texto → escribe .md
 *   .docx → mammoth   → texto → escribe .md
 *   .doc  → LibreOffice (txt) → escribe .md  [fallback, raro en Tutelas]
 *
 * Filtros:
 *   - MIN_YEAR (env, default 2012). Sentencias anteriores se saltan.
 *   - EARLY_STOP_THRESHOLD (env, default 5). Como el scraper general.
 *
 * Uso:
 *   node scrapers/corte-suprema-tutelas-md.js
 *   MIN_YEAR=2018 node scrapers/corte-suprema-tutelas-md.js
 *   node scrapers/corte-suprema-tutelas-md.js --max 100
 *   node scrapers/corte-suprema-tutelas-md.js --force
 */

import 'dotenv/config';
import { parseArgs } from 'node:util';
import path from 'node:path';
import fs from 'node:fs';
import pdf from 'pdf-parse';
import mammoth from 'mammoth';

import { fetchJSON, fetchBuffer } from './lib/http.js';
import { writeState, acquireLock, releaseLock } from './lib/state.js';
import { safeName, cleanupTmp, buildBasenameIndex } from './lib/storage.js';
import { convertDocxBufferToPdf } from './lib/docx.js';

const SALA       = 'Tutelas';
const ORGANO     = `Sala ${SALA} - Corte Suprema de Justicia`;
const STATE_NAME = `corte-suprema-${SALA.toLowerCase()}-md`;

const API_SEARCH   = 'https://consultaprovidenciasbk.cortesuprema.gov.co/api';
const API_DOWNLOAD = 'https://consultaprovidenciasbk.cortesuprema.gov.co/downloadFile';

const MIN_YEAR             = Number(process.env.MIN_YEAR || 2012);
const EARLY_STOP_THRESHOLD = Math.max(1, Number(process.env.EARLY_STOP_THRESHOLD || 5));
const UPLOADS_DIR          = path.join(process.cwd(), 'uploads');
const SUPPORTED_EXTS       = new Set(['pdf', 'docx', 'doc']);

// ─── API ──────────────────────────────────────────────────────────────────────

function buildQuery({ start, query = 'a', ano = '' }) {
  const esc = s => String(s ?? '').replace(/"/g, '\\"');
  return `{
  getSearchResult(searchQuery:{
    query: "${esc(query)}"
    typeOfQuery: "${esc(SALA)}"
    start: ${Number(start)}
    isExact: false
    magistrate: ""
    year: "${esc(ano)}"
    autoSentencia: "SENTENCIA"
    order: "NEW_FIRST"
    roomTutelas: ""
    addedQueries: []
  })
  { searchResults { title onlinePath doctor ano } numOfResults }
}`;
}

async function searchPage({ start, query, ano }) {
  const data = await fetchJSON(API_SEARCH, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ query: buildQuery({ start, query, ano }) })
  });
  const r = data?.data?.getSearchResult;
  if (!r || !Array.isArray(r.searchResults)) {
    throw new Error(`API malformada: ${JSON.stringify(data).slice(0, 200)}`);
  }
  return r;
}

async function downloadFile(onlinePath) {
  return fetchBuffer(API_DOWNLOAD, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ path: onlinePath })
  }, { timeoutMs: 60_000 });
}

function flattenOnlinePath(onlinePath) {
  const parts = onlinePath.split('/');
  if (parts.length <= 8) return onlinePath;
  return [...parts.slice(0, 7), parts[parts.length - 1]].join('/');
}

async function downloadWithFallback(onlinePath) {
  try { return await downloadFile(onlinePath); }
  catch (e1) {
    const flat = flattenOnlinePath(onlinePath);
    if (flat === onlinePath) throw e1;
    try { return await downloadFile(flat); }
    catch (e2) { throw new Error(`nested: ${e1.message} | flat: ${e2.message}`); }
  }
}

// ─── Extracción según formato ─────────────────────────────────────────────────

function extractExt(onlinePath) {
  const m = String(onlinePath || '').toLowerCase().match(/\.([a-z0-9]+)$/);
  return m ? m[1] : null;
}

function extractBasename(doc) {
  const raw = doc.title || path.basename(doc.onlinePath || '');
  return raw.replace(/\.(pdf|docx|doc|md|txt)$/i, '').trim();
}

async function extractTextFromBuffer(buffer, ext) {
  if (ext === 'pdf') {
    const parsed = await pdf(buffer);
    return { text: (parsed.text || '').trim(), numPages: parsed.numpages || 0 };
  }
  if (ext === 'docx') {
    const r = await mammoth.extractRawText({ buffer });
    return { text: (r.value || '').trim(), numPages: 0 };
  }
  if (ext === 'doc') {
    // Para .doc viejo, no hay parser puro JS confiable. Pasamos por LibreOffice
    // (lo más raro en Tutelas — la mayoría son docx o pdf modernos).
    const pdfBuf = await convertDocxBufferToPdf(buffer, 'doc');
    const parsed = await pdf(pdfBuf);
    return { text: (parsed.text || '').trim(), numPages: parsed.numpages || 0 };
  }
  return { text: '', numPages: 0 };
}

function saveMdAtomic({ basename, año, sourcePath, numPages, text }) {
  const dir  = path.join(UPLOADS_DIR, safeName(ORGANO), String(año));
  fs.mkdirSync(dir, { recursive: true });
  const filename = safeName(`${basename}.md`);
  const full = path.join(dir, filename);
  const tmp  = full + '.tmp';

  const header = `<!-- source: ${sourcePath} pages: ${numPages} -->\n\n`;
  fs.writeFileSync(tmp, header + text, 'utf8');
  fs.renameSync(tmp, full);
  return full;
}

// ─── Main ─────────────────────────────────────────────────────────────────────

async function main() {
  const { values } = parseArgs({
    options: {
      max:   { type: 'string',  default: '999999' },
      query: { type: 'string',  default: 'a' },
      ano:   { type: 'string',  default: '' },
      force: { type: 'boolean', default: false }
    }
  });
  const max   = Number(values.max);
  const query = values.query;
  const ano   = values.ano;
  const force = values.force;

  console.log('═══════════════════════════════════════════════════════════════');
  console.log('  Scraper Tutelas → .md directo');
  console.log('═══════════════════════════════════════════════════════════════');
  console.log(`  Modo:        ${force ? 'force (sin early-stop)' : `incremental (umbral ${EARLY_STOP_THRESHOLD})`}`);
  console.log(`  Año mínimo:  ${MIN_YEAR}`);
  console.log(`  Max:         ${max}`);
  console.log(`  Query: "${query}" · Año filter: "${ano || 'todos'}"`);

  const lock = acquireLock(STATE_NAME);
  if (lock.heldByPid) {
    console.error(`  Ya hay otra instancia (PID ${lock.heldByPid}). Salgo.`);
    process.exit(0);
  }

  try {
    const tmpClean = cleanupTmp(ORGANO);
    if (tmpClean) console.log(`  Limpiados ${tmpClean} .tmp huérfanos.`);

    const onDisk = buildBasenameIndex(ORGANO);
    console.log(`  Basenames ya en disco: ${onDisk.size}\n`);

    let start = 0, total = Infinity;
    let downloaded = 0, skipped = 0, errors = 0, oldYear = 0, dupRun = 0, unsupported = 0;
    let consecutiveExisting = 0;
    let earlyStop = false;
    const seenThisRun = new Set();
    const t0 = Date.now();

    while (downloaded < max && start < total && !earlyStop) {
      let result;
      try { result = await searchPage({ start, query, ano }); }
      catch (e) { console.error(`  Error en page start=${start}: ${e.message}`); break; }

      total = result.numOfResults;
      if (start === 0) console.log(`  Total API: ${total}\n`);
      if (!result.searchResults.length) break;

      for (const doc of result.searchResults) {
        if (downloaded >= max) break;

        const año = Number(doc.ano);
        if (!año || año < MIN_YEAR) { oldYear++; continue; }

        const ext = extractExt(doc.onlinePath);
        if (!ext || !SUPPORTED_EXTS.has(ext)) { unsupported++; continue; }

        const base = extractBasename(doc);
        if (!base) continue;
        const key = base.toLowerCase();

        if (seenThisRun.has(key)) { dupRun++; continue; }
        seenThisRun.add(key);

        if (onDisk.has(key)) {
          skipped++;
          if (!force) {
            consecutiveExisting++;
            if (consecutiveExisting >= EARLY_STOP_THRESHOLD) {
              console.log(`    [existe] ${base} → ${consecutiveExisting} consecutivos, early-stop`);
              earlyStop = true;
              break;
            }
            console.log(`    [existe] ${base} (${consecutiveExisting}/${EARLY_STOP_THRESHOLD})`);
          }
          continue;
        }

        try {
          const buffer = await downloadWithFallback(doc.onlinePath);
          const { text, numPages } = await extractTextFromBuffer(buffer, ext);
          if (!text || text.length < 50) {
            console.log(`    [skip-empty] ${base} (sin texto extraíble)`);
            errors++;
            continue;
          }
          saveMdAtomic({ basename: base, año, sourcePath: doc.onlinePath, numPages, text });
          downloaded++;
          consecutiveExisting = 0;
          const kb = (text.length / 1024).toFixed(1);
          console.log(`    [ok ${ext}→md]  ${base}.md (${kb} KB texto, año ${año})`);
        } catch (e) {
          console.log(`    [err]    ${base}: ${e.message.slice(0, 80)}`);
          errors++;
        }
      }
      start += result.searchResults.length;
    }

    const elapsedMin = ((Date.now() - t0) / 1000 / 60).toFixed(1);
    console.log(`\n═══════════════════════════════════════════════════════════════`);
    console.log(`  RESUMEN — Tutelas`);
    console.log(`═══════════════════════════════════════════════════════════════`);
    console.log(`  Descargados (.md):   ${downloaded}`);
    console.log(`  Saltados (existían): ${skipped}`);
    console.log(`  Saltados (año<${MIN_YEAR}): ${oldYear}`);
    console.log(`  Saltados (formato):  ${unsupported}`);
    console.log(`  Errores:             ${errors}`);
    console.log(`  Dup en run:          ${dupRun}`);
    console.log(`  Tiempo:              ${elapsedMin} min`);
    if (earlyStop) console.log(`  Early-stop activado.`);

    writeState(STATE_NAME, {
      sala: SALA,
      ultima_corrida_exitosa: new Date().toISOString(),
      ultima_corrida_descargados: downloaded,
      ultima_corrida_saltados: skipped,
      ultima_corrida_old_year: oldYear,
      ultima_corrida_errores: errors,
      ultima_corrida_early_stop: earlyStop,
      min_year: MIN_YEAR
    });
  } finally {
    releaseLock(STATE_NAME);
  }
}

main().catch(e => {
  console.error('FALLO FATAL:', e);
  process.exit(1);
});
