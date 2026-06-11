/**
 * marcar-salvamentos-por-stem.js
 *
 * Variante del script de marcado que mapea archivos a puntos de Qdrant
 * por STEM (basename sin extensión) en lugar de por document_id.
 *
 * Por qué: para Sala Laboral CSJ y Sala Tutelas CSJ los archivos en disco
 * fueron convertidos de .pdf → .md después del ingest. El hash
 * `document_id = hash(filename + text)` ya no coincide:
 *   - en Qdrant: filename termina en .pdf, text es output de pdf-parse
 *   - en disco : filename termina en .md,  text es el contenido del .md
 *
 * El stem (ej. "SL1979-2023") es el mismo en ambos casos, así que sirve
 * como llave de matching.
 *
 * Para casos posicionales, en lugar de mapear chunk_index → palabra exacta
 * (que asumiría chunking idéntico al ingest), usa proporciones: si el
 * salvamento empieza al 70% del archivo en disco, marca los chunks con
 * chunk_index >= round(0.70 * total_chunks). Robusto a diferencias
 * menores de chunking entre el texto del .pdf original y el .md actual.
 *
 * Uso:
 *   node scripts/marcar-salvamentos-por-stem.js --organo "Sala Tutelas - Corte Suprema de Justicia"
 *   node scripts/marcar-salvamentos-por-stem.js --organo "Sala Laboral - Corte Suprema de Justicia"
 *   --dry-run, --limit N, CONCURRENCY=N como en el v1.
 */

import 'dotenv/config';
import fs from 'fs';
import path from 'path';
import pdf from 'pdf-parse';
import { convert as htmlToText } from 'html-to-text';
import { QdrantClient } from '@qdrant/js-client-rest';

// ─── Config ───────────────────────────────────────────────────────────────────

const UPLOADS_DIR = path.join(process.cwd(), 'uploads');
const COLLECTION  = process.env.QDRANT_COLLECTION || 'sentencias';
const QDRANT_URL  = process.env.QDRANT_URL        || 'http://localhost:6333';
const QDRANT_KEY  = process.env.QDRANT_API_KEY    || undefined;

const THRESHOLD_PCT = Number(process.env.THRESHOLD_PCT || 50);
const CONCURRENCY   = Math.max(1, Number(process.env.CONCURRENCY || 1));

const DRY_RUN       = process.argv.includes('--dry-run');
const ORGANO_FILTER = process.argv[process.argv.indexOf('--organo') + 1] || null;
const LIMIT         = Number(process.argv[process.argv.indexOf('--limit') + 1] || 0);

const STATE_FILE = path.join(process.cwd(), '.marcar-salvamentos-stem-state.json');
const INDEXABLE_EXTS = ['.pdf', '.md', '.txt', '.html'];

if (!ORGANO_FILTER) {
  console.error('Falta --organo "Nombre del órgano". Este script siempre va por uno.');
  process.exit(1);
}

// ─── State ────────────────────────────────────────────────────────────────────

function loadState() {
  try {
    const raw = JSON.parse(fs.readFileSync(STATE_FILE, 'utf8'));
    return { processed: new Set(raw.processed || []), stats: raw.stats || {} };
  } catch {
    return { processed: new Set(), stats: {} };
  }
}

function saveState(state) {
  const tmp = STATE_FILE + '.tmp';
  fs.writeFileSync(tmp, JSON.stringify({
    processed: [...state.processed],
    stats: state.stats,
    saved_at: new Date().toISOString(),
  }));
  fs.renameSync(tmp, STATE_FILE);
}

// ─── Helpers ──────────────────────────────────────────────────────────────────

function fmt(n) { return n.toLocaleString('es-CO'); }

function stemOf(filename) {
  return path.basename(filename, path.extname(filename));
}

function findFiles(dir) {
  const results = [];
  if (!fs.existsSync(dir)) return results;
  for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
    if (entry.name.startsWith('._')) continue;
    const full = path.join(dir, entry.name);
    if (entry.isDirectory()) results.push(...findFiles(full));
    else if (entry.isFile() && INDEXABLE_EXTS.includes(path.extname(entry.name).toLowerCase())) {
      results.push(full);
    }
  }
  return results;
}

async function extractText(filePath) {
  const ext = path.extname(filePath).toLowerCase();
  if (ext === '.pdf') {
    const buffer = fs.readFileSync(filePath);
    const parsed = await pdf(buffer);
    return (parsed.text || '').trim();
  }
  if (ext === '.md' || ext === '.txt') {
    return fs.readFileSync(filePath, 'utf8').trim();
  }
  if (ext === '.html') {
    const html = fs.readFileSync(filePath, 'utf8');
    return htmlToText(html, {
      wordwrap: false,
      selectors: [
        { selector: 'script', format: 'skip' },
        { selector: 'style',  format: 'skip' },
        { selector: 'nav',    format: 'skip' },
        { selector: 'a',      options: { ignoreHref: true } },
      ],
    }).trim();
  }
  throw new Error(`extensión no soportada: ${ext}`);
}

// ─── Detector posicional ──────────────────────────────────────────────────────

const SALVAMENTO_RE = /^[ \t]*(SALVAMENTO\s+(?:PARCIAL\s+)?DE\s+VOTO|ACLARACI[ÓO]N\s+(?:PARCIAL\s+)?DE\s+VOTO)\b/gim;

/**
 * Devuelve { charOffset, totalChars } o null si no detecta.
 * Trabaja en chars para luego pasar a proporción (no palabras, porque la
 * v2 usa proporción y los chars son más consistentes entre formatos).
 */
function detectarSalvamento(text) {
  SALVAMENTO_RE.lastIndex = 0;
  const matches = [];
  let m;
  while ((m = SALVAMENTO_RE.exec(text)) !== null) matches.push(m.index);
  if (matches.length === 0) return null;
  const threshold = text.length * (THRESHOLD_PCT / 100);
  const valido = matches.find(idx => idx >= threshold);
  if (valido === undefined) return null;
  return { charOffset: valido, totalChars: text.length };
}

// ─── Construcción del índice por stem ────────────────────────────────────────

async function construirIndicePorStem(qdrant, organo) {
  console.log(`  Scrolling Qdrant para "${organo}"...`);
  const stemMap = new Map();   // stem → [{id, chunk_index}]
  let offset = undefined;
  let count = 0;
  const start = Date.now();

  do {
    const r = await qdrant.scroll(COLLECTION, {
      filter: { must: [{ key: 'organo', match: { value: organo } }] },
      limit: 1024,
      offset,
      with_payload: { include: ['filename', 'chunk_index'] },
      with_vector: false,
    });
    for (const p of r.points) {
      const filename = p.payload?.filename;
      const chunkIndex = p.payload?.chunk_index;
      if (!filename || chunkIndex === undefined || chunkIndex === null) continue;
      const stem = stemOf(filename);
      let arr = stemMap.get(stem);
      if (!arr) { arr = []; stemMap.set(stem, arr); }
      arr.push({ id: p.id, chunk_index: chunkIndex });
    }
    count += r.points.length;
    offset = r.next_page_offset;
    if (count % 10240 < 1024) {
      const sec = ((Date.now() - start) / 1000).toFixed(0);
      process.stdout.write(`\r  Indexados: ${fmt(count)} puntos / ${fmt(stemMap.size)} stems (${sec}s)`);
    }
  } while (offset);

  const sec = ((Date.now() - start) / 1000).toFixed(0);
  process.stdout.write(`\r  Indexados: ${fmt(count)} puntos / ${fmt(stemMap.size)} stems (${sec}s)\n`);
  return stemMap;
}

// ─── Procesamiento por archivo ────────────────────────────────────────────────

async function procesarArchivo(qdrant, filePath, stemMap, stats) {
  const filename = path.basename(filePath);
  const stem     = stemOf(filename);

  const chunks = stemMap.get(stem);
  if (!chunks || chunks.length === 0) {
    return { status: 'no_chunks' };
  }

  let text;
  try { text = await extractText(filePath); }
  catch (e) {
    stats.errores++;
    return { status: 'error', mensaje: `lectura: ${e.message}` };
  }
  if (text.length < 50) return { status: 'sin_texto' };

  const det = detectarSalvamento(text);
  if (!det) return { status: 'sin_salvamento' };

  // Proporción del salvamento dentro del documento, en chars.
  const fraction = det.charOffset / det.totalChars;

  // Marcar chunks cuyo índice esté por encima del umbral proporcional.
  // (i+1)/total_chunks > fraction  ⇔  i >= floor(fraction * total_chunks)
  const maxIdx = chunks.reduce((m, c) => Math.max(m, c.chunk_index), 0);
  const total  = maxIdx + 1;
  const umbralIdx = Math.floor(fraction * total);

  const idsSalvamento = chunks
    .filter(c => c.chunk_index >= umbralIdx)
    .map(c => c.id);

  if (idsSalvamento.length === 0) {
    return { status: 'sin_salvamento_post_umbral' };
  }

  if (!DRY_RUN) {
    const BATCH = 200;
    try {
      for (let i = 0; i < idsSalvamento.length; i += BATCH) {
        await qdrant.setPayload(COLLECTION, {
          payload: { tipo_seccion: 'salvamento' },
          points: idsSalvamento.slice(i, i + BATCH),
          wait: true,
        });
      }
    } catch (e) {
      stats.errores++;
      return { status: 'error', mensaje: `setPayload: ${e.message}` };
    }
  }

  return {
    status: 'marcado',
    total: chunks.length,
    marcados: idsSalvamento.length,
    fraction: fraction.toFixed(2),
  };
}

// ─── Main ─────────────────────────────────────────────────────────────────────

async function ensureIndex(qdrant) {
  try {
    await qdrant.createPayloadIndex(COLLECTION, {
      field_name: 'tipo_seccion',
      field_schema: 'keyword',
    });
  } catch (e) {
    if (!/already exists|exists/i.test(e.message || '')) throw e;
  }
}

async function main() {
  const qdrant = new QdrantClient({ url: QDRANT_URL, apiKey: QDRANT_KEY });
  const organo = ORGANO_FILTER;

  console.log(`\n=== Marcado de salvamentos (por stem) ===`);
  console.log(`Modo         : ${DRY_RUN ? 'DRY-RUN' : 'REAL'}`);
  console.log(`Órgano       : ${organo}`);
  console.log(`Concurrencia : ${CONCURRENCY}`);
  console.log(`Threshold    : ${THRESHOLD_PCT}%`);
  if (LIMIT > 0) console.log(`Límite       : ${LIMIT} archivos`);

  if (!DRY_RUN) await ensureIndex(qdrant);

  const state = loadState();
  console.log(`State        : ${fmt(state.processed.size)} stems ya procesados\n`);

  // 1. Índice por stem (una sola pasada por Qdrant)
  const stemMap = await construirIndicePorStem(qdrant, organo);
  if (stemMap.size === 0) {
    console.log(`  No hay puntos en Qdrant para "${organo}". Nada que hacer.`);
    return;
  }

  // 2. Archivos en disco
  const dir = path.join(UPLOADS_DIR, organo);
  let files = findFiles(dir);
  if (LIMIT > 0) files = files.slice(0, LIMIT);
  console.log(`  Archivos en disco: ${fmt(files.length)}\n`);

  const stats = {
    total: files.length,
    marcados: 0, sinSalvamento: 0, yaProcesados: 0,
    noChunks: 0, sinTexto: 0, errores: 0, chunksMarcados: 0,
  };

  let cursor = 0;
  let saveCounter = 0;
  const startTime = Date.now();

  const worker = async () => {
    while (true) {
      const idx = cursor++;
      if (idx >= files.length) return;
      const filePath = files[idx];
      const filename = path.basename(filePath);
      const stem     = stemOf(filename);
      const short    = filename.length > 45 ? filename.slice(0, 42) + '...' : filename;
      const prefix   = `  [${idx + 1}/${files.length}] ${short.padEnd(47)}`;

      if (state.processed.has(stem)) {
        stats.yaProcesados++;
        continue;
      }

      const r = await procesarArchivo(qdrant, filePath, stemMap, stats);
      switch (r.status) {
        case 'marcado':
          stats.marcados++;
          stats.chunksMarcados += r.marcados;
          console.log(`${prefix} ${r.marcados}/${r.total} (frac ${r.fraction})`);
          break;
        case 'sin_salvamento':
        case 'sin_salvamento_post_umbral':
          stats.sinSalvamento++;
          break;
        case 'no_chunks':
          stats.noChunks++;
          break;
        case 'sin_texto':
          stats.sinTexto++;
          break;
        case 'error':
          console.log(`${prefix} ERROR: ${r.mensaje?.slice(0, 80)}`);
          break;
      }

      if (!DRY_RUN) {
        state.processed.add(stem);
        if (++saveCounter >= 200) {
          saveState(state);
          saveCounter = 0;
        }
      }
    }
  };

  await Promise.all(Array.from({ length: CONCURRENCY }, () => worker()));

  state.stats[organo] = stats;
  if (!DRY_RUN) saveState(state);

  const elapsed = ((Date.now() - startTime) / 1000 / 60).toFixed(1);
  console.log(`\n${'─'.repeat(60)}`);
  console.log(`Resumen ${organo}:`);
  console.log(`  Marcados (con salvamento)  : ${fmt(stats.marcados)}  (${fmt(stats.chunksMarcados)} chunks)`);
  console.log(`  Sin salvamento detectado   : ${fmt(stats.sinSalvamento)}`);
  console.log(`  Ya procesados (skip state) : ${fmt(stats.yaProcesados)}`);
  console.log(`  Sin chunks en Qdrant       : ${fmt(stats.noChunks)}`);
  console.log(`  Sin texto                  : ${fmt(stats.sinTexto)}`);
  console.log(`  Errores                    : ${fmt(stats.errores)}`);
  console.log(`Tiempo: ${elapsed} min`);
  if (DRY_RUN) console.log(`\n⚠️  DRY-RUN. Nada escrito a Qdrant.`);
}

main().catch(e => { console.error('\nFATAL:', e); process.exit(1); });
