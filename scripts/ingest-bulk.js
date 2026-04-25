/**
 * ingest-bulk.js
 *
 * Ingesta masiva de PDFs desde la carpeta uploads/.
 * - Cada subcarpeta de primer nivel = nombre del órgano judicial.
 * - Busca PDFs recursivamente en subcarpetas de año u otras.
 * - Detecta duplicados: omite archivos ya indexados en Qdrant.
 * - Muestra progreso en tiempo real.
 * - Optimización mtime: salta archivos anteriores a la última corrida
 *   exitosa sin leerlos ni parsearlos. Acelera cron diario de ~30 min
 *   a ~segundos cuando solo hay pocos archivos nuevos.
 *
 * Uso:
 *   node scripts/ingest-bulk.js              # incremental (usa mtime filter)
 *   node scripts/ingest-bulk.js --force      # procesa TODO, ignora state
 *
 * Opciones de entorno (en .env):
 *   BULK_ORGANO         — fuerza un órgano específico ignorando el nombre de carpeta
 *   BULK_PATH           — procesa solo esta subcarpeta dentro de uploads/
 *   INGEST_FORCE        — equivalente a --force (true/false)
 *   INGEST_CONCURRENCY  — archivos en paralelo (default 1 = secuencial)
 *   INGEST_DELAY_MS     — delay entre sub-lotes de embeddings (default 150)
 *   INGEST_FILE_DELAY_MS — delay entre archivos por worker (default 500)
 */

import 'dotenv/config';
import fs from 'fs';
import path from 'path';
import pdf from 'pdf-parse';
import OpenAI from 'openai';
import { QdrantClient } from '@qdrant/js-client-rest';
import crypto from 'crypto';

// ─── Config ───────────────────────────────────────────────────────────────────

const UPLOADS_DIR    = path.join(process.cwd(), 'uploads');
const COLLECTION     = process.env.QDRANT_COLLECTION  || 'sentencias';
const EMBEDDING_MODEL= process.env.EMBEDDING_MODEL    || 'text-embedding-3-large';
const EMBEDDING_DIM  = Number(process.env.EMBEDDING_DIM || 3072);
const QDRANT_URL     = process.env.QDRANT_URL         || 'http://localhost:6333';
const QDRANT_KEY     = process.env.QDRANT_API_KEY     || undefined;

const CHUNK_SIZE  = 500;
const CHUNK_OVERLAP = 100;
const EMB_BATCH   = 20;   // chunks por llamada a OpenAI
const DELAY_MS    = Number(process.env.INGEST_DELAY_MS || 150);
const FILE_DELAY_MS = Number(process.env.INGEST_FILE_DELAY_MS || 500);
const CONCURRENCY = Math.max(1, Number(process.env.INGEST_CONCURRENCY || 1));

// ─── Estado persistente (optimización mtime) ──────────────────────────────────

const STATE_FILE  = path.join(process.cwd(), '.ingest-state.json');
const FORCE_FULL  = process.argv.includes('--force')
                 || process.env.INGEST_FORCE === 'true';
const SKIP_N      = Number(process.argv.find(a => a.startsWith('--skip='))?.split('=')[1] || 0);

function loadState() {
  if (FORCE_FULL) return { last_completed_at: 0 };
  try {
    return JSON.parse(fs.readFileSync(STATE_FILE, 'utf8'));
  } catch {
    return { last_completed_at: 0 };
  }
}

function saveState(state) {
  const tmp = STATE_FILE + '.tmp';
  fs.writeFileSync(tmp, JSON.stringify(state, null, 2));
  fs.renameSync(tmp, STATE_FILE);
}

// ─── Helpers ──────────────────────────────────────────────────────────────────

function hash(s) {
  return crypto.createHash('sha256').update(s).digest('hex');
}

function hashToUUID(s) {
  const h = crypto.createHash('sha256').update(s).digest('hex');
  return `${h.slice(0,8)}-${h.slice(8,12)}-4${h.slice(13,16)}-${(parseInt(h.slice(16,18),16)&0x3f|0x80).toString(16)}${h.slice(18,20)}-${h.slice(20,32)}`;
}

function chunkText(text) {
  const words = text.split(/\s+/).filter(Boolean);
  const chunks = [];
  let start = 0;
  while (start < words.length) {
    const end = Math.min(start + CHUNK_SIZE, words.length);
    chunks.push(words.slice(start, end).join(' '));
    if (end >= words.length) break;
    start += CHUNK_SIZE - CHUNK_OVERLAP;
  }
  return chunks;
}

function findPDFs(dir) {
  const results = [];
  for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
    // Ignorar sidecars AppleDouble de macOS (._*) — son metadata, no PDFs reales
    if (entry.name.startsWith('._')) continue;
    const full = path.join(dir, entry.name);
    if (entry.isDirectory()) results.push(...findPDFs(full));
    else if (entry.isFile() && entry.name.toLowerCase().endsWith('.pdf')) results.push(full);
  }
  return results;
}

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

function fmt(n) { return n.toLocaleString('es-CO'); }

// ─── Qdrant helpers ───────────────────────────────────────────────────────────

async function ensureCollection(qdrant) {
  const { collections } = await qdrant.getCollections();
  if (!collections.some(c => c.name === COLLECTION)) {
    await qdrant.createCollection(COLLECTION, {
      vectors: { size: EMBEDDING_DIM, distance: 'Cosine' }
    });
    console.log(`  Colección "${COLLECTION}" creada.`);
  }
}

async function isIndexed(qdrant, documentId) {
  try {
    const r = await qdrant.scroll(COLLECTION, {
      filter: { must: [{ key: 'document_id', match: { value: documentId } }] },
      limit: 1, with_payload: false, with_vector: false
    });
    return r.points.length > 0;
  } catch { return false; }
}

// ─── Main ─────────────────────────────────────────────────────────────────────

async function main() {
  if (!process.env.OPENAI_API_KEY) {
    console.error('ERROR: Falta OPENAI_API_KEY en .env');
    process.exit(1);
  }

  const openai = new OpenAI({ apiKey: process.env.OPENAI_API_KEY });
  const qdrant  = new QdrantClient({ url: QDRANT_URL, apiKey: QDRANT_KEY });

  await ensureCollection(qdrant);

  if (!fs.existsSync(UPLOADS_DIR)) {
    console.error(`ERROR: No existe la carpeta uploads/ en ${process.cwd()}`);
    process.exit(1);
  }

  // Determinar qué carpetas procesar
  const targetDir = process.env.BULK_PATH
    ? path.join(UPLOADS_DIR, process.env.BULK_PATH)
    : UPLOADS_DIR;

  const organos = fs.readdirSync(targetDir, { withFileTypes: true })
    .filter(e => e.isDirectory() && e.name !== 'Doctrina')
    .map(e => e.name);

  if (!organos.length) {
    console.error('No se encontraron subcarpetas en uploads/. Estructura esperada:\n  uploads/<Nombre del órgano>/<año>/*.pdf');
    process.exit(1);
  }

  const state = loadState();
  const filterThreshold = state.last_completed_at || 0;

  console.log(`\n=== Ingestión masiva ===`);
  console.log(`Órganos: ${organos.join(', ')}`);
  if (FORCE_FULL) {
    console.log(`Modo:    --force (ignorando state, procesando todo)`);
  } else if (filterThreshold > 0) {
    console.log(`Modo:    incremental (salta archivos anteriores al ${new Date(filterThreshold).toISOString()})`);
  } else {
    console.log(`Modo:    primera corrida (sin state previo, procesa todo y guarda state al terminar)`);
  }
  if (CONCURRENCY > 1) {
    console.log(`Concurrencia: ${CONCURRENCY} archivos en paralelo`);
  }
  console.log('');

  const startTime = Date.now();
  let totalFiles = 0, totalProcessed = 0, totalSkipped = 0, totalErrors = 0, totalChunks = 0;
  let totalMtimeSkipped = 0;
  let globalIndex = 0;

  for (const organo of organos) {
    const organoDir = path.join(targetDir, organo);
    const allPdfs = findPDFs(organoDir);

    // Filtro por mtime: saltamos archivos anteriores a la última corrida
    // exitosa (con alta probabilidad ya están indexados). En corridas con
    // --force, filterThreshold=0 → no se filtra nada.
    let pdfs = allPdfs;
    if (filterThreshold > 0) {
      pdfs = allPdfs.filter(p => {
        try { return fs.statSync(p).mtimeMs > filterThreshold; }
        catch { return true; }   // si statSync falla, mejor procesar que perder
      });
    }
    const mtimeSkipped = allPdfs.length - pdfs.length;
    totalMtimeSkipped += mtimeSkipped;
    totalFiles += pdfs.length;

    if (mtimeSkipped > 0) {
      console.log(`\n─── ${organo} (${fmt(pdfs.length)} a procesar de ${fmt(allPdfs.length)} totales, ${fmt(mtimeSkipped)} saltados por mtime) ───`);
    } else {
      console.log(`\n─── ${organo} (${fmt(pdfs.length)} PDFs) ───`);
    }

    if (pdfs.length === 0) continue;

    // Pre-filtrar: aplicar --skip=N (global cross-órgano) antes de armar la cola
    const work = [];
    for (let i = 0; i < pdfs.length; i++) {
      globalIndex++;
      if (globalIndex <= SKIP_N) {
        totalSkipped++;
        continue;
      }
      work.push({ filePath: pdfs[i], label: `[${i+1}/${pdfs.length}]` });
    }

    if (work.length === 0) continue;

    // Worker pool: con CONCURRENCY=1 es idéntico al loop secuencial original.
    // Con N>1, N workers consumen la cola en paralelo. Cada upsert se loggea
    // en una sola línea para que la salida concurrente sea legible.
    let cursor = 0;
    const workerFn = async () => {
      while (true) {
        const idx = cursor++;
        if (idx >= work.length) return;
        const { filePath, label } = work[idx];
        const filename  = path.basename(filePath);
        const shortName = filename.length > 45 ? filename.slice(0,42) + '...' : filename;
        const prefix    = `${label} ${shortName.padEnd(47)} `;

        let text;
        try {
          const buffer = fs.readFileSync(filePath);
          const parsed = await pdf(buffer);
          text = parsed.text?.trim() || '';
        } catch (e) {
          console.log(prefix + `ERROR lectura: ${e.message.slice(0,60)}`);
          totalErrors++;
          continue;
        }

        if (text.length < 50) {
          console.log(prefix + 'SALTADO (sin texto)');
          totalSkipped++;
          continue;
        }

        const documentId = hash(`${filename}:${text.slice(0, 10000)}`);

        if (await isIndexed(qdrant, documentId)) {
          console.log(prefix + 'ya indexado');
          totalSkipped++;
          continue;
        }

        const chunks = chunkText(text);

        const embeddings = [];
        try {
          for (let j = 0; j < chunks.length; j += EMB_BATCH) {
            const batch = chunks.slice(j, j + EMB_BATCH);
            const emb = await openai.embeddings.create({
              model: EMBEDDING_MODEL, input: batch, dimensions: EMBEDDING_DIM
            });
            embeddings.push(...emb.data.map(d => d.embedding));
            if (j + EMB_BATCH < chunks.length) await sleep(DELAY_MS);
          }
        } catch (e) {
          console.log(prefix + `ERROR OpenAI: ${e.message.slice(0,60)}`);
          totalErrors++;
          continue;
        }

        const filePath_rel = path.relative(UPLOADS_DIR, filePath);
        const points = chunks.map((chunk, idx) => ({
          id: hashToUUID(`${documentId}:${idx}`),
          vector: embeddings[idx],
          payload: {
            document_id : documentId,
            filename,
            file_path   : filePath_rel,
            organo,
            chunk_index : idx,
            text        : chunk.slice(0, 1200)
          }
        }));

        try {
          await qdrant.upsert(COLLECTION, { wait: true, points });
          totalChunks    += points.length;
          totalProcessed++;
          console.log(prefix + `${fmt(points.length)} fragmentos`);
        } catch (e) {
          console.log(prefix + `ERROR Qdrant: ${e.message.slice(0,60)}`);
          totalErrors++;
        }

        await sleep(FILE_DELAY_MS);
      }
    };

    await Promise.all(Array.from({ length: CONCURRENCY }, () => workerFn()));
  }

  const elapsed = ((Date.now() - startTime) / 1000 / 60).toFixed(1);
  console.log(`\n${'─'.repeat(60)}`);
  console.log(`RESUMEN`);
  console.log(`  PDFs a procesar  : ${fmt(totalFiles)}`);
  if (totalMtimeSkipped > 0) {
    console.log(`  Saltados por mtime: ${fmt(totalMtimeSkipped)}  (archivos antiguos sin leer)`);
  }
  console.log(`  Procesados       : ${fmt(totalProcessed)}`);
  console.log(`  Ya indexados     : ${fmt(totalSkipped)}`);
  console.log(`  Errores          : ${fmt(totalErrors)}`);
  console.log(`  Fragmentos totales: ${fmt(totalChunks)}`);
  console.log(`  Tiempo           : ${elapsed} min`);

  // Guardar state tras corrida exitosa — próxima corrida salta lo viejo.
  // Solo guardamos si no hubo --force y si no hubo errores graves (si todos
  // los archivos fallaron no queremos cachear un state malo que impida retry).
  const hadHardFailure = totalFiles > 0 && totalProcessed === 0 && totalErrors > 0 && totalSkipped === 0;
  if (!hadHardFailure) {
    saveState({
      last_completed_at: Date.now(),
      last_resumen: {
        pdfs_a_procesar: totalFiles,
        saltados_por_mtime: totalMtimeSkipped,
        procesados: totalProcessed,
        ya_indexados: totalSkipped,
        errores: totalErrors,
        fragmentos_totales: totalChunks
      }
    });
    console.log(`\n  State guardado en ${path.relative(process.cwd(), STATE_FILE)} — próxima corrida salta archivos anteriores.`);
  }
}

main().catch(e => { console.error('\nERROR FATAL:', e.message); process.exit(1); });
