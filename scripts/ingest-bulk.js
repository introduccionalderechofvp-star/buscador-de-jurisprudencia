/**
 * ingest-bulk.js
 *
 * Ingesta masiva de PDFs desde la carpeta uploads/.
 * - Cada subcarpeta de primer nivel = nombre del órgano judicial.
 * - Busca PDFs recursivamente en subcarpetas de año u otras.
 * - Detecta duplicados: omite archivos ya indexados en Qdrant.
 * - Muestra progreso en tiempo real.
 *
 * Uso:
 *   node scripts/ingest-bulk.js
 *
 * Opciones de entorno (en .env):
 *   BULK_ORGANO   — fuerza un órgano específico ignorando el nombre de carpeta
 *   BULK_PATH     — procesa solo esta subcarpeta dentro de uploads/
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
const DELAY_MS    = 150;  // pausa entre lotes (evitar rate-limit)

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
    .filter(e => e.isDirectory())
    .map(e => e.name);

  if (!organos.length) {
    console.error('No se encontraron subcarpetas en uploads/. Estructura esperada:\n  uploads/<Nombre del órgano>/<año>/*.pdf');
    process.exit(1);
  }

  console.log(`\n=== Ingestión masiva ===`);
  console.log(`Órganos: ${organos.join(', ')}\n`);

  const startTime = Date.now();
  let totalFiles = 0, totalProcessed = 0, totalSkipped = 0, totalErrors = 0, totalChunks = 0;

  for (const organo of organos) {
    const organoDir = path.join(targetDir, organo);
    const pdfs = findPDFs(organoDir);
    totalFiles += pdfs.length;

    console.log(`\n─── ${organo} (${fmt(pdfs.length)} PDFs) ───`);

    for (let i = 0; i < pdfs.length; i++) {
      const filePath  = pdfs[i];
      const filename  = path.basename(filePath);
      const fileLabel = `[${i+1}/${pdfs.length}]`;
      const shortName = filename.length > 45 ? filename.slice(0,42) + '...' : filename;

      process.stdout.write(`${fileLabel} ${shortName.padEnd(47)} `);

      // Parse PDF
      let text;
      try {
        const buffer = fs.readFileSync(filePath);
        const parsed = await pdf(buffer);
        text = parsed.text?.trim() || '';
      } catch (e) {
        console.log(`ERROR lectura: ${e.message.slice(0,60)}`);
        totalErrors++;
        continue;
      }

      if (text.length < 50) {
        console.log('SALTADO (sin texto)');
        totalSkipped++;
        continue;
      }

      const documentId = hash(`${filename}:${text.slice(0, 10000)}`);

      if (await isIndexed(qdrant, documentId)) {
        console.log('ya indexado');
        totalSkipped++;
        continue;
      }

      const chunks = chunkText(text);

      // Embeddings en sub-lotes
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
        console.log(`ERROR OpenAI: ${e.message.slice(0,60)}`);
        totalErrors++;
        continue;
      }

      // Construir puntos
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

      // Upsert a Qdrant
      try {
        await qdrant.upsert(COLLECTION, { wait: true, points });
        totalChunks    += points.length;
        totalProcessed++;
        console.log(`${fmt(points.length)} fragmentos`);
      } catch (e) {
        console.log(`ERROR Qdrant: ${e.message.slice(0,60)}`);
        totalErrors++;
      }

      await sleep(100);
    }
  }

  const elapsed = ((Date.now() - startTime) / 1000 / 60).toFixed(1);
  console.log(`\n${'─'.repeat(60)}`);
  console.log(`RESUMEN`);
  console.log(`  PDFs encontrados : ${fmt(totalFiles)}`);
  console.log(`  Procesados       : ${fmt(totalProcessed)}`);
  console.log(`  Ya indexados     : ${fmt(totalSkipped)}`);
  console.log(`  Errores          : ${fmt(totalErrors)}`);
  console.log(`  Fragmentos totales: ${fmt(totalChunks)}`);
  console.log(`  Tiempo           : ${elapsed} min`);
}

main().catch(e => { console.error('\nERROR FATAL:', e.message); process.exit(1); });
