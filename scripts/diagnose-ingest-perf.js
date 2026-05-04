/**
 * diagnose-ingest-perf.js
 *
 * Mide la latencia de cada paso del ingest sobre N archivos de muestra,
 * sin escribir nada definitivo. Sirve para localizar el cuello de botella.
 *
 * Uso:
 *   node scripts/diagnose-ingest-perf.js [carpeta] [N]
 *
 *   carpeta : ruta dentro de uploads/ a muestrear (default: primera subcarpeta)
 *   N       : cantidad de archivos a probar (default: 5)
 */

import 'dotenv/config';
import fs from 'fs';
import path from 'path';
import OpenAI from 'openai';
import { QdrantClient } from '@qdrant/js-client-rest';
import crypto from 'crypto';

const UPLOADS_DIR    = path.join(process.cwd(), 'uploads');
const COLLECTION     = process.env.QDRANT_COLLECTION  || 'sentencias';
const EMBEDDING_MODEL= process.env.EMBEDDING_MODEL    || 'text-embedding-3-large';
const EMBEDDING_DIM  = Number(process.env.EMBEDDING_DIM || 3072);
const QDRANT_URL     = process.env.QDRANT_URL         || 'http://localhost:6333';
const QDRANT_KEY     = process.env.QDRANT_API_KEY     || undefined;

function hash(s) {
  return crypto.createHash('sha256').update(s).digest('hex').slice(0, 32);
}

function chunkText(text, size = 500, overlap = 100) {
  const words = text.split(/\s+/);
  const chunks = [];
  for (let i = 0; i < words.length; i += (size - overlap)) {
    chunks.push(words.slice(i, i + size).join(' '));
    if (i + size >= words.length) break;
  }
  return chunks;
}

async function timed(label, fn) {
  const t0 = Date.now();
  let result, error = null;
  try { result = await fn(); }
  catch (e) { error = e; }
  const ms = Date.now() - t0;
  return { label, ms, result, error };
}

async function main() {
  const targetDir = process.argv[2]
    ? path.join(UPLOADS_DIR, process.argv[2])
    : null;
  const N = Number(process.argv[3] || 5);

  // Resolver carpeta de muestreo
  let sampleDir;
  if (targetDir && fs.existsSync(targetDir)) {
    sampleDir = targetDir;
  } else {
    const subs = fs.readdirSync(UPLOADS_DIR, { withFileTypes: true })
      .filter(e => e.isDirectory())
      .map(e => e.name);
    sampleDir = path.join(UPLOADS_DIR, subs[0]);
  }
  console.log(`Carpeta muestra: ${sampleDir}`);

  // Recolectar N archivos .md/.pdf/.txt
  const allFiles = [];
  function walk(dir) {
    for (const e of fs.readdirSync(dir, { withFileTypes: true })) {
      const full = path.join(dir, e.name);
      if (e.isDirectory()) walk(full);
      else if (/\.(md|txt|pdf)$/i.test(e.name)) allFiles.push(full);
    }
  }
  walk(sampleDir);
  const sample = allFiles.slice(0, N);
  console.log(`Archivos a probar: ${sample.length} (de ${allFiles.length} total)\n`);

  const openai = new OpenAI({ apiKey: process.env.OPENAI_API_KEY });
  const qdrant = new QdrantClient({ url: QDRANT_URL, apiKey: QDRANT_KEY });

  // Sumas globales
  const totals = { read: 0, isIndexed: 0, embed: 0, upsertDryRun: 0, total: 0 };
  let indexedCount = 0;

  for (let i = 0; i < sample.length; i++) {
    const filePath = sample[i];
    const filename = path.basename(filePath);
    console.log(`[${i+1}/${sample.length}] ${filename.slice(0, 60)}`);

    const fileT0 = Date.now();

    // 1. Read
    const r1 = await timed('read', async () => {
      return fs.promises.readFile(filePath, 'utf8');
    });
    const text = r1.result;

    // 2. Hash
    const documentId = hash(`${filename}:${text.slice(0, 10000)}`);

    // 3. isIndexed
    const r2 = await timed('isIndexed', async () => {
      const r = await qdrant.scroll(COLLECTION, {
        filter: { must: [{ key: 'document_id', match: { value: documentId } }] },
        limit: 1, with_payload: false, with_vector: false
      });
      return r.points.length > 0;
    });
    const alreadyIndexed = r2.result;
    if (alreadyIndexed) indexedCount++;

    // 4. Chunk + Embed (1 batch de hasta 20 chunks)
    const chunks = chunkText(text);
    const batch = chunks.slice(0, 20);
    const r3 = await timed('embed (1 batch)', async () => {
      return openai.embeddings.create({
        model: EMBEDDING_MODEL, input: batch, dimensions: EMBEDDING_DIM
      });
    });

    // 5. Upsert dry-run a colección temporal — NO toca prod
    const r4 = await timed('upsert (a colección temporal)', async () => {
      const tmpColl = '__perf_diag__';
      try {
        await qdrant.createCollection(tmpColl, {
          vectors: { size: EMBEDDING_DIM, distance: 'Cosine' }
        });
      } catch {} // ya existe
      const points = batch.map((c, idx) => ({
        id: idx + 1,
        vector: r3.result.data[idx].embedding,
        payload: { _: 1 }
      }));
      await qdrant.upsert(tmpColl, { wait: true, points });
    });

    const fileTotal = Date.now() - fileT0;
    totals.read         += r1.ms;
    totals.isIndexed    += r2.ms;
    totals.embed        += r3.ms;
    totals.upsertDryRun += r4.ms;
    totals.total        += fileTotal;

    console.log(`   read         : ${r1.ms.toString().padStart(6)} ms`);
    console.log(`   isIndexed    : ${r2.ms.toString().padStart(6)} ms ${alreadyIndexed ? '(ya en Qdrant)' : '(nuevo)'}`);
    console.log(`   embed batch  : ${r3.ms.toString().padStart(6)} ms (${batch.length} chunks, ${chunks.length} totales)`);
    console.log(`   upsert (tmp) : ${r4.ms.toString().padStart(6)} ms (${batch.length} points)`);
    console.log(`   TOTAL        : ${fileTotal.toString().padStart(6)} ms\n`);
  }

  // Limpiar colección temporal
  try { await qdrant.deleteCollection('__perf_diag__'); } catch {}

  console.log('─'.repeat(60));
  console.log('PROMEDIOS:');
  const n = sample.length;
  console.log(`   read         : ${(totals.read/n).toFixed(0).padStart(6)} ms`);
  console.log(`   isIndexed    : ${(totals.isIndexed/n).toFixed(0).padStart(6)} ms`);
  console.log(`   embed batch  : ${(totals.embed/n).toFixed(0).padStart(6)} ms`);
  console.log(`   upsert       : ${(totals.upsertDryRun/n).toFixed(0).padStart(6)} ms`);
  console.log(`   TOTAL/file   : ${(totals.total/n).toFixed(0).padStart(6)} ms`);
  console.log('');
  console.log(`Ya indexados: ${indexedCount}/${n}`);
  console.log('');
  console.log('Comparativa esperada vs medida:');
  console.log(`  Tiempo real promedio del ingest actual: ~28000 ms/archivo (28s)`);
  console.log(`  Tiempo medido aquí: ${(totals.total/n).toFixed(0)} ms/archivo`);
  console.log('');
  console.log('Si la suma de pasos es <<28s, el cuello está en concurrencia/serialización del worker.');
  console.log('Si un paso domina (>5s), ese es el cuello — investigar índices/quotas.');
}

main().catch(e => {
  console.error('FALLO:', e);
  process.exit(1);
});
