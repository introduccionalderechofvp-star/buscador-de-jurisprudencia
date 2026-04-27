/**
 * ingest-doctrina.js
 *
 * Ingesta de libros de doctrina jurídica (PDF) a Qdrant.
 * Pipeline: OCR (si necesario) → clasificación (Haiku) → chunking → embeddings → Qdrant
 *
 * Los libros van en uploads/Doctrina/ (plano o con subcarpetas).
 * La clasificación (materia, jurisdicción, autor, título) se hace con
 * Claude Haiku y se cachea en .doctrina-clasificaciones.json.
 *
 * Uso:
 *   node scripts/ingest-doctrina.js              # incremental
 *   node scripts/ingest-doctrina.js --force       # reprocesar todo
 *   node scripts/ingest-doctrina.js --skip-ocr    # saltar paso de OCR
 *   node scripts/ingest-doctrina.js --classify-only  # solo clasificar, sin embeddings
 *
 * Recovery quirúrgico:
 *   FILES_LIST=/tmp/lista.txt node scripts/ingest-doctrina.js
 *   (donde lista.txt tiene un basename de PDF por línea — solo esos se procesan)
 */

import 'dotenv/config';
import fs from 'fs';
import path from 'path';
import crypto from 'crypto';
import { execSync, spawnSync } from 'child_process';
import pdfParse from 'pdf-parse';
import OpenAI from 'openai';
import { QdrantClient } from '@qdrant/js-client-rest';
import Anthropic from '@anthropic-ai/sdk';

// ─── Config ──────────────────────────────────────────────────────────────────

const UPLOADS_DIR    = path.join(process.cwd(), 'uploads');
const DOCTRINA_DIR   = path.join(UPLOADS_DIR, 'Doctrina');
const CACHE_FILE     = path.join(process.cwd(), '.doctrina-clasificaciones.json');
const STATE_FILE     = path.join(process.cwd(), '.ingest-state-doctrina.json');
const COLLECTION     = process.env.QDRANT_COLLECTION  || 'sentencias';
const EMBEDDING_MODEL= process.env.EMBEDDING_MODEL    || 'text-embedding-3-large';
const EMBEDDING_DIM  = Number(process.env.EMBEDDING_DIM || 3072);
const QDRANT_URL     = process.env.QDRANT_URL         || 'http://localhost:6333';
const QDRANT_KEY     = process.env.QDRANT_API_KEY     || undefined;

const CHUNK_SIZE    = 500;
const CHUNK_OVERLAP = 100;
const EMB_BATCH     = 20;
const DELAY_MS      = Number(process.env.INGEST_DELAY_MS || 150);
const FILE_DELAY_MS = Number(process.env.INGEST_FILE_DELAY_MS || 500);

const FORCE_FULL    = process.argv.includes('--force')    || process.env.INGEST_FORCE === 'true';
const SKIP_OCR      = process.argv.includes('--skip-ocr');
const CLASSIFY_ONLY = process.argv.includes('--classify-only');

// ─── Helpers ─────────────────────────────────────────────────────────────────

function hash(s) {
  return crypto.createHash('sha256').update(s).digest('hex');
}

function hashToUUID(s) {
  const h = crypto.createHash('sha256').update(s).digest('hex');
  return `${h.slice(0,8)}-${h.slice(8,12)}-4${h.slice(13,16)}-` +
    `${(parseInt(h.slice(16,18),16)&0x3f|0x80).toString(16)}${h.slice(18,20)}-${h.slice(20,32)}`;
}

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }
function fmt(n)    { return n.toLocaleString('es-CO'); }

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
  // Cap por caracteres: si el OCR mete bloques sin espacios, una "palabra"
  // gigante puede hacer que un chunk supere los 8192 tokens de OpenAI.
  // Para texto OCR-eado en español, hemos visto que algunos chunks de 6000
  // chars producen >8192 tokens (1.4 t/c) por contenido hostil al tokenizer
  // (números seguidos, símbolos, fragmentos sin separación). Bajamos a 3000
  // (~6000 tokens worst-case) que da margen seguro.
  const MAX_CHUNK_CHARS = 3000;
  const capped = [];
  for (const c of chunks) {
    if (c.length <= MAX_CHUNK_CHARS) capped.push(c);
    else for (let i = 0; i < c.length; i += MAX_CHUNK_CHARS) capped.push(c.slice(i, i + MAX_CHUNK_CHARS));
  }
  return capped;
}

// Reintenta operaciones cuando el error es transitorio (fetch failed, ECONNRESET,
// timeout, etc). NO reintenta errores 4xx legítimos como "Bad Request" o "input too long".
async function withRetry(fn, maxAttempts = 3) {
  let lastErr;
  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    try { return await fn(); }
    catch (e) {
      lastErr = e;
      const msg = String(e?.message || e);
      const transient = /fetch failed|ECONNRESET|ETIMEDOUT|EAI_AGAIN|timeout|socket hang up|503|502|504/i.test(msg);
      if (!transient || attempt === maxAttempts) throw e;
      await new Promise(r => setTimeout(r, 2000 * Math.pow(2, attempt - 1)));
    }
  }
  throw lastErr;
}

function findPDFs(dir) {
  const results = [];
  if (!fs.existsSync(dir)) return results;
  for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
    if (entry.name.startsWith('.') || entry.name.startsWith('._')) continue;
    const full = path.join(dir, entry.name);
    if (entry.isDirectory()) results.push(...findPDFs(full));
    else if (entry.isFile() && entry.name.toLowerCase().endsWith('.pdf')) results.push(full);
  }
  return results;
}

// ─── Estado persistente ──────────────────────────────────────────────────────

function loadState() {
  if (FORCE_FULL) return { last_completed_at: 0 };
  try { return JSON.parse(fs.readFileSync(STATE_FILE, 'utf8')); }
  catch { return { last_completed_at: 0 }; }
}

function saveState(state) {
  const tmp = STATE_FILE + '.tmp';
  fs.writeFileSync(tmp, JSON.stringify(state, null, 2));
  fs.renameSync(tmp, STATE_FILE);
}

// ─── Caché de clasificaciones ────────────────────────────────────────────────

function loadCache() {
  try { return JSON.parse(fs.readFileSync(CACHE_FILE, 'utf8')); }
  catch { return {}; }
}

function saveCache(cache) {
  const tmp = CACHE_FILE + '.tmp';
  fs.writeFileSync(tmp, JSON.stringify(cache, null, 2));
  fs.renameSync(tmp, CACHE_FILE);
}

// ─── Qdrant ──────────────────────────────────────────────────────────────────

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

// ─── OCR ─────────────────────────────────────────────────────────────────────

function checkOCRAvailable() {
  const r = spawnSync('ocrmypdf', ['--version'], { stdio: 'pipe' });
  if (r.status !== 0) {
    console.error('ERROR: ocrmypdf no está instalado.');
    console.error('  Instálalo con: apt install ocrmypdf tesseract-ocr-spa');
    process.exit(1);
  }
}

function runOCR(pdfPath) {
  try {
    execSync(
      `ocrmypdf --skip-text --language spa "${pdfPath}" "${pdfPath}"`,
      { stdio: 'pipe', timeout: 600_000 }
    );
    return 'ocr-applied';
  } catch (e) {
    const stderr = e.stderr?.toString() || '';
    if (e.status === 6 || stderr.includes('already has text'))
      return 'already-has-text';
    if (stderr.includes('PriorOcrFoundError'))
      return 'already-has-text';
    console.error(`  [ocr warn] ${stderr.slice(0, 120)}`);
    return 'error';
  }
}

// ─── Clasificación con Claude Haiku ──────────────────────────────────────────

async function classifyBook(anthropic, text, filename) {
  const preview = text.slice(0, 4000);
  const msg = await anthropic.messages.create({
    model: 'claude-haiku-4-5-20251001',
    max_tokens: 300,
    messages: [{
      role: 'user',
      content:
        `Clasifica este libro jurídico. Responde SOLO con un JSON válido ` +
        `(sin markdown, sin backticks, sin explicaciones):\n` +
        `{"materia": "<Penal|Civil|Procesal|Laboral|Constitucional|` +
        `Administrativo|Comercial|Teoría del Derecho|Internacional|Familia|Otro>",` +
        ` "jurisdiccion": "<Colombia|España|Argentina|México|General|Otro>",` +
        ` "autor": "<nombre(s) del autor(es)>",` +
        ` "titulo": "<título del libro>"}\n\n` +
        `Archivo: ${filename}\n\nPrimeras páginas:\n${preview}`
    }]
  });

  const raw = msg.content[0].text.trim();
  try {
    return JSON.parse(raw);
  } catch {
    const match = raw.match(/\{[\s\S]*\}/);
    if (match) return JSON.parse(match[0]);
    throw new Error(`Clasificación no-JSON: ${raw.slice(0, 200)}`);
  }
}

// ─── Main ────────────────────────────────────────────────────────────────────

async function main() {
  console.log('\n=== Ingestión de doctrina jurídica ===');

  if (!fs.existsSync(DOCTRINA_DIR)) {
    fs.mkdirSync(DOCTRINA_DIR, { recursive: true });
    console.log(`Directorio creado: ${DOCTRINA_DIR}`);
    console.log('Sube tus PDFs ahí y vuelve a correr este script.');
    return;
  }

  if (!SKIP_OCR) checkOCRAvailable();

  if (!process.env.OPENAI_API_KEY && !CLASSIFY_ONLY) {
    console.error('ERROR: Falta OPENAI_API_KEY en .env');
    process.exit(1);
  }
  if (!process.env.ANTHROPIC_API_KEY) {
    console.error('ERROR: Falta ANTHROPIC_API_KEY en .env (necesaria para clasificación)');
    process.exit(1);
  }

  const openai     = CLASSIFY_ONLY ? null : new OpenAI({ apiKey: process.env.OPENAI_API_KEY });
  const qdrant     = CLASSIFY_ONLY ? null : new QdrantClient({ url: QDRANT_URL, apiKey: QDRANT_KEY });
  const anthropic  = new Anthropic();

  if (!CLASSIFY_ONLY) await ensureCollection(qdrant);

  // Encontrar PDFs
  const allPdfs = findPDFs(DOCTRINA_DIR);
  if (allPdfs.length === 0) {
    console.log(`No se encontraron PDFs en ${DOCTRINA_DIR}`);
    return;
  }

  // Filtro mtime
  const state = loadState();
  const threshold = state.last_completed_at || 0;
  let pdfs = allPdfs;
  let mtimeSkipped = 0;
  if (threshold > 0 && !CLASSIFY_ONLY) {
    pdfs = allPdfs.filter(p => {
      try { return fs.statSync(p).mtimeMs > threshold; }
      catch { return true; }
    });
    mtimeSkipped = allPdfs.length - pdfs.length;
  }

  // Filtro por lista de archivos específicos (recovery quirúrgico).
  // FILES_LIST = ruta a un archivo de texto con un basename de PDF por línea.
  // Si está set, ignora todo lo demás y solo procesa esos.
  const FILES_LIST = process.env.FILES_LIST;
  if (FILES_LIST) {
    if (!fs.existsSync(FILES_LIST)) {
      console.error(`ERROR: FILES_LIST apunta a un archivo que no existe: ${FILES_LIST}`);
      process.exit(1);
    }
    const wanted = new Set(
      fs.readFileSync(FILES_LIST, 'utf8')
        .split('\n').map(s => s.trim()).filter(Boolean)
    );
    const before = pdfs.length;
    pdfs = allPdfs.filter(p => wanted.has(path.basename(p)));
    mtimeSkipped = 0; // FILES_LIST anula el mtime filter
    console.log(`FILES_LIST: ${wanted.size} pedidos, ${pdfs.length} encontrados en disco`);
  }

  console.log(`PDFs encontrados : ${fmt(allPdfs.length)} total, ${fmt(pdfs.length)} a procesar`);
  if (mtimeSkipped) console.log(`Saltados por mtime: ${fmt(mtimeSkipped)}`);
  console.log(`Modo  : ${FORCE_FULL ? '--force' : 'incremental'}${CLASSIFY_ONLY ? ' (solo clasificación)' : ''}`);
  console.log(`OCR   : ${SKIP_OCR ? 'deshabilitado' : 'habilitado'}`);
  console.log();

  const cache = loadCache();
  const startTime = Date.now();
  let processed = 0, skipped = 0, errors = 0, totalChunks = 0;
  let ocrApplied = 0, ocrHadText = 0, ocrErrors = 0;

  for (let i = 0; i < pdfs.length; i++) {
    const pdfPath  = pdfs[i];
    const filename = path.basename(pdfPath);
    const relPath  = path.relative(UPLOADS_DIR, pdfPath);
    const prefix   = `[${i + 1}/${pdfs.length}]`;
    const shortName = filename.length > 42 ? filename.slice(0, 39) + '...' : filename;

    try {
      // ── OCR ──
      if (!SKIP_OCR) {
        const ocrResult = runOCR(pdfPath);
        if (ocrResult === 'ocr-applied')     ocrApplied++;
        else if (ocrResult === 'already-has-text') ocrHadText++;
        else                                  ocrErrors++;
      }

      // ── Extraer texto ──
      const buf  = fs.readFileSync(pdfPath);
      const data = await pdfParse(buf);
      const text = data.text?.trim() || '';

      if (text.length < 50) {
        console.log(`${prefix} ${shortName}`.padEnd(60) + 'texto insuficiente — saltado');
        errors++;
        continue;
      }

      // ── Clasificación ──
      let clasif = cache[filename];
      if (!clasif) {
        clasif = await classifyBook(anthropic, text, filename);
        cache[filename] = clasif;
        saveCache(cache);
      }

      if (CLASSIFY_ONLY) {
        console.log(
          `${prefix} ${shortName}`.padEnd(60) +
          `${clasif.materia} · ${clasif.jurisdiccion} · ${clasif.autor}`
        );
        processed++;
        continue;
      }

      // ── Dedup ──
      const documentId = hash(`${filename}:${text.slice(0, 10000)}`);
      if (await isIndexed(qdrant, documentId)) {
        process.stdout.write(`${prefix} ${shortName}`.padEnd(60) + 'ya indexado\n');
        skipped++;
        continue;
      }

      // ── Chunking ──
      const chunks = chunkText(text);
      totalChunks += chunks.length;

      // ── Embeddings ──
      // Estrategia: intenta el batch normal. Si OpenAI rechaza (400) un input
      // por exceder 8192 tokens (contenido OCR-eado con ratio extremo
      // tokens/char), cae a procesar el batch chunk por chunk y skippea
      // solo los problemáticos. Así un libro no pierde toda su indexación
      // por un solo chunk patológico.
      const embeddings = [];
      const droppedIndices = [];
      for (let j = 0; j < chunks.length; j += EMB_BATCH) {
        const batch = chunks.slice(j, j + EMB_BATCH);
        try {
          const emb = await withRetry(() => openai.embeddings.create({
            model: EMBEDDING_MODEL, input: batch, dimensions: EMBEDDING_DIM
          }));
          embeddings.push(...emb.data.map(d => d.embedding));
        } catch (eBatch) {
          // Fallback: uno por uno, marcar nulls los que fallen
          for (let k = 0; k < batch.length; k++) {
            try {
              const emb1 = await withRetry(() => openai.embeddings.create({
                model: EMBEDDING_MODEL, input: [batch[k]], dimensions: EMBEDDING_DIM
              }));
              embeddings.push(emb1.data[0].embedding);
            } catch (eOne) {
              embeddings.push(null);
              droppedIndices.push(j + k);
            }
          }
        }
        if (j + EMB_BATCH < chunks.length) await sleep(DELAY_MS);
      }

      if (droppedIndices.length === chunks.length) {
        // Todos los chunks fallaron individualmente → libro irrecuperable
        throw new Error(`todos los ${chunks.length} chunks rechazados por OpenAI`);
      }

      // ── Puntos para Qdrant ──
      // Filtrar chunks sin embedding (los que fallaron individualmente)
      const points = chunks.map((chunk, idx) => ({
        idx, chunk,
        embedding: embeddings[idx]
      })).filter(p => p.embedding !== null).map(p => ({
        id: hashToUUID(`${documentId}:${p.idx}`),
        vector: p.embedding,
        payload: {
          document_id:   documentId,
          filename,
          file_path:     relPath,
          organo:        'Doctrina',
          tipo:          'doctrina',
          materia:       clasif.materia       || 'Sin clasificar',
          jurisdiccion:  clasif.jurisdiccion  || 'Sin clasificar',
          autor:         clasif.autor         || 'Desconocido',
          titulo_libro:  clasif.titulo        || filename.replace(/\.pdf$/i, ''),
          chunk_index:   p.idx,
          text:          p.chunk.slice(0, 1200)
        }
      }));

      // ── Upsert ──
      await withRetry(() => qdrant.upsert(COLLECTION, { wait: true, points }));
      processed++;
      const droppedNote = droppedIndices.length > 0 ? ` (${droppedIndices.length} chunks rechazados)` : '';
      console.log(
        `${prefix} ${shortName}`.padEnd(60) +
        `${fmt(points.length)} frag  [${clasif.materia} · ${clasif.jurisdiccion}]${droppedNote}`
      );

      await sleep(FILE_DELAY_MS);
    } catch (e) {
      errors++;
      console.error(`${prefix} ${shortName}`.padEnd(60) + `ERROR: ${e.message.slice(0, 80)}`);
    }
  }

  // ── Resumen ──
  const elapsed = ((Date.now() - startTime) / 60_000).toFixed(1);
  console.log('\n' + '─'.repeat(60));
  console.log('RESUMEN');
  console.log(`  PDFs a procesar  : ${fmt(pdfs.length)}`);
  if (mtimeSkipped) console.log(`  Saltados por mtime: ${fmt(mtimeSkipped)}`);
  if (!SKIP_OCR) {
    console.log(`  OCR aplicado     : ${ocrApplied}  ya tenían texto: ${ocrHadText}  errores: ${ocrErrors}`);
  }
  console.log(`  Procesados       : ${fmt(processed)}`);
  if (!CLASSIFY_ONLY) {
    console.log(`  Ya indexados     : ${fmt(skipped)}`);
    console.log(`  Errores          : ${fmt(errors)}`);
    console.log(`  Fragmentos totales: ${fmt(totalChunks)}`);
  }
  console.log(`  Tiempo           : ${elapsed} min`);

  if (!CLASSIFY_ONLY) {
    const hadHardFailure = pdfs.length > 0 && processed === 0 && errors > 0 && skipped === 0;
    if (!hadHardFailure) {
      saveState({
        last_completed_at: Date.now(),
        last_resumen: {
          pdfs_a_procesar: pdfs.length,
          procesados: processed,
          ya_indexados: skipped,
          errores: errors,
          fragmentos_totales: totalChunks
        }
      });
      console.log(`\n  State guardado en ${path.basename(STATE_FILE)}`);
    }
  }
}

main().catch(e => { console.error('\nERROR FATAL:', e.message); process.exit(1); });
