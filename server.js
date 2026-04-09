import 'dotenv/config';
import express from 'express';
import multer from 'multer';
import pdf from 'pdf-parse';
import OpenAI from 'openai';
import { QdrantClient } from '@qdrant/js-client-rest';
import crypto from 'crypto';
import path from 'path';
import fs from 'fs';

const app = express();
const port = process.env.PORT || 3000;
const upload = multer({ storage: multer.memoryStorage(), limits: { fileSize: 30 * 1024 * 1024 } });

const openai = new OpenAI({ apiKey: process.env.OPENAI_API_KEY });
const qdrant = new QdrantClient({ url: process.env.QDRANT_URL, apiKey: process.env.QDRANT_API_KEY || undefined });

const COLLECTION = process.env.QDRANT_COLLECTION || 'sentencias';
const EMBEDDING_MODEL = process.env.EMBEDDING_MODEL || 'text-embedding-3-large';
const EMBEDDING_DIM = Number(process.env.EMBEDDING_DIM || 3072);
const UPLOADS_DIR = path.join(process.cwd(), 'uploads');

fs.mkdirSync(UPLOADS_DIR, { recursive: true });

app.use(express.json({ limit: '10mb' }));
app.use(express.static('public'));

function hash(content) {
  return crypto.createHash('sha256').update(content).digest('hex');
}

// Qdrant only accepts UUIDs or unsigned integers as point IDs.
// Derive a deterministic UUID v4 from any string.
function hashToUUID(content) {
  const h = crypto.createHash('sha256').update(content).digest('hex');
  return `${h.slice(0,8)}-${h.slice(8,12)}-4${h.slice(13,16)}-${(parseInt(h.slice(16,18),16)&0x3f|0x80).toString(16)}${h.slice(18,20)}-${h.slice(20,32)}`;
}

function chunkText(text, chunkSize = 500, overlap = 100) {
  const words = text.split(/\s+/).filter(Boolean);
  const chunks = [];
  let start = 0;
  while (start < words.length) {
    const end = Math.min(start + chunkSize, words.length);
    chunks.push(words.slice(start, end).join(' '));
    if (end >= words.length) break;
    start += chunkSize - overlap;
  }
  return chunks;
}

async function ensureCollection() {
  const collections = await qdrant.getCollections();
  const exists = collections.collections.some((c) => c.name === COLLECTION);
  if (!exists) {
    await qdrant.createCollection(COLLECTION, {
      vectors: { size: EMBEDDING_DIM, distance: 'Cosine' }
    });
  }
}

app.get('/api/download', (req, res) => {
  const rel = decodeURIComponent(req.query.path || '').replace(/\.\./g, '').replace(/^[/\\]/, '');
  if (!rel) return res.status(400).json({ error: 'Falta el parámetro path.' });
  const filePath = path.resolve(UPLOADS_DIR, rel);
  if (!filePath.startsWith(UPLOADS_DIR + path.sep) && filePath !== UPLOADS_DIR) {
    return res.status(403).json({ error: 'Acceso denegado.' });
  }
  res.download(filePath, path.basename(filePath), err => {
    if (err && !res.headersSent) res.status(404).json({ error: 'Archivo no encontrado.' });
  });
});

app.get('/api/health', async (_req, res) => {
  try {
    await ensureCollection();
    res.json({ ok: true, collection: COLLECTION });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

app.post('/api/ingest', upload.array('pdfs', 20), async (req, res) => {
  const organo = (req.body.organo || 'No especificado').trim();
  if (!req.files?.length) return res.status(400).json({ error: 'No se recibieron PDFs.' });

  try {
    await ensureCollection();
    const summary = [];

    for (const file of req.files) {
      const parsed = await pdf(file.buffer);
      const text = parsed.text?.trim() || '';
      if (text.length < 50) {
        summary.push({ file: file.originalname, indexed: 0, skipped: 'Texto insuficiente' });
        continue;
      }

      const documentId = hash(`${file.originalname}:${text.slice(0, 10000)}`);
      const chunks = chunkText(text);

      const emb = await openai.embeddings.create({
        model: EMBEDDING_MODEL,
        input: chunks,
        dimensions: EMBEDDING_DIM
      });

      const filePath = path.join(organo, file.originalname);
      const points = chunks.map((chunk, i) => ({
        id: hashToUUID(`${documentId}:${i}`),
        vector: emb.data[i].embedding,
        payload: {
          document_id: documentId,
          filename: file.originalname,
          file_path: filePath,
          organo,
          chunk_index: i,
          text: chunk.slice(0, 1200)
        }
      }));

      await qdrant.upsert(COLLECTION, { wait: true, points });

      // Persist file to disk so it can be downloaded later
      const organoDir = path.join(UPLOADS_DIR, organo);
      fs.mkdirSync(organoDir, { recursive: true });
      fs.writeFileSync(path.join(organoDir, file.originalname), file.buffer);

      summary.push({ file: file.originalname, indexed: points.length, organo });
    }

    res.json({ ok: true, summary });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

app.post('/api/search', async (req, res) => {
  const { query, organo, limit = 10 } = req.body || {};
  if (!query?.trim()) return res.status(400).json({ error: 'La consulta está vacía.' });

  try {
    await ensureCollection();
    const emb = await openai.embeddings.create({
      model: EMBEDDING_MODEL,
      input: [query],
      dimensions: EMBEDDING_DIM
    });

    const filter = organo && organo !== 'TODAS'
      ? { must: [{ key: 'organo', match: { value: organo } }] }
      : undefined;

    const results = await qdrant.search(COLLECTION, {
      vector: emb.data[0].embedding,
      limit: Number(limit),
      filter,
      with_payload: true
    });

    res.json({
      ok: true,
      results: results.map(r => ({ score: r.score, ...r.payload }))
    });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

app.listen(port, () => console.log(`Servidor en http://localhost:${port}`));
