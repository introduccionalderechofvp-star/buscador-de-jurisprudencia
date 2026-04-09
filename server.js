import 'dotenv/config';
import express from 'express';
import multer from 'multer';
import pdf from 'pdf-parse';
import OpenAI from 'openai';
import Anthropic from '@anthropic-ai/sdk';
import { QdrantClient } from '@qdrant/js-client-rest';
import crypto from 'crypto';
import path from 'path';
import fs from 'fs';

const app = express();
const port = process.env.PORT || 3000;
const upload = multer({ storage: multer.memoryStorage(), limits: { fileSize: 30 * 1024 * 1024 } });

const openai    = new OpenAI({ apiKey: process.env.OPENAI_API_KEY });
const qdrant    = new QdrantClient({ url: process.env.QDRANT_URL, apiKey: process.env.QDRANT_API_KEY || undefined });
const anthropic = process.env.ANTHROPIC_API_KEY
  ? new Anthropic({ apiKey: process.env.ANTHROPIC_API_KEY })
  : null;

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

// Reciprocal Rank Fusion — combina múltiples listas de resultados
function rrf(lists, k = 60) {
  const scores = {};
  for (const list of lists) {
    list.forEach((item, rank) => {
      const key = item.document_id || item.filename;
      if (!scores[key]) scores[key] = { item, s: 0 };
      scores[key].s += 1 / (k + rank + 1);
    });
  }
  return Object.values(scores).sort((a, b) => b.s - a.s).map(x => x.item);
}

async function expandQuery(query) {
  if (!anthropic) throw new Error('ANTHROPIC_API_KEY no configurada');
  const msg = await anthropic.messages.create({
    model: 'claude-haiku-4-5-20251001',
    max_tokens: 500,
    messages: [{ role: 'user', content:
      `Eres un experto en derecho colombiano. Dada la siguiente consulta jurídica, genera términos de búsqueda adicionales: sinónimos jurídicos, términos técnicos equivalentes, conceptos relacionados, variantes de redacción en sentencias colombianas.\n\nCONSULTA: "${query}"\n\nResponde SOLO JSON sin markdown: {"expanded_query":"consulta reformulada","keywords":["t1","t2",...]} (10-20 keywords)`
    }]
  });
  return JSON.parse(msg.content[0].text.replace(/```json|```/g, '').trim());
}

async function rerankWithClaude(query, candidates) {
  if (!anthropic) throw new Error('ANTHROPIC_API_KEY no configurada');
  const frags = candidates
    .map((c, i) => `[${i+1}] ${c.filename}\n${c.text}`)
    .join('\n\n---\n\n');
  const msg = await anthropic.messages.create({
    model: 'claude-sonnet-4-6',
    max_tokens: 2000,
    messages: [{ role: 'user', content:
      `Eres un asistente jurídico experto en derecho colombiano. Evalúa cada fragmento del 1-10 según relevancia para la consulta.\n\nCONSULTA: "${query}"\n\nFRAGMENTOS:\n${frags}\n\nResponde SOLO JSON sin markdown: [{"index":1,"score":9,"reason":"..."},...]`
    }]
  });
  return JSON.parse(msg.content[0].text.replace(/```json|```/g, '').trim());
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
  const { query, organo, limit = 10, advanced = false, rerank = false } = req.body || {};
  if (!query?.trim()) return res.status(400).json({ error: 'La consulta está vacía.' });

  try {
    await ensureCollection();

    const filter = organo && organo !== 'TODAS'
      ? { must: [{ key: 'organo', match: { value: organo } }] }
      : undefined;

    // Paso 1: expansión de query (opcional)
    let expandedData = null;
    if (advanced && anthropic) {
      try { expandedData = await expandQuery(query); } catch (_) {}
    }

    // Paso 2: búsqueda vectorial con query original
    const emb = await openai.embeddings.create({
      model: EMBEDDING_MODEL, input: [query], dimensions: EMBEDDING_DIM
    });
    const raw1 = await qdrant.search(COLLECTION, {
      vector: emb.data[0].embedding, limit: 50, filter, with_payload: true
    });
    const list1 = raw1.map(r => ({ score: r.score, ...r.payload }));

    let candidates;

    if (advanced && expandedData?.expanded_query) {
      // Paso 3: búsqueda con query expandida + RRF
      const emb2 = await openai.embeddings.create({
        model: EMBEDDING_MODEL, input: [expandedData.expanded_query], dimensions: EMBEDDING_DIM
      });
      const raw2 = await qdrant.search(COLLECTION, {
        vector: emb2.data[0].embedding, limit: 50, filter, with_payload: true
      });
      const list2 = raw2.map(r => ({ score: r.score, ...r.payload }));
      candidates = rrf([list1, list2]);
    } else {
      candidates = list1;
    }

    // Deduplicar por documento (mejor fragmento por sentencia)
    const seen = new Set();
    const unique = candidates.filter(r => {
      const key = r.document_id || r.filename;
      if (seen.has(key)) return false;
      seen.add(key); return true;
    });

    let results = unique.slice(0, Math.max(Number(limit), 20));

    // Paso 4: re-ranking con Claude (opcional)
    if (rerank && anthropic && results.length > 0) {
      try {
        const top = results.slice(0, 20);
        const ranks = await rerankWithClaude(query, top);
        results = top.map((r, i) => {
          const rank = ranks.find(x => x.index === i + 1);
          return { ...r, claudeScore: rank?.score ?? 0, claudeReason: rank?.reason ?? '' };
        }).sort((a, b) => b.claudeScore - a.claudeScore || b.score - a.score);
      } catch (_) {}
    }

    res.json({
      ok: true,
      expanded: expandedData?.keywords || null,
      results: results.slice(0, Number(limit))
    });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

app.listen(port, () => console.log(`Servidor en http://localhost:${port}`));
