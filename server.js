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

// Reciprocal Rank Fusion — combina múltiples listas con pesos opcionales
function rrf(lists, k = 60, weights = null) {
  const w = weights || lists.map(() => 1);
  const scores = {};
  lists.forEach((list, i) => {
    list.forEach((item, rank) => {
      const key = item.document_id || item.filename;
      if (!scores[key]) scores[key] = { item, s: 0 };
      scores[key].s += w[i] * (1 / (k + rank + 1));
    });
  });
  return Object.values(scores).sort((a, b) => b.s - a.s).map(x => x.item);
}

// Extrae palabras clave significativas de la consulta para búsqueda léxica
function extractKeywords(query) {
  const stopwords = new Set([
    'puede', 'como', 'para', 'una', 'que', 'los', 'las', 'del', 'con', 'por',
    'este', 'esta', 'esto', 'cuando', 'donde', 'cual', 'cuales', 'tiene',
    'tener', 'hacer', 'haber', 'sido', 'estas', 'estos', 'debe', 'deben',
    'sobre', 'entre', 'según', 'dicha', 'dicho', 'qué', 'cómo', 'cuál'
  ]);
  return query.toLowerCase()
    .replace(/[¿?¡!.,;:()"]/g, ' ')
    .split(/\s+/)
    .filter(w => w.length >= 5 && !stopwords.has(w))
    .slice(0, 5);
}

// Wrapper con timeout: si la promesa no resuelve en `ms`, rechaza con error
// claro. Útil para degradar grácilmente cuando una API externa (Anthropic,
// OpenAI) tarda demasiado y bloquearía la respuesta al cliente.
function withTimeout(promise, ms, label = 'operation') {
  let timer;
  const timeout = new Promise((_, reject) => {
    timer = setTimeout(() => reject(new Error(`${label} timeout (${ms}ms)`)), ms);
  });
  return Promise.race([promise, timeout]).finally(() => clearTimeout(timer));
}

// Crea el índice de texto en Qdrant si no existe (idempotente)
async function ensureTextIndex() {
  try {
    const url = `${process.env.QDRANT_URL || 'http://localhost:6333'}/collections/${COLLECTION}/index`;
    const headers = { 'Content-Type': 'application/json' };
    if (process.env.QDRANT_API_KEY) headers['api-key'] = process.env.QDRANT_API_KEY;
    await fetch(url, {
      method: 'PUT', headers,
      body: JSON.stringify({ field_name: 'text', field_schema: 'text' })
    });
  } catch (_) {}
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
    max_tokens: 8192,
    messages: [{ role: 'user', content:
      `Eres un asistente jurídico experto en derecho colombiano. Evalúa cada fragmento del 1-10 según relevancia para la consulta.\n\nCONSULTA: "${query}"\n\nFRAGMENTOS:\n${frags}\n\nResponde SOLO JSON compacto sin markdown, sin texto adicional. Usa "i" (índice), "s" (score 1-10) y "r" (razón, MÁXIMO 15 palabras en español):\n[{"i":1,"s":9,"r":"..."},{"i":2,"s":7,"r":"..."},...]`
    }]
  });
  const raw = msg.content?.[0]?.text?.replace(/```json|```/g, '').trim() || '';
  let parsed;
  try {
    parsed = JSON.parse(raw);
  } catch (e) {
    throw new Error(`Claude JSON malformado (len=${raw.length}, stop=${msg.stop_reason}): ${raw.slice(0, 200)}`);
  }
  // Normalizar al formato que espera el caller: {index, score, reason}
  return parsed.map(x => ({
    index:  x.i ?? x.index,
    score:  x.s ?? x.score,
    reason: x.r ?? x.reason ?? ''
  }));
}

async function ensureCollection() {
  const collections = await qdrant.getCollections();
  const exists = collections.collections.some((c) => c.name === COLLECTION);
  if (!exists) {
    await qdrant.createCollection(COLLECTION, {
      vectors: { size: EMBEDDING_DIM, distance: 'Cosine' }
    });
    await ensureTextIndex();
  }
}

// Resuelve un path relativo de uploads/ a un path absoluto en disco. Si el
// archivo no existe y termina en .pdf, prueba con .md/.txt del mismo basename
// (algunas salas se convirtieron a markdown para liberar disco — la búsqueda
// en Qdrant sigue apuntando al .pdf original, así que el server hace fallback).
function resolveFilePath(rel) {
  const abs = path.resolve(UPLOADS_DIR, rel);
  if (fs.existsSync(abs)) return abs;
  if (/\.pdf$/i.test(abs)) {
    for (const ext of ['.md', '.txt']) {
      const alt = abs.replace(/\.pdf$/i, ext);
      if (fs.existsSync(alt)) return alt;
    }
  }
  return abs;
}

app.get('/api/download', (req, res) => {
  const rel = decodeURIComponent(req.query.path || '').replace(/\.\./g, '').replace(/^[/\\]/, '');
  if (!rel) return res.status(400).json({ error: 'Falta el parámetro path.' });
  const filePath = resolveFilePath(rel);
  if (!filePath.startsWith(UPLOADS_DIR + path.sep) && filePath !== UPLOADS_DIR) {
    return res.status(403).json({ error: 'Acceso denegado.' });
  }
  res.download(filePath, path.basename(filePath), err => {
    if (err && !res.headersSent) res.status(404).json({ error: 'Archivo no encontrado.' });
  });
});

// Devuelve el texto completo del documento — para que un LLM pueda leer la
// sentencia/libro entero, no solo el chunk truncado de la búsqueda.
// Soporta PDF (extrae con pdf-parse) y .md/.txt (lee directo, más rápido y
// sin necesidad de OCR — útil para corpus pre-procesados como Consejo de Estado).
app.get('/api/document/text', async (req, res) => {
  const rel = decodeURIComponent(req.query.path || '').replace(/\.\./g, '').replace(/^[/\\]/, '');
  if (!rel) return res.status(400).json({ error: 'Falta el parámetro path.' });
  const filePath = resolveFilePath(rel);
  if (!filePath.startsWith(UPLOADS_DIR + path.sep) && filePath !== UPLOADS_DIR) {
    return res.status(403).json({ error: 'Acceso denegado.' });
  }
  try {
    const ext = path.extname(filePath).toLowerCase();
    let text, numPages = 0;

    if (ext === '.pdf') {
      const buffer = await fs.promises.readFile(filePath);
      const parsed = await pdf(buffer);
      text = (parsed.text || '').trim();
      numPages = parsed.numpages || 0;
    } else if (ext === '.md' || ext === '.txt') {
      text = (await fs.promises.readFile(filePath, 'utf8')).trim();
    } else {
      return res.status(415).json({ error: `Formato no soportado: ${ext}` });
    }

    res.json({
      filename:  path.basename(filePath),
      file_path: rel,
      organo:    rel.split('/')[0] || null,
      num_pages: numPages,
      num_chars: text.length,
      full_text: text
    });
  } catch (e) {
    res.status(404).json({ error: `No se pudo leer el archivo: ${e.message}` });
  }
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
  const { query, organo, limit = 15, advanced = false, rerank = false } = req.body || {};
  if (!query?.trim()) return res.status(400).json({ error: 'La consulta está vacía.' });

  try {
    await ensureCollection();

    const filter = organo && organo !== 'TODAS'
      ? { must: [{ key: 'organo', match: { value: organo } }] }
      : undefined;

    // Candidatos por búsqueda: 100 fragmentos → ~60-80 documentos únicos
    const FETCH_LIMIT = 100;

    // Pasos 1+2 en paralelo: expand (Anthropic Haiku) y embedding (OpenAI) son
    // independientes entre sí — no esperar uno antes del otro ahorra ~3 seg en
    // modo avanzado (en cold start, donde TLS handshakes dominan la latencia).
    // expand tiene timeout de 10s con degradación grácil: si Anthropic responde
    // lento o falla, el modo avanzado simplemente cae a búsqueda básica en vez
    // de tirar Internal Server Error al cliente.
    const [expandedData, emb] = await Promise.all([
      (advanced && anthropic)
        ? withTimeout(expandQuery(query), 10000, 'expand').catch(e => {
            console.error('[expand error]', e.message);
            return null;
          })
        : Promise.resolve(null),
      openai.embeddings.create({
        model: EMBEDDING_MODEL, input: [query], dimensions: EMBEDDING_DIM
      })
    ]);

    // Búsqueda vectorial con query original.
    // search_params.quantization fuerza usar solo int8 (rápido, todo en RAM con
    // always_ram=true) sin rescorear contra los vectores originales en disco.
    // El disco del VPS es lento — rescorear agregaba 10-15s a cada query cold.
    // oversampling=2.0 compensa la pérdida de precisión: pide el doble de
    // candidatos al HNSW para que los buenos casi seguro estén en el set.
    const SEARCH_PARAMS = {
      quantization: { rescore: false, oversampling: 2.0 }
    };

    const raw1 = await withTimeout(
      qdrant.search(COLLECTION, {
        vector: emb.data[0].embedding, limit: FETCH_LIMIT, filter, with_payload: true,
        params: SEARCH_PARAMS
      }),
      20000, 'qdrant-vector'
    ).catch(e => {
      // Si el vector search falla, seguimos con array vacío. El keyword search
      // puede salvar la query. Si los dos fallan, devolvemos resultado vacío
      // (mejor que tirar 500 al cliente).
      console.error('[vector search error]', e.message);
      return [];
    });
    const list1 = raw1.map(r => ({ score: r.score, ...r.payload }));

    // Paso 3: búsqueda léxica con keywords enriquecidas.
    // En modo avanzado los keywords de Claude se añaden al filtro OR (más sinónimos,
    // sin segunda búsqueda vectorial que introduciría ruido competitivo).
    let keywordList = null;
    const keywords = extractKeywords(query);
    const enrichedKw = [...keywords];
    if (advanced && expandedData?.keywords?.length) {
      const extra = expandedData.keywords
        .map(k => k.toLowerCase().replace(/[¿?¡!.,;:()"]/g, ' ').trim())
        .filter(k => k.length >= 4 && !enrichedKw.includes(k));
      enrichedKw.push(...extra.slice(0, 10));
    }
    if (enrichedKw.length > 0) {
      try {
        const kwFilter = {
          should: enrichedKw.map(kw => ({ key: 'text', match: { text: kw } })),
          ...(filter?.must?.length ? { must: filter.must } : {})
        };
        const rawKw = await withTimeout(
          qdrant.search(COLLECTION, {
            vector: emb.data[0].embedding,
            limit: FETCH_LIMIT,
            filter: kwFilter,
            with_payload: true,
            params: SEARCH_PARAMS
          }),
          15000, 'qdrant-keyword'
        );
        if (rawKw.length > 0) keywordList = rawKw.map(r => ({ score: r.score, ...r.payload }));
      } catch (e) { console.error('[keyword search error]', e.message); }
    }

    // RRF: keyword list va primero y con más peso para que el fragmento
    // con coincidencia léxica sea el representante del documento.
    const allLists = keywordList ? [keywordList, list1] : [list1];
    const weights  = keywordList ? [2.0, 1.0] : null;
    const candidates = allLists.length > 1 ? rrf(allLists, 60, weights) : list1;

    // Índice del fragmento con mayor similitud vectorial por documento (de list1).
    // Ese fragmento tiene más probabilidad de contener el análisis jurídico relevante
    // que el de mayor peso léxico (que suele ser el de hechos del caso).
    const bestVectorFrag = {};
    for (const item of list1) {
      const key = item.document_id || item.filename;
      if (!bestVectorFrag[key] || item.score > bestVectorFrag[key].score) {
        bestVectorFrag[key] = item;
      }
    }

    // Deduplicar por documento. El orden lo determina el RRF (qué documentos incluir),
    // pero el fragmento mostrado es el de mayor similitud semántica (mejor para re-ranking).
    const seen = new Set();
    const unique = candidates
      .filter(r => {
        const key = r.document_id || r.filename;
        if (seen.has(key)) return false;
        seen.add(key); return true;
      })
      .map(r => bestVectorFrag[r.document_id || r.filename] || r);

    // Pool más amplio en modo avanzado para no descartar candidatos por el límite.
    // rerankSize fijo en 25 (antes 35 con advanced): un prompt con 35 fragmentos
    // saturaba el contexto de Sonnet y agregaba 5-10s de latencia sin mejorar
    // calidad notablemente. 25 da buena precisión y responde más rápido.
    const poolSize   = advanced ? 80 : 30;
    const rerankSize = 25;
    let results = unique.slice(0, Math.max(Number(limit), poolSize));

    // Re-ranking con Claude (opcional). Timeout de 25s con degradación grácil:
    // si Sonnet tarda demasiado, devolvemos los resultados sin rerank en lugar
    // de hacer esperar al cliente o tirar 500.
    if (rerank && anthropic && results.length > 0) {
      try {
        const top = results.slice(0, rerankSize);
        const ranks = await withTimeout(rerankWithClaude(query, top), 25000, 'rerank');
        results = top.map((r, i) => {
          const rank = ranks.find(x => x.index === i + 1);
          return { ...r, claudeScore: rank?.score ?? 0, claudeReason: rank?.reason ?? '' };
        }).sort((a, b) => b.claudeScore - a.claudeScore || b.score - a.score);
      } catch (e) {
        console.error('[rerank error]', e.message);
      }
    }

    // Términos que el frontend debe resaltar en el texto de cada resultado.
    // Incluye: keywords significativas del query, palabras del query de ≥4 chars,
    // y (si hay expansión) las keywords jurídicas de Claude.
    const queryWords = query.toLowerCase()
      .replace(/[¿?¡!.,;:()"]/g, ' ')
      .split(/\s+/)
      .filter(w => w.length >= 4);
    const highlights = [...new Set([...enrichedKw, ...queryWords])]
      .filter(Boolean)
      .sort((a, b) => b.length - a.length);

    res.json({
      ok: true,
      highlights,
      expanded: expandedData?.keywords || null,
      results: results.slice(0, Number(limit))
    });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

app.listen(port, () => console.log(`Servidor en http://localhost:${port}`));
