/**
 * migrate-pinecone.js
 *
 * Migra todos los vectores de un índice Pinecone (serverless) a Qdrant.
 *
 * Requisitos en .env:
 *   PINECONE_API_KEY   — API key de Pinecone (pcsk_...)
 *   PINECONE_HOST      — Host del índice (sentencias-xxxxx.svc.xxxx.pinecone.io)
 *   QDRANT_URL         — URL de Qdrant (default: http://localhost:6333)
 *   QDRANT_API_KEY     — API key de Qdrant (opcional)
 *   QDRANT_COLLECTION  — Nombre de la colección (default: sentencias)
 *   EMBEDDING_DIM      — Dimensiones (default: 3072)
 *
 * Uso:
 *   node scripts/migrate-pinecone.js
 *
 * Nota: el índice Pinecone DEBE ser serverless para usar /vectors/list.
 * Si es pod-based, usa primero el script de Pinecone para exportar los IDs.
 */

import 'dotenv/config';
import { QdrantClient } from '@qdrant/js-client-rest';
import crypto from 'crypto';

// ─── Config ───────────────────────────────────────────────────────────────────

const PINECONE_KEY  = process.env.PINECONE_API_KEY;
const PINECONE_HOST = process.env.PINECONE_HOST;
const QDRANT_URL    = process.env.QDRANT_URL    || 'http://localhost:6333';
const QDRANT_KEY    = process.env.QDRANT_API_KEY || undefined;
const COLLECTION    = process.env.QDRANT_COLLECTION || 'sentencias';
const DIM           = Number(process.env.EMBEDDING_DIM || 3072);

const FETCH_BATCH  = 100;  // IDs por llamada a /vectors/fetch
const UPSERT_BATCH = 100;  // Puntos por llamada a Qdrant upsert
const DELAY_MS     = 150;  // Pausa entre batches (evitar rate-limit)

// ─── Helpers ──────────────────────────────────────────────────────────────────

function pcBase() {
  let h = (PINECONE_HOST || '').trim();
  if (!h.startsWith('http')) h = 'https://' + h;
  return h.replace(/\/$/, '');
}

function toUUID(str) {
  // UUID v4 determinista derivado del ID original de Pinecone
  const h = crypto.createHash('sha256').update(str).digest('hex');
  return [h.slice(0,8), h.slice(8,12), '4' + h.slice(13,16),
    (parseInt(h.slice(16,18), 16) & 0x3f | 0x80).toString(16) + h.slice(18,20),
    h.slice(20,32)].join('-');
}

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

// ─── Pinecone API ─────────────────────────────────────────────────────────────

async function pcGet(path) {
  const r = await fetch(pcBase() + path, {
    headers: { 'Api-Key': PINECONE_KEY }
  });
  if (!r.ok) {
    const e = await r.json().catch(() => ({}));
    throw new Error(`Pinecone GET ${path} → HTTP ${r.status}: ${e.message || JSON.stringify(e)}`);
  }
  return r.json();
}

async function describeIndex() {
  const r = await fetch(pcBase() + '/describe_index_stats', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'Api-Key': PINECONE_KEY },
    body: '{}'
  });
  if (!r.ok) throw new Error(`describe_index_stats → HTTP ${r.status}`);
  return r.json();
}

/** Lista todos los IDs del namespace default vía paginación (serverless). */
async function listAllIds() {
  const ids = [];
  let token;
  do {
    const qs = new URLSearchParams({ limit: '100' });
    if (token) qs.set('paginationToken', token);
    const data = await pcGet(`/vectors/list?${qs}`);
    for (const v of (data.vectors || [])) ids.push(v.id);
    token = data.pagination?.next;
    process.stdout.write(`\r  Listando IDs: ${ids.length}...`);
  } while (token);
  process.stdout.write('\n');
  return ids;
}

/** Trae vectores + metadata para un lote de IDs. */
async function fetchVectors(ids) {
  const qs = ids.map(id => `ids=${encodeURIComponent(id)}`).join('&');
  const data = await pcGet(`/vectors/fetch?${qs}`);
  return Object.values(data.vectors || {});
}

// ─── Qdrant ───────────────────────────────────────────────────────────────────

async function ensureCollection(qdrant) {
  const { collections } = await qdrant.getCollections();
  if (!collections.some(c => c.name === COLLECTION)) {
    await qdrant.createCollection(COLLECTION, {
      vectors: { size: DIM, distance: 'Cosine' }
    });
    console.log(`  Colección "${COLLECTION}" creada en Qdrant.`);
  } else {
    console.log(`  Colección "${COLLECTION}" ya existe — se usará upsert (sin duplicados).`);
  }
}

// ─── Main ─────────────────────────────────────────────────────────────────────

async function main() {
  // Validar config
  if (!PINECONE_KEY) { console.error('ERROR: Falta PINECONE_API_KEY en .env'); process.exit(1); }
  if (!PINECONE_HOST) { console.error('ERROR: Falta PINECONE_HOST en .env'); process.exit(1); }

  console.log('=== Migración Pinecone → Qdrant ===\n');
  console.log(`  Pinecone: ${PINECONE_HOST}`);
  console.log(`  Qdrant:   ${QDRANT_URL} › ${COLLECTION}\n`);

  // Describe index
  console.log('Verificando índice Pinecone...');
  try {
    const stats = await describeIndex();
    const total = stats.totalVectorCount ?? stats.totalRecordCount ?? '?';
    const dim   = stats.dimension ?? '?';
    console.log(`  Vectores totales : ${total}`);
    console.log(`  Dimensiones      : ${dim}`);
    if (dim && Number(dim) !== DIM) {
      console.warn(`\n⚠️  ADVERTENCIA: Pinecone tiene ${dim} dims, Qdrant espera ${DIM} dims.`);
      console.warn('   Verifica EMBEDDING_DIM en .env antes de continuar.\n');
    }
  } catch (e) {
    console.warn(`  No se pudo obtener stats: ${e.message}`);
  }

  // Conectar Qdrant
  const qdrant = new QdrantClient({ url: QDRANT_URL, apiKey: QDRANT_KEY });
  await ensureCollection(qdrant);

  // Listar todos los IDs
  console.log('\nPaso 1: Listando IDs de Pinecone...');
  let allIds;
  try {
    allIds = await listAllIds();
  } catch (e) {
    console.error(`\nERROR al listar IDs: ${e.message}`);
    console.error('Nota: /vectors/list solo funciona en índices serverless de Pinecone.');
    console.error('Si tu índice es pod-based, contacta soporte o usa el SDK de Python para exportar.');
    process.exit(1);
  }

  if (!allIds.length) {
    console.log('No se encontraron vectores. El índice está vacío.');
    process.exit(0);
  }
  console.log(`  Total IDs encontrados: ${allIds.length}\n`);

  // Migrar en batches
  console.log('Paso 2: Descargando vectores e importando a Qdrant...');
  let migrated = 0, skipped = 0, errors = 0;

  for (let i = 0; i < allIds.length; i += FETCH_BATCH) {
    const batchIds = allIds.slice(i, i + FETCH_BATCH);

    let vectors;
    try {
      vectors = await fetchVectors(batchIds);
    } catch (e) {
      console.error(`\n  ERROR fetch batch ${i}-${i + batchIds.length}: ${e.message}`);
      errors += batchIds.length;
      continue;
    }

    // Filtrar vectores sin valores (edge case)
    const valid = vectors.filter(v => v.values?.length === DIM);
    const invalid = vectors.length - valid.length;
    if (invalid) skipped += invalid;

    // Construir puntos para Qdrant
    const points = valid.map(v => ({
      id: toUUID(v.id),
      vector: v.values,
      payload: {
        pinecone_id : v.id,
        filename    : v.metadata?.filename  || 'desconocido',
        organo      : 'No especificado',      // original no tenía campo organo
        page_start  : v.metadata?.pageStart  ?? 0,
        page_end    : v.metadata?.pageEnd    ?? 0,
        chunk_index : 0,
        text        : v.metadata?.text       || ''
      }
    }));

    // Upsert a Qdrant en sub-batches si es necesario
    for (let j = 0; j < points.length; j += UPSERT_BATCH) {
      const sub = points.slice(j, j + UPSERT_BATCH);
      try {
        await qdrant.upsert(COLLECTION, { wait: true, points: sub });
        migrated += sub.length;
      } catch (e) {
        console.error(`\n  ERROR upsert Qdrant batch ${i + j}: ${e.message}`);
        errors += sub.length;
      }
    }

    const pct = Math.round(((i + batchIds.length) / allIds.length) * 100);
    process.stdout.write(`\r  Progreso: ${pct}% · migrados: ${migrated} · errores: ${errors}`);
    await sleep(DELAY_MS);
  }

  // Resumen final
  console.log('\n\n=== Resultado ===');
  console.log(`  Migrados  : ${migrated}`);
  console.log(`  Saltados  : ${skipped}  (dimensión incorrecta)`);
  console.log(`  Errores   : ${errors}`);
  console.log(`\n  Colección "${COLLECTION}" lista en ${QDRANT_URL}`);
  console.log('  Nota: campo "organo" queda como "No especificado" — actualízalo vía /api/ingest si resubes los PDFs.');
}

main().catch(e => { console.error('\nERROR FATAL:', e.message); process.exit(1); });
