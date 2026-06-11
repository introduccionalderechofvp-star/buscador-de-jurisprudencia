/**
 * marcar-salvamentos.js
 *
 * Marca con `tipo_seccion: "salvamento"` los chunks de Qdrant que pertenecen
 * a salvamentos / aclaraciones de voto. NO re-embebe nada (cero costo OpenAI).
 *
 * Convención del payload:
 *   - chunk de decisión / ratio decidendi → SIN campo `tipo_seccion`
 *   - chunk de salvamento o aclaración    → `tipo_seccion: "salvamento"`
 *   El MCP/API filtra con `must_not tipo_seccion=salvamento`. Chunks sin el
 *   campo pasan; chunks marcados se excluyen por defecto.
 *
 * Estrategia de detección:
 *   - Consejo de Estado: por filename (archivos pre-segmentados con palabra
 *     "Salvamentodev" o "Aclaraciondev" en el nombre).
 *   - Otros órganos: por posición — regex multi-línea del marcador,
 *     validado contra threshold posicional (50%).
 *
 * Resumible: state file con set de document_ids ya procesados.
 *
 * Uso:
 *   node scripts/marcar-salvamentos.js
 *   node scripts/marcar-salvamentos.js --organo "Sala Civil - Corte Suprema de Justicia"
 *   node scripts/marcar-salvamentos.js --dry-run
 *   node scripts/marcar-salvamentos.js --limit 100
 *   CONCURRENCY=4 node scripts/marcar-salvamentos.js
 */

import 'dotenv/config';
import fs from 'fs';
import path from 'path';
import crypto from 'crypto';
import pdf from 'pdf-parse';
import { convert as htmlToText } from 'html-to-text';
import { QdrantClient } from '@qdrant/js-client-rest';

// ─── Config ───────────────────────────────────────────────────────────────────

const UPLOADS_DIR = path.join(process.cwd(), 'uploads');
const COLLECTION  = process.env.QDRANT_COLLECTION || 'sentencias';
const QDRANT_URL  = process.env.QDRANT_URL        || 'http://localhost:6333';
const QDRANT_KEY  = process.env.QDRANT_API_KEY    || undefined;

// Mismos parámetros que ingest-bulk.js. NO TOCAR — si cambian, los chunk_index
// dejan de mapear al texto y se rompe la lógica posicional.
const CHUNK_SIZE    = 500;
const CHUNK_OVERLAP = 100;
const CHUNK_STEP    = CHUNK_SIZE - CHUNK_OVERLAP;

const THRESHOLD_PCT = Number(process.env.THRESHOLD_PCT || 50);
const CONCURRENCY   = Math.max(1, Number(process.env.CONCURRENCY || 1));

// CLI flags
const DRY_RUN       = process.argv.includes('--dry-run');
const FORCE         = process.argv.includes('--force');
const ORGANO_FILTER = process.argv.find(a => a.startsWith('--organo='))?.split('=')[1]
                   || process.argv[process.argv.indexOf('--organo') + 1] || null;
const LIMIT         = Number(process.argv.find(a => a.startsWith('--limit='))?.split('=')[1]
                   || process.argv[process.argv.indexOf('--limit') + 1] || 0);

const STATE_FILE = path.join(process.cwd(), '.marcar-salvamentos-state.json');
const INDEXABLE_EXTS = ['.pdf', '.md', '.txt', '.html'];

// Órganos a procesar. Doctrina y Legislación se excluyen (no tienen votos
// disidentes — códigos, libros, etc.).
const ORGANOS_RELEVANTES = [
  'Sala Civil - Corte Suprema de Justicia',
  'Sala Laboral - Corte Suprema de Justicia',
  'Sala Penal - Corte Suprema de Justicia',
  'Sala Tutelas - Corte Suprema de Justicia',
  'Consejo de Estado',
  'Sala Civil - Tribunal Superior de Medellín',
  'Sala Laboral - Tribunal Superior de Medellín',
  'Sala Penal - Tribunal Superior de Medellín',
];

// ─── State ────────────────────────────────────────────────────────────────────

function loadState() {
  try {
    const raw = JSON.parse(fs.readFileSync(STATE_FILE, 'utf8'));
    return {
      processed: new Set(raw.processed || []),
      stats: raw.stats || {},
    };
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

function hash(s) {
  return crypto.createHash('sha256').update(s).digest('hex');
}

function fmt(n) { return n.toLocaleString('es-CO'); }

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

// ─── Detector posicional (validado en test-detector-salvamentos.js) ──────────

const SALVAMENTO_RE = /^[ \t]*(SALVAMENTO\s+(?:PARCIAL\s+)?DE\s+VOTO|ACLARACI[ÓO]N\s+(?:PARCIAL\s+)?DE\s+VOTO)\b/gim;

/**
 * Devuelve el offset en palabras donde empieza la sección de salvamento,
 * o null si no detecta nada confiable. Matches en el primer THRESHOLD_PCT%
 * del documento se descartan (suelen ser menciones en prosa, no cabeceras).
 */
function detectarSalvamentoPosicional(text) {
  SALVAMENTO_RE.lastIndex = 0;
  const matches = [];
  let m;
  while ((m = SALVAMENTO_RE.exec(text)) !== null) {
    matches.push(m.index);
  }
  if (matches.length === 0) return null;
  const threshold = text.length * (THRESHOLD_PCT / 100);
  const valido = matches.find(idx => idx >= threshold);
  if (valido === undefined) return null;
  return text.slice(0, valido).split(/\s+/).filter(Boolean).length;
}

// ─── Detector por filename ────────────────────────────────────────────────────

// Algunos órganos pre-segmentan providencias o nombran archivos por tipo de
// pieza, de modo que el filename basta para saber que TODO el documento es un
// salvamento o aclaración (no solo una sección al final).
//
// Las regex son ESPECÍFICAS al patrón típico de cada órgano para evitar falsos
// positivos:
//   - "salvamento marítimo", "salvamento de bienes" → NO matchea (regex exige
//     que después de SALVAMENTO/ACLARACIÓN venga DE VOTO).
//   - Tribunal Medellín usa filenames humanos con espacios.
//   - CE pre-segmenta con palabras-clave concatenadas.
const FILENAME_PATTERNS = {
  'Consejo de Estado':
    /(Salvamentodev|Aclaraciondev|SALVAMENTODEV|ACLARACIONDEV|Aclaracion[_-]de[_-]voto|Salvamento[_-]de[_-]voto)/i,
  'Sala Civil - Tribunal Superior de Medellín':
    /(SALVAMENTO|ACLARACI[ÓO]N)\s+(?:PARCIAL\s+)?DE\s+VOTO/i,
  'Sala Laboral - Tribunal Superior de Medellín':
    /(SALVAMENTO|ACLARACI[ÓO]N)\s+(?:PARCIAL\s+)?DE\s+VOTO/i,
  'Sala Penal - Tribunal Superior de Medellín':
    /(SALVAMENTO|ACLARACI[ÓO]N)\s+(?:PARCIAL\s+)?DE\s+VOTO/i,
};

function detectarPorFilename(organo, filename) {
  const re = FILENAME_PATTERNS[organo];
  if (!re) return null;
  if (!re.test(filename)) return null;
  const motivo = organo === 'Consejo de Estado' ? 'filename_CE' : 'filename_tribunal';
  return { allSalvamento: true, motivo };
}

// ─── Lógica de marcado ────────────────────────────────────────────────────────

/**
 * Decide, dado un archivo + organo, qué chunks (por chunk_index) deben
 * marcarse como salvamento. Devuelve:
 *   - { allSalvamento: true }  → todos los chunks son salvamento
 *   - { fromWord: N }          → chunks cuyo end-word > N son salvamento
 *   - null                     → no marcar nada (es decisión completa)
 */
async function determinarEstrategia(filePath, organo) {
  const filename = path.basename(filePath);

  // 1. Intentar detección por filename (rápida, no necesita leer el archivo).
  //    Si matchea, el doc entero es salvamento. Si no, caemos al posicional.
  const porFilename = detectarPorFilename(organo, filename);
  if (porFilename) return porFilename;

  let text;
  try {
    text = await extractText(filePath);
  } catch (e) {
    throw new Error(`lectura: ${e.message.slice(0, 60)}`);
  }
  if (text.length < 50) {
    return null;  // sin contenido útil
  }

  const wordOffset = detectarSalvamentoPosicional(text);
  if (wordOffset === null) return null;

  return { fromWord: wordOffset, motivo: 'posicional', filename, text };
}

/**
 * Procesa un archivo: determina estrategia, scrollea Qdrant por document_id,
 * y aplica setPayload sobre los chunks que correspondan.
 */
async function procesarArchivo(qdrant, filePath, organo, stats) {
  const filename = path.basename(filePath);

  let estrategia;
  try {
    estrategia = await determinarEstrategia(filePath, organo);
  } catch (e) {
    stats.errores++;
    return { documentId: null, status: 'error', mensaje: e.message };
  }

  // Necesitamos el texto para calcular document_id. Si la estrategia es
  // "allSalvamento" por filename, todavía no leímos el texto — hay que leerlo.
  let text = estrategia?.text;
  if (!text) {
    try { text = await extractText(filePath); }
    catch (e) {
      stats.errores++;
      return { documentId: null, status: 'error', mensaje: `lectura: ${e.message}` };
    }
  }
  if (text.length < 50) {
    return { documentId: null, status: 'sin_texto' };
  }

  const documentId = hash(`${filename}:${text.slice(0, 10000)}`);

  // Scroll todos los chunks del documento
  const points = [];
  let nextOffset = undefined;
  try {
    do {
      const r = await qdrant.scroll(COLLECTION, {
        filter: { must: [{ key: 'document_id', match: { value: documentId } }] },
        limit: 256,
        offset: nextOffset,
        with_payload: { include: ['chunk_index'] },
        with_vector: false,
      });
      points.push(...r.points);
      nextOffset = r.next_page_offset;
    } while (nextOffset);
  } catch (e) {
    stats.errores++;
    return { documentId, status: 'error', mensaje: `qdrant scroll: ${e.message}` };
  }

  if (points.length === 0) {
    return { documentId, status: 'no_chunks' };
  }

  // Determinar qué IDs marcar
  let idsSalvamento;
  if (estrategia === null) {
    idsSalvamento = [];
  } else if (estrategia.allSalvamento) {
    idsSalvamento = points.map(p => p.id);
  } else {
    const w = estrategia.fromWord;
    idsSalvamento = points
      .filter(p => {
        const idx = p.payload?.chunk_index;
        if (idx === undefined || idx === null) return false;
        return idx * CHUNK_STEP + CHUNK_SIZE > w;  // chunk toca zona de salvamento
      })
      .map(p => p.id);
  }

  if (idsSalvamento.length === 0) {
    return { documentId, status: 'sin_salvamento', chunks: points.length };
  }

  if (!DRY_RUN) {
    // Batch para no mandar bodies enormes en docs con muchos chunks
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
      return { documentId, status: 'error', mensaje: `setPayload: ${e.message}` };
    }
  }

  return {
    documentId,
    status: 'marcado',
    chunks: points.length,
    marcados: idsSalvamento.length,
    motivo: estrategia.motivo,
  };
}

// ─── Main ─────────────────────────────────────────────────────────────────────

async function ensureIndex(qdrant) {
  try {
    await qdrant.createPayloadIndex(COLLECTION, {
      field_name: 'tipo_seccion',
      field_schema: 'keyword',
    });
    console.log('  Índice payload "tipo_seccion" creado.');
  } catch (e) {
    if (!/already exists|exists/i.test(e.message || '')) throw e;
  }
}

async function main() {
  const qdrant = new QdrantClient({ url: QDRANT_URL, apiKey: QDRANT_KEY });

  console.log(`\n=== Marcado de salvamentos ===`);
  console.log(`Modo         : ${DRY_RUN ? 'DRY-RUN (no escribe Qdrant)' : 'REAL (escribe Qdrant)'}${FORCE ? ' + FORCE (ignora state)' : ''}`);
  console.log(`Concurrencia : ${CONCURRENCY}`);
  console.log(`Threshold    : ${THRESHOLD_PCT}% (detector posicional)`);
  if (ORGANO_FILTER) console.log(`Órgano       : ${ORGANO_FILTER}`);
  if (LIMIT > 0)     console.log(`Límite       : ${LIMIT} archivos por órgano`);

  if (!DRY_RUN) await ensureIndex(qdrant);

  const state = loadState();
  console.log(`State        : ${fmt(state.processed.size)} document_ids ya procesados\n`);

  const organos = ORGANO_FILTER
    ? [ORGANO_FILTER]
    : ORGANOS_RELEVANTES.filter(o => fs.existsSync(path.join(UPLOADS_DIR, o)));

  const startTime = Date.now();
  let saveCounter = 0;

  for (const organo of organos) {
    const dir = path.join(UPLOADS_DIR, organo);
    let files = findFiles(dir);
    if (files.length === 0) {
      console.log(`─── ${organo}: sin archivos ───`);
      continue;
    }
    if (LIMIT > 0) files = files.slice(0, LIMIT);

    const stats = {
      total: files.length,
      marcados: 0, sinSalvamento: 0, yaProcesados: 0,
      sinChunks: 0, sinTexto: 0, errores: 0,
      chunksMarcados: 0,
    };

    console.log(`\n─── ${organo} (${fmt(files.length)} archivos) ───`);

    let cursor = 0;
    const worker = async () => {
      while (true) {
        const idx = cursor++;
        if (idx >= files.length) return;
        const filePath = files[idx];
        const filename = path.basename(filePath);
        const short    = filename.length > 50 ? filename.slice(0, 47) + '...' : filename;
        const prefix   = `  [${idx + 1}/${files.length}] ${short.padEnd(52)}`;

        const r = await procesarArchivo(qdrant, filePath, organo, stats);

        if (r.documentId && state.processed.has(r.documentId) && !FORCE) {
          stats.yaProcesados++;
          // Log silenciado para no spamear
          continue;
        }

        switch (r.status) {
          case 'marcado':
            stats.marcados++;
            stats.chunksMarcados += r.marcados;
            console.log(`${prefix} ${r.marcados}/${r.chunks} marcados (${r.motivo})`);
            break;
          case 'sin_salvamento':
            stats.sinSalvamento++;
            // Silenciado: es el caso más común
            break;
          case 'no_chunks':
            stats.sinChunks++;
            // Silenciado
            break;
          case 'sin_texto':
            stats.sinTexto++;
            break;
          case 'error':
            console.log(`${prefix} ERROR: ${r.mensaje?.slice(0, 80)}`);
            break;
        }

        // Solo trackeamos como "procesado" cuando efectivamente escribimos.
        // En dry-run el state queda intacto para que el real run procese todo.
        if (r.documentId && !DRY_RUN) {
          state.processed.add(r.documentId);
          saveCounter++;
          if (saveCounter >= 100) {
            saveState(state);
            saveCounter = 0;
          }
        }
      }
    };

    await Promise.all(Array.from({ length: CONCURRENCY }, () => worker()));

    state.stats[organo] = stats;
    if (!DRY_RUN) saveState(state);

    console.log(`  ─ Resumen ${organo}:`);
    console.log(`     Marcados (con salvamento): ${fmt(stats.marcados)}  (${fmt(stats.chunksMarcados)} chunks)`);
    console.log(`     Sin salvamento detectado : ${fmt(stats.sinSalvamento)}`);
    console.log(`     Ya procesados (skip state): ${fmt(stats.yaProcesados)}`);
    console.log(`     Sin chunks en Qdrant     : ${fmt(stats.sinChunks)}`);
    console.log(`     Sin texto                : ${fmt(stats.sinTexto)}`);
    console.log(`     Errores                  : ${fmt(stats.errores)}`);
  }

  if (!DRY_RUN) saveState(state);
  const elapsed = ((Date.now() - startTime) / 1000 / 60).toFixed(1);
  console.log(`\n${'─'.repeat(60)}`);
  console.log(`Tiempo total: ${elapsed} min`);
  console.log(`State final : ${fmt(state.processed.size)} document_ids procesados`);
  if (DRY_RUN) {
    console.log(`\n⚠️  Esto fue DRY-RUN. Nada se escribió a Qdrant. Quitá --dry-run para aplicar.`);
  }
}

main().catch(e => { console.error('\nFATAL:', e); process.exit(1); });
