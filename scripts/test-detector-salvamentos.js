/**
 * test-detector-salvamentos.js
 *
 * Test del detector de salvamentos / aclaraciones de voto.
 * NO toca Qdrant. NO modifica archivos. Solo lee, detecta, reporta.
 *
 * Para cada archivo muestreado imprime:
 *   - si detectó salvamento: posición + contexto alrededor del corte
 *   - si no: lo dice
 *   - si hubo matches descartados por estar muy al principio: los lista
 *     (sirve para detectar falsos positivos del regex)
 *
 * Uso:
 *   node scripts/test-detector-salvamentos.js
 *   ORGANO_FILTER="Sala Civil - Corte Suprema de Justicia" node scripts/test-detector-salvamentos.js
 *   SAMPLE_PER_ORGANO=20 node scripts/test-detector-salvamentos.js
 *   THRESHOLD_PCT=40 node scripts/test-detector-salvamentos.js
 */

import 'dotenv/config';
import fs from 'fs';
import path from 'path';
import pdf from 'pdf-parse';
import { convert as htmlToText } from 'html-to-text';

const UPLOADS_DIR       = path.join(process.cwd(), 'uploads');
const SAMPLE_PER_ORGANO = Number(process.env.SAMPLE_PER_ORGANO || 10);
const ORGANO_FILTER     = process.env.ORGANO_FILTER || null;
const THRESHOLD_PCT     = Number(process.env.THRESHOLD_PCT || 50);

// Órganos donde tiene sentido buscar salvamentos.
// Doctrina y Legislación no tienen votos disidentes.
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

const INDEXABLE_EXTS = ['.pdf', '.md', '.txt', '.html'];

// ─── Detector ─────────────────────────────────────────────────────────────────

// Marcador típico al inicio de línea. Tolera:
//   - mayúsculas y minúsculas (flag i, aunque CSJ casi siempre usa mayúsculas)
//   - "ACLARACIÓN" con o sin tilde
//   - "PARCIAL" opcional entre el sustantivo y "DE VOTO"
//   - texto adicional después en la misma línea (nombre del magistrado)
const SALVAMENTO_RE = /^[ \t]*(SALVAMENTO\s+(?:PARCIAL\s+)?DE\s+VOTO|ACLARACI[ÓO]N\s+(?:PARCIAL\s+)?DE\s+VOTO)\b/gim;

/**
 * Encuentra todos los matches del marcador, filtra los que estén en el último
 * THRESHOLD_PCT % del documento, y devuelve el primero de esos como inicio de
 * la sección de salvamentos. Los descartados (antes del threshold) se reportan
 * para que el humano pueda revisar si fueron falsos positivos del regex o si
 * el threshold está mal calibrado.
 *
 * Returns:
 *   - null si no hay ningún match en todo el documento
 *   - { wordOffset, charOffset, marker, descartados } si encontró uno válido
 *   - { wordOffset: null, descartados } si solo hay matches descartados
 */
function findSalvamento(text) {
  SALVAMENTO_RE.lastIndex = 0;
  const matches = [];
  let m;
  while ((m = SALVAMENTO_RE.exec(text)) !== null) {
    matches.push({ index: m.index, text: m[0].trim() });
  }
  if (matches.length === 0) return null;

  const threshold = text.length * (THRESHOLD_PCT / 100);
  const validos      = matches.filter(x => x.index >= threshold);
  const descartados  = matches.filter(x => x.index <  threshold);

  if (validos.length === 0) {
    return { wordOffset: null, charOffset: null, marker: null, descartados };
  }

  const first = validos[0];
  const wordsBefore = text.slice(0, first.index).split(/\s+/).filter(Boolean).length;

  return {
    wordOffset : wordsBefore,
    charOffset : first.index,
    marker     : first.text,
    descartados,
  };
}

// ─── extractText (idéntico al ingest) ─────────────────────────────────────────

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

// ─── Walker ───────────────────────────────────────────────────────────────────

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

function sample(arr, n) {
  const copy = [...arr];
  for (let i = copy.length - 1; i > 0; i--) {
    const j = Math.floor(Math.random() * (i + 1));
    [copy[i], copy[j]] = [copy[j], copy[i]];
  }
  return copy.slice(0, n);
}

function snippet(text, charOffset, before = 80, after = 100) {
  const start = Math.max(0, charOffset - before);
  const end   = Math.min(text.length, charOffset + after);
  return text.slice(start, end).replace(/\s+/g, ' ').trim();
}

// ─── Main ─────────────────────────────────────────────────────────────────────

async function main() {
  const organos = ORGANO_FILTER
    ? [ORGANO_FILTER]
    : ORGANOS_RELEVANTES.filter(o => fs.existsSync(path.join(UPLOADS_DIR, o)));

  console.log(`\n=== Test detector de salvamentos ===`);
  console.log(`Threshold posición : ${THRESHOLD_PCT}% del documento`);
  console.log(`Muestra por órgano : ${SAMPLE_PER_ORGANO}`);
  console.log(`Órganos            : ${organos.length} (${organos.join(', ')})\n`);

  let totalConSalv = 0, totalSinSalv = 0, totalErrores = 0, totalSospechosos = 0;

  for (const organo of organos) {
    const dir = path.join(UPLOADS_DIR, organo);
    const all = findFiles(dir);
    if (all.length === 0) {
      console.log(`\n─── ${organo}: sin archivos ───`);
      continue;
    }
    const muestra = sample(all, SAMPLE_PER_ORGANO);

    console.log(`\n─── ${organo} (muestra: ${muestra.length}/${all.length}) ───\n`);

    for (const filePath of muestra) {
      const filename = path.basename(filePath);
      let text;
      try {
        text = await extractText(filePath);
      } catch (e) {
        console.log(`  [ERROR] ${filename}: ${e.message.slice(0, 80)}`);
        totalErrores++;
        continue;
      }

      const totalWords = text.split(/\s+/).filter(Boolean).length;
      const r = findSalvamento(text);

      if (r === null) {
        console.log(`  [SIN]  ${filename}  (${totalWords}p)`);
        totalSinSalv++;
        continue;
      }

      if (r.wordOffset === null) {
        const ej  = r.descartados[0];
        const pct = ((ej.index / text.length) * 100).toFixed(0);
        console.log(`  [?]    ${filename}  (${totalWords}p, ${r.descartados.length} match descartado(s), primero al ${pct}%)`);
        console.log(`         ctx: "${snippet(text, ej.index)}"`);
        totalSospechosos++;
        continue;
      }

      const pctPos = ((r.charOffset / text.length) * 100).toFixed(0);
      console.log(`  [SAL]  ${filename}  (palabra ${r.wordOffset}/${totalWords}, ${pctPos}%)`);
      console.log(`         marcador: "${r.marker}"`);
      console.log(`         ctx: "${snippet(text, r.charOffset)}"`);
      if (r.descartados.length > 0) {
        console.log(`         (${r.descartados.length} otro(s) match(es) descartado(s) por posición temprana)`);
      }
      totalConSalv++;
    }
  }

  console.log(`\n${'─'.repeat(60)}`);
  console.log(`RESUMEN`);
  console.log(`  Con salvamento detectado : ${totalConSalv}`);
  console.log(`  Sin salvamento           : ${totalSinSalv}`);
  console.log(`  Sospechosos (match descartado por posición): ${totalSospechosos}`);
  console.log(`  Errores                  : ${totalErrores}`);
}

main().catch(e => { console.error('FATAL:', e); process.exit(1); });
