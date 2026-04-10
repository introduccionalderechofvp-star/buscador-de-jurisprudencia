/**
 * ocr-saltados.js
 *
 * Aplica OCR a los PDFs sin texto extraíble en uploads/.
 * Usa ocrmypdf + tesseract (español) para incrustar texto en el PDF.
 *
 * Paso 1: obtiene la lista de archivos sin texto del log del ingest
 *         (mucho más rápido que re-escanear los 31K PDFs).
 * Paso 2: aplica OCR archivo por archivo.
 *
 * Requisitos previos:
 *   apt-get install -y ocrmypdf tesseract-ocr-spa
 *
 * Uso:
 *   node scripts/ocr-saltados.js
 *   node scripts/ocr-saltados.js --rescan   # fuerza re-escaneo completo
 */

import 'dotenv/config';
import fs   from 'fs';
import path from 'path';
import { execSync } from 'child_process';
import pdf  from 'pdf-parse';

// ─── Config ───────────────────────────────────────────────────────────────────

const UPLOADS_DIR   = path.join(process.cwd(), 'uploads');
const INGEST_LOG    = path.join(process.cwd(), 'ingest.log');   // log del ingest-bulk
const INGEST_LOG2   = path.join(process.cwd(), 'ingest2.log');
const LISTA_FILE    = path.join(process.cwd(), 'lista-sin-texto.txt');
const OCR_LOG       = path.join(process.cwd(), 'ocr-saltados.log');
const MIN_TEXT      = 50;
const OCR_TIMEOUT   = 300_000;   // 5 min por archivo

const FORCE_RESCAN  = process.argv.includes('--rescan');

// ─── Helpers ──────────────────────────────────────────────────────────────────

function log(msg) {
  const line = `[${new Date().toISOString()}] ${msg}`;
  console.log(msg);
  fs.appendFileSync(OCR_LOG, line + '\n');
}

function fmt(n) { return Number(n).toLocaleString('es-CO'); }

function findPDFs(dir) {
  const results = [];
  for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
    const full = path.join(dir, entry.name);
    if (entry.isDirectory()) results.push(...findPDFs(full));
    else if (entry.isFile() && entry.name.toLowerCase().endsWith('.pdf')) results.push(full);
  }
  return results;
}

async function extractText(filePath) {
  try {
    const buf    = fs.readFileSync(filePath);
    const parsed = await pdf(buf);
    return (parsed.text || '').trim();
  } catch {
    return '';
  }
}

// ─── Obtener lista de saltados ────────────────────────────────────────────────

/**
 * Extrae nombres de archivo del log del ingest buscando líneas con
 * "SALTADO (sin texto)", luego localiza el archivo completo en uploads/.
 */
function getSaltadosFromLog() {
  const logFiles = [INGEST_LOG, INGEST_LOG2].filter(f => fs.existsSync(f));
  if (!logFiles.length) return null;

  log('Leyendo lista de saltados del log del ingest...');

  // Construir índice nombre→ruta completa de todos los PDFs
  log('Construyendo índice de archivos...');
  const allPdfs = findPDFs(UPLOADS_DIR);
  const index   = new Map();   // basename → full path (último gana si hay duplicado)
  for (const p of allPdfs) index.set(path.basename(p), p);
  log(`  ${fmt(allPdfs.length)} PDFs indexados.`);

  const found   = new Set();
  const missing = [];

  for (const logFile of logFiles) {
    const lines = fs.readFileSync(logFile, 'utf8').split('\n');
    for (const line of lines) {
      if (!line.includes('SALTADO (sin texto)')) continue;

      // Formato: "[x/y] nombre_del_archivo.pdf    SALTADO (sin texto)"
      // El nombre puede estar truncado con "..." si supera 45 chars.
      const match = line.match(/\]\s+(.+?)\s{2,}SALTADO/);
      if (!match) continue;

      let name = match[1].trim();
      if (name.endsWith('...')) {
        // Nombre truncado: buscar por prefijo
        const prefix = name.slice(0, -3);
        for (const [basename, fullPath] of index) {
          if (basename.startsWith(prefix) && !found.has(fullPath)) {
            found.add(fullPath);
          }
        }
      } else {
        const fullPath = index.get(name);
        if (fullPath && !found.has(fullPath)) {
          found.add(fullPath);
        } else if (!fullPath) {
          missing.push(name);
        }
      }
    }
  }

  if (missing.length) {
    log(`  Advertencia: ${missing.length} nombres no encontrados en uploads/ (pueden haber sido movidos).`);
  }

  return [...found];
}

// ─── Main ─────────────────────────────────────────────────────────────────────

async function main() {
  log('=== OCR de PDFs sin texto ===\n');

  if (!fs.existsSync(UPLOADS_DIR)) {
    console.error('ERROR: No existe uploads/'); process.exit(1);
  }

  // ── Paso 1: obtener lista ──────────────────────────────────────────────────
  let sinTexto = [];

  // Opción A: usar lista guardada de una corrida anterior
  if (!FORCE_RESCAN && fs.existsSync(LISTA_FILE)) {
    sinTexto = fs.readFileSync(LISTA_FILE, 'utf8').split('\n').filter(Boolean);
    log(`Lista existente cargada: ${fmt(sinTexto.length)} archivos.`);

  // Opción B: extraer del log del ingest (rápido)
  } else if (!FORCE_RESCAN) {
    const fromLog = getSaltadosFromLog();
    if (fromLog && fromLog.length > 0) {
      sinTexto = fromLog;
      log(`Encontrados en log del ingest: ${fmt(sinTexto.length)} archivos.`);
      fs.writeFileSync(LISTA_FILE, sinTexto.join('\n') + '\n');

  // Opción C: re-escanear todo (lento, solo con --rescan)
    } else {
      log('No se encontró log del ingest. Escaneando todos los PDFs...');
      log('(Esto puede tardar 1-2 horas. Usa --rescan para forzarlo en el futuro)\n');
      const allPdfs = findPDFs(UPLOADS_DIR);
      for (let i = 0; i < allPdfs.length; i++) {
        if ((i + 1) % 500 === 0) log(`  Revisando ${fmt(i + 1)}/${fmt(allPdfs.length)}...`);
        const text = await extractText(allPdfs[i]);
        if (text.length < MIN_TEXT) sinTexto.push(allPdfs[i]);
      }
      fs.writeFileSync(LISTA_FILE, sinTexto.join('\n') + '\n');
    }
  } else {
    log('--rescan: escaneando todos los PDFs...');
    const allPdfs = findPDFs(UPLOADS_DIR);
    for (let i = 0; i < allPdfs.length; i++) {
      if ((i + 1) % 500 === 0) log(`  Revisando ${fmt(i + 1)}/${fmt(allPdfs.length)}...`);
      const text = await extractText(allPdfs[i]);
      if (text.length < MIN_TEXT) sinTexto.push(allPdfs[i]);
    }
    fs.writeFileSync(LISTA_FILE, sinTexto.join('\n') + '\n');
  }

  if (!sinTexto.length) {
    log('No hay archivos para procesar.'); return;
  }

  // ── Paso 2: aplicar OCR ────────────────────────────────────────────────────
  log(`\n${'─'.repeat(60)}`);
  log(`Aplicando OCR a ${fmt(sinTexto.length)} archivos...\n`);

  let ok = 0, errores = 0;
  const startTime = Date.now();

  for (let i = 0; i < sinTexto.length; i++) {
    const filePath  = sinTexto[i];
    const name      = path.basename(filePath);
    const shortName = name.length > 50 ? name.slice(0, 47) + '...' : name;
    const label     = `[${i + 1}/${sinTexto.length}]`;
    const tmpPath   = filePath + '.__ocr__.pdf';

    log(`${label} ${shortName}`);

    try {
      execSync(
        `ocrmypdf -l spa --force-ocr --output-type pdf "${filePath}" "${tmpPath}"`,
        { stdio: 'pipe', timeout: OCR_TIMEOUT }
      );
      const newText = await extractText(tmpPath);
      if (newText.length >= MIN_TEXT) {
        fs.renameSync(tmpPath, filePath);
        log(`  → OK (${newText.length} chars)`);
        ok++;
      } else {
        fs.unlinkSync(tmpPath);
        log('  → SIN TEXTO TRAS OCR (imagen ilegible)');
        errores++;
      }
    } catch (e) {
      if (fs.existsSync(tmpPath)) fs.unlinkSync(tmpPath);
      const msg = (e.stderr?.toString() || e.message).split('\n')[0].slice(0, 100);
      log(`  → ERROR: ${msg}`);
      errores++;
    }
  }

  const elapsed = ((Date.now() - startTime) / 1000 / 60).toFixed(1);
  log(`\n${'─'.repeat(60)}`);
  log('RESUMEN OCR');
  log(`  Procesados con éxito : ${fmt(ok)}`);
  log(`  Errores / ilegibles  : ${fmt(errores)}`);
  log(`  Tiempo               : ${elapsed} min`);
  log('\nSiguiente paso: node scripts/ingest-bulk.js\n');
}

main().catch(e => { console.error('\nERROR FATAL:', e.message); process.exit(1); });
