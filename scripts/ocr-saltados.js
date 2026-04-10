/**
 * ocr-saltados.js
 *
 * Aplica OCR a los PDFs sin texto extraíble en uploads/.
 * Usa ocrmypdf + tesseract (español) para incrustar texto en el PDF.
 * Al terminar, corre node scripts/ingest-bulk.js para indexar los nuevos.
 *
 * Requisitos previos:
 *   apt-get install -y ocrmypdf tesseract-ocr-spa
 *
 * Uso:
 *   node scripts/ocr-saltados.js
 */

import 'dotenv/config';
import fs   from 'fs';
import path from 'path';
import { execSync } from 'child_process';
import pdf  from 'pdf-parse';

// ─── Config ───────────────────────────────────────────────────────────────────

const UPLOADS_DIR = path.join(process.cwd(), 'uploads');
const MIN_TEXT    = 50;        // mínimo de caracteres para considerar que hay texto
const OCR_TIMEOUT = 300_000;   // 5 min por archivo (PDFs grandes pueden tardar)
const LOG_FILE    = path.join(process.cwd(), 'ocr-saltados.log');

// ─── Helpers ──────────────────────────────────────────────────────────────────

function findPDFs(dir) {
  const results = [];
  for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
    const full = path.join(dir, entry.name);
    if (entry.isDirectory())                                    results.push(...findPDFs(full));
    else if (entry.isFile() && entry.name.toLowerCase().endsWith('.pdf')) results.push(full);
  }
  return results;
}

function log(msg) {
  const line = `[${new Date().toISOString()}] ${msg}`;
  console.log(msg);
  fs.appendFileSync(LOG_FILE, line + '\n');
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

function fmt(n) { return n.toLocaleString('es-CO'); }

// ─── Main ─────────────────────────────────────────────────────────────────────

async function main() {
  log('=== OCR de PDFs sin texto ===');
  log(`Directorio: ${UPLOADS_DIR}\n`);

  if (!fs.existsSync(UPLOADS_DIR)) {
    console.error('ERROR: No existe la carpeta uploads/');
    process.exit(1);
  }

  // ── Paso 1: identificar archivos sin texto ──────────────────────────────────
  const allPdfs = findPDFs(UPLOADS_DIR);
  log(`PDFs encontrados: ${fmt(allPdfs.length)}`);
  log('Identificando sin texto (puede tardar unos minutos)...\n');

  const sinTexto  = [];
  const corruptos = [];

  for (let i = 0; i < allPdfs.length; i++) {
    const filePath = allPdfs[i];
    process.stdout.write(`\r  Revisando ${fmt(i + 1)}/${fmt(allPdfs.length)}...`);

    let text = '';
    try {
      text = await extractText(filePath);
    } catch {
      corruptos.push(filePath);
      continue;
    }

    if (text.length < MIN_TEXT) sinTexto.push(filePath);
  }

  process.stdout.write('\n');
  log(`\nSin texto  : ${fmt(sinTexto.length)}`);
  log(`Corruptos  : ${fmt(corruptos.length)}`);

  // Guardar listas
  fs.writeFileSync(
    path.join(process.cwd(), 'lista-sin-texto.txt'),
    sinTexto.join('\n') + '\n'
  );
  fs.writeFileSync(
    path.join(process.cwd(), 'lista-corruptos.txt'),
    corruptos.join('\n') + '\n'
  );
  log('\nListas guardadas en lista-sin-texto.txt y lista-corruptos.txt');

  if (!sinTexto.length) {
    log('No hay archivos para procesar con OCR.');
    return;
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

    process.stdout.write(`${label} ${shortName.padEnd(52)} `);

    try {
      execSync(
        `ocrmypdf -l spa --force-ocr --output-type pdf "${filePath}" "${tmpPath}"`,
        { stdio: 'pipe', timeout: OCR_TIMEOUT }
      );

      // Verificar que el resultado tiene texto
      const newText = await extractText(tmpPath);
      if (newText.length >= MIN_TEXT) {
        fs.renameSync(tmpPath, filePath);
        console.log(`OK  (${newText.length} chars)`);
        ok++;
      } else {
        fs.unlinkSync(tmpPath);
        console.log('SIN TEXTO TRAS OCR (imagen ilegible)');
        errores++;
      }
    } catch (e) {
      if (fs.existsSync(tmpPath)) fs.unlinkSync(tmpPath);
      const msg = e.stderr?.toString().slice(0, 80) || e.message.slice(0, 80);
      console.log(`ERROR: ${msg}`);
      errores++;
    }
  }

  const elapsed = ((Date.now() - startTime) / 1000 / 60).toFixed(1);

  log(`\n${'─'.repeat(60)}`);
  log('RESUMEN OCR');
  log(`  Procesados con éxito : ${fmt(ok)}`);
  log(`  Errores / ilegibles  : ${fmt(errores)}`);
  log(`  Tiempo               : ${elapsed} min`);
  log('\nSiguiente paso:');
  log('  node scripts/ingest-bulk.js');
  log('  (salta los ya indexados y procesa los nuevos OCR)\n');
}

main().catch(e => { console.error('\nERROR FATAL:', e.message); process.exit(1); });
