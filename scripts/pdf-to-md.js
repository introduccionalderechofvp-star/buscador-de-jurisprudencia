/**
 * pdf-to-md.js
 *
 * Convierte una carpeta de PDFs en archivos .md (texto plano con extensión md)
 * preservando la estructura de subcarpetas. Pensado para correr LOCALMENTE
 * (PC del usuario, no en VPS) antes de subir solo los .md al VPS.
 *
 * GARANTÍA: este script SOLO LEE los PDFs originales. Nunca los elimina,
 * mueve, ni modifica. El output se escribe en una carpeta separada.
 *
 * Uso (desde el repo, con node instalado):
 *   node scripts/pdf-to-md.js <carpeta-pdfs> <carpeta-output-md>
 *
 * Ejemplo Windows:
 *   node scripts/pdf-to-md.js "C:\providencias\consejo-de-estado" "C:\providencias\consejo-de-estado-md"
 *
 * Ejemplo Linux/Mac:
 *   node scripts/pdf-to-md.js ~/Downloads/CE ~/Downloads/CE-md
 *
 * Idempotente: si un .md ya existe en el output, lo salta (no reprocesa).
 * Útil para reanudar si la corrida se interrumpe.
 */

import fs from 'fs';
import path from 'path';
import pdf from 'pdf-parse';

const SRC_DIR = process.argv[2];
const OUT_DIR = process.argv[3];

if (!SRC_DIR || !OUT_DIR) {
  console.error('Uso: node scripts/pdf-to-md.js <carpeta-pdfs> <carpeta-output-md>');
  process.exit(1);
}

if (!fs.existsSync(SRC_DIR)) {
  console.error(`ERROR: No existe la carpeta de origen: ${SRC_DIR}`);
  process.exit(1);
}

// Resolver paths absolutos para chequear que no son la misma carpeta
const srcAbs = path.resolve(SRC_DIR);
const outAbs = path.resolve(OUT_DIR);
if (srcAbs === outAbs) {
  console.error('ERROR: la carpeta de output no puede ser la misma que la de origen.');
  console.error('       Por seguridad, los .md se escriben en una carpeta separada.');
  process.exit(1);
}
if (outAbs.startsWith(srcAbs + path.sep)) {
  console.error('ERROR: la carpeta de output no puede estar adentro de la de origen.');
  process.exit(1);
}

fs.mkdirSync(OUT_DIR, { recursive: true });

function findPDFs(dir) {
  const results = [];
  for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
    if (entry.name.startsWith('._')) continue;  // ignorar AppleDouble metadata de macOS
    const full = path.join(dir, entry.name);
    if (entry.isDirectory()) results.push(...findPDFs(full));
    else if (entry.isFile() && entry.name.toLowerCase().endsWith('.pdf')) results.push(full);
  }
  return results;
}

function fmt(n) {
  return n.toLocaleString('es-CO');
}

async function main() {
  console.log(`\n=== PDF → Markdown ===`);
  console.log(`Origen: ${srcAbs}`);
  console.log(`Output: ${outAbs}`);
  console.log(`(los PDFs originales NO se tocan)\n`);

  const pdfs = findPDFs(SRC_DIR);
  if (pdfs.length === 0) {
    console.log('No se encontraron PDFs en la carpeta.');
    return;
  }

  console.log(`PDFs encontrados: ${fmt(pdfs.length)}\n`);

  const startTime = Date.now();
  let processed = 0, skipped = 0, errors = 0;
  let totalCharsOut = 0, totalBytesIn = 0;

  for (let i = 0; i < pdfs.length; i++) {
    const pdfPath = pdfs[i];
    const relPath = path.relative(SRC_DIR, pdfPath);
    const mdRelPath = relPath.replace(/\.pdf$/i, '.md');
    const mdPath = path.join(OUT_DIR, mdRelPath);
    const filename = path.basename(pdfPath);
    const shortName = filename.length > 50 ? filename.slice(0, 47) + '...' : filename;
    const prefix = `[${i + 1}/${pdfs.length}]`;

    process.stdout.write(`${prefix} ${shortName.padEnd(52)} `);

    // Idempotente: si ya existe el .md, saltar
    if (fs.existsSync(mdPath)) {
      console.log('YA EXISTE — saltado');
      skipped++;
      continue;
    }

    try {
      const stat = fs.statSync(pdfPath);
      totalBytesIn += stat.size;

      const buffer = fs.readFileSync(pdfPath);
      const parsed = await pdf(buffer);
      const text = (parsed.text || '').trim();

      if (text.length < 50) {
        console.log('SIN TEXTO (puede necesitar OCR) — saltado');
        skipped++;
        continue;
      }

      // Asegurar que existe la subcarpeta de destino
      fs.mkdirSync(path.dirname(mdPath), { recursive: true });

      // Header opcional con metadata mínima — útil si querés ver de qué PDF salió
      const header = `<!-- source: ${relPath.replace(/\\/g, '/')} pages: ${parsed.numpages || '?'} -->\n\n`;
      fs.writeFileSync(mdPath, header + text, 'utf8');

      totalCharsOut += text.length;
      processed++;
      console.log(`${fmt(text.length)} chars → ${path.basename(mdPath)}`);
    } catch (e) {
      console.log(`ERROR: ${e.message.slice(0, 60)}`);
      errors++;
    }
  }

  const elapsed = ((Date.now() - startTime) / 1000 / 60).toFixed(1);
  const compressionRatio = totalBytesIn > 0
    ? ((1 - (totalCharsOut / totalBytesIn)) * 100).toFixed(1)
    : '0';

  console.log(`\n${'─'.repeat(60)}`);
  console.log(`RESUMEN`);
  console.log(`  PDFs encontrados : ${fmt(pdfs.length)}`);
  console.log(`  Convertidos      : ${fmt(processed)}`);
  console.log(`  Ya existían (saltados): ${fmt(skipped)}`);
  console.log(`  Errores          : ${fmt(errors)}`);
  console.log(`  Tamaño PDFs in   : ${fmt(Math.round(totalBytesIn / 1024 / 1024))} MB`);
  console.log(`  Tamaño MD out    : ${fmt(Math.round(totalCharsOut / 1024 / 1024))} MB (~${compressionRatio}% menos)`);
  console.log(`  Tiempo           : ${elapsed} min`);
  console.log(`\n  PDFs originales: INTACTOS en ${srcAbs}`);
  console.log(`  Archivos .md   : en ${outAbs}`);
}

main().catch(e => {
  console.error('\nERROR FATAL:', e.message);
  process.exit(1);
});
