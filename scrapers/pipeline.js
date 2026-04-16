/**
 * Pipeline downstream — lo que pasa DESPUÉS de que uno o más scrapers
 * descargaron archivos nuevos a uploads/.
 *
 *   PDFs nuevos en disco
 *          │
 *          ▼
 *   scripts/ocr-saltados.js      (OCR si los PDFs no tienen texto)
 *          │
 *          ▼
 *   scripts/ingest-bulk.js       (indexa chunks nuevos en Qdrant)
 *          │
 *          ▼
 *   Sentencias nuevas disponibles en el buscador web + MCP
 *
 * Se dispara solo cuando el orquestador detectó al menos una descarga nueva.
 * Si nadie bajó nada, no tiene sentido correr estos scripts — ambos son
 * idempotentes y no harían nada útil, solo consumirían CPU.
 *
 * Los scripts se invocan como subprocesos (no import) para aislar su
 * lifecycle y sus variables globales de dotenv/config del orquestador.
 */

import { spawn } from 'node:child_process';
import path from 'node:path';
import fs from 'node:fs';

function runNodeScript(scriptPath, label) {
  return new Promise((resolve, reject) => {
    if (!fs.existsSync(scriptPath)) {
      return reject(new Error(`Script no existe: ${scriptPath}`));
    }

    const startTime = Date.now();
    console.log(`\n[pipeline] ▶ Ejecutando ${label}: ${scriptPath}`);
    console.log('  ─────────────────────────────────────────────────────────────');

    const proc = spawn('node', [scriptPath], {
      stdio: 'inherit',       // los logs del subproceso salen directo a stdout/stderr nuestros
      env: process.env,
      cwd: process.cwd()
    });

    proc.on('close', code => {
      const elapsedMin = ((Date.now() - startTime) / 1000 / 60).toFixed(1);
      console.log('  ─────────────────────────────────────────────────────────────');
      if (code === 0) {
        console.log(`[pipeline] ✓ ${label} terminó OK en ${elapsedMin} min`);
        resolve({ label, exitCode: code, elapsedMin });
      } else {
        console.error(`[pipeline] ✗ ${label} falló con exit code ${code} tras ${elapsedMin} min`);
        reject(new Error(`${label} salió con código ${code}`));
      }
    });

    proc.on('error', err => {
      console.error(`[pipeline] Error arrancando ${label}:`, err.message);
      reject(err);
    });
  });
}

/**
 * Ejecuta los dos scripts downstream en orden: OCR primero (si algún PDF nuevo
 * no tiene texto), luego ingest (indexa todos los nuevos a Qdrant).
 *
 * Si OCR falla, NO se corre el ingest — porque eso podría dejar archivos a
 * medias en Qdrant que después serían difíciles de limpiar. Preferimos que
 * un operador vea el fallo y decida qué hacer antes de indexar.
 *
 * Ambos scripts son idempotentes: si los archivos ya están procesados, no
 * hacen trabajo redundante. Eso hace seguro correr la pipeline aun cuando
 * el volumen de "nuevos" sea bajo.
 */
export async function runDownstream() {
  const cwd = process.cwd();
  const ocrScript    = path.join(cwd, 'scripts', 'ocr-saltados.js');
  const ingestScript = path.join(cwd, 'scripts', 'ingest-bulk.js');

  console.log('\n══════════════════════════════════════════════════════════════');
  console.log('  PIPELINE DOWNSTREAM — OCR → Ingest');
  console.log('══════════════════════════════════════════════════════════════');

  const results = [];

  try {
    results.push(await runNodeScript(ocrScript, 'OCR (ocr-saltados.js)'));
  } catch (e) {
    console.error(`[pipeline] OCR falló. ABORTO — no se corre el ingest.`);
    throw e;
  }

  try {
    results.push(await runNodeScript(ingestScript, 'Ingest (ingest-bulk.js)'));
  } catch (e) {
    console.error(`[pipeline] Ingest falló.`);
    throw e;
  }

  console.log('\n[pipeline] ✓ Pipeline downstream completado.');
  return results;
}
