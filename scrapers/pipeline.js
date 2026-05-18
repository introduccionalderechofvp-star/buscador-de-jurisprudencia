/**
 * Pipeline downstream — lo que pasa DESPUÉS de que uno o más scrapers
 * descargaron archivos nuevos a uploads/.
 *
 *   PDFs nuevos en disco
 *          │
 *          ▼
 *   scripts/ocr-saltados.js      (OCR — SOLO si RUN_OCR=1; ver nota abajo)
 *          │
 *          ▼
 *   scripts/ingest-bulk.js       (indexa chunks nuevos en Qdrant)
 *          │
 *          ▼
 *   Sentencias nuevas disponibles en el buscador web + MCP
 *
 * Se dispara solo cuando el orquestador detectó al menos una descarga nueva.
 *
 * NOTA SOBRE OCR: las providencias modernas de CSJ y Tribunal vienen como PDF
 * con capa de texto digital — pdf-parse las lee bien y NUNCA necesitan OCR.
 * El ocr-saltados.js solo sirve para cargas históricas de PDFs escaneados.
 * Correrlo en cada cron semanal desperdiciaba ~12 min de CPU verificando una
 * lista vieja y OCR-izando 0 archivos. Por eso ahora el OCR es opt-in:
 * solo corre si RUN_OCR=1 en el entorno. Para cargas históricas, correr
 * ocr-saltados.js a mano.
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
 * Ejecuta el ingest de lo nuevo. OCR solo si RUN_OCR=1 (ver nota arriba).
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
  console.log('  PIPELINE DOWNSTREAM — Ingest' + (process.env.RUN_OCR === '1' ? ' (con OCR)' : ' (OCR omitido)'));
  console.log('══════════════════════════════════════════════════════════════');

  const results = [];

  // OCR opt-in. Las providencias nuevas nunca lo necesitan; correrlo en cada
  // cron era ~12 min de CPU desperdiciados. Para cargas históricas de PDFs
  // escaneados: correr `RUN_OCR=1` o `node scripts/ocr-saltados.js` a mano.
  if (process.env.RUN_OCR === '1') {
    try {
      results.push(await runNodeScript(ocrScript, 'OCR (ocr-saltados.js)'));
    } catch (e) {
      console.error(`[pipeline] OCR falló. ABORTO — no se corre el ingest.`);
      throw e;
    }
  } else {
    console.log('\n[pipeline] OCR omitido (RUN_OCR != 1). Las providencias nuevas no lo necesitan.');
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
