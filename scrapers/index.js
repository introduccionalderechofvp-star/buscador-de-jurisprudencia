#!/usr/bin/env node
/**
 * Orquestador del módulo scrapers/.
 *
 * Entry point diseñado para ser invocado por pm2 en un cron diario.
 * Corre todos los scrapers registrados en orden, y si alguno descargó
 * archivos nuevos, dispara el pipeline downstream (OCR → ingest).
 *
 * Invocación típica (cron diario en pm2):
 *
 *   pm2 start scrapers/index.js \
 *     --name scraper-diario \
 *     --cron "0 3 * * *" \
 *     --no-autorestart
 *
 * Invocación manual (para testing o trigger desde el panel admin):
 *
 *   node scrapers/index.js
 *
 * Para ejecutar UN scraper específico sin el orquestador completo, usa su
 * archivo directo:
 *
 *   node scrapers/corte-suprema.js --ad-hoc --sala Civil --max 10
 *
 * Exit codes:
 *   0 — todo ok (con o sin descargas nuevas, con o sin pipeline ejecutada)
 *   1 — algún scraper tuvo errores pero otros funcionaron
 *   2 — todos los scrapers fallaron
 *   3 — scrapers ok pero el pipeline downstream (OCR/ingest) falló
 */

import 'dotenv/config';

import { run as runCorteSuprema, printSummary as printCorteSuprema } from './corte-suprema.js';
import { runDownstream } from './pipeline.js';

// ─── Registro de scrapers ─────────────────────────────────────────────────────

// Cada entrada: { nombre descriptivo, función run, función printSummary }.
// Para agregar una corporación nueva, solo se agrega una entrada aquí.
const SCRAPERS = [
  {
    name: 'Corte Suprema de Justicia',
    run: runCorteSuprema,
    printSummary: printCorteSuprema,
    // CSJ_SALAS=Civil,Laboral,Penal limita a esas tres (omite Tutelas, p.ej.).
    // Sin la env var, el scraper usa su default (las 4 salas).
    options: process.env.CSJ_SALAS
      ? { salas: process.env.CSJ_SALAS.split(',').map(s => s.trim()).filter(Boolean) }
      : {}
  }
  // Futuros scrapers:
  // { name: 'Corte Constitucional', run: runCorteConstitucional, ... },
  // { name: 'Consejo de Estado',    run: runConsejoEstado,       ... },
];

// ─── Main ─────────────────────────────────────────────────────────────────────

async function main() {
  const startTime = Date.now();
  console.log('\n═══════════════════════════════════════════════════════════════');
  console.log('  ORQUESTADOR DE SCRAPERS');
  console.log(`  Inicio: ${new Date().toISOString()}`);
  console.log('═══════════════════════════════════════════════════════════════');

  let totalDownloaded = 0;
  let totalErrors = 0;
  let scrapersOk = 0;
  let scrapersFail = 0;

  for (const scraper of SCRAPERS) {
    try {
      const result = await scraper.run({ incremental: true, ...(scraper.options || {}) });
      scraper.printSummary(result);
      totalDownloaded += result.totals.downloaded;
      totalErrors     += result.totals.errors;
      scrapersOk++;
    } catch (e) {
      console.error(`\n[orquestador] ✗ Scraper "${scraper.name}" falló: ${e.message}`);
      scrapersFail++;
    }
  }

  // Decidir si disparar el pipeline downstream
  let pipelineOk = null;   // null = no se corrió, true = ok, false = falló
  if (totalDownloaded > 0) {
    console.log(`\n[orquestador] ${totalDownloaded} archivos nuevos en total. ` +
                `Disparando pipeline downstream.`);
    try {
      await runDownstream();
      pipelineOk = true;
    } catch (e) {
      console.error(`\n[orquestador] ✗ Pipeline downstream falló: ${e.message}`);
      pipelineOk = false;
    }
  } else {
    console.log(`\n[orquestador] Sin descargas nuevas. Pipeline downstream NO se dispara.`);
  }

  // Resumen final del orquestador
  const elapsedMin = ((Date.now() - startTime) / 1000 / 60).toFixed(1);
  console.log('\n═══════════════════════════════════════════════════════════════');
  console.log('  ORQUESTADOR — RESUMEN');
  console.log('═══════════════════════════════════════════════════════════════');
  console.log(`  Scrapers OK       : ${scrapersOk}/${SCRAPERS.length}`);
  console.log(`  Scrapers fallidos : ${scrapersFail}`);
  console.log(`  Descargas totales : ${totalDownloaded}`);
  console.log(`  Errores totales   : ${totalErrors}`);
  console.log(`  Pipeline downstream: ${
    pipelineOk === null ? 'no ejecutado (sin archivos nuevos)' :
    pipelineOk === true ? '✓ ok' :
    '✗ falló'
  }`);
  console.log(`  Tiempo total      : ${elapsedMin} min`);
  console.log(`  Fin: ${new Date().toISOString()}`);

  // Exit codes
  if (scrapersFail === SCRAPERS.length)      process.exit(2);
  if (pipelineOk === false)                  process.exit(3);
  if (scrapersFail > 0 || totalErrors > 0)   process.exit(1);
  process.exit(0);
}

main().catch(e => {
  console.error('\nERROR FATAL en orquestador:', e.message);
  console.error(e.stack);
  process.exit(1);
});
