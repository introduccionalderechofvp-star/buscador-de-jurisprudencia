/**
 * Log estructurado de errores de scrapers. Una línea JSON por error, en
 * archivos agrupados por fecha.
 *
 *   scrapers/errors/2026-04-16.jsonl
 *
 * Cada línea es parseable individualmente (JSONL). Permite:
 *   - Post-mortem: qué falló, cuándo, con qué mensaje
 *   - Retry dirigido: procesar solo los errores de una fecha específica
 *   - Análisis de patrones: ¿qué corporación falla más? ¿qué horas?
 */

import fs from 'fs';
import path from 'path';

const ERRORS_DIR = path.join(process.cwd(), 'scrapers', 'errors');

function today() {
  return new Date().toISOString().slice(0, 10);  // YYYY-MM-DD
}

/**
 * Registra un error al log del día.
 *
 * @param {string} scraper — nombre del scraper (ej: "corte-suprema-civil")
 * @param {object} details — cualquier info útil sobre el error.
 *                           Ejemplos: {doc_title, doc_path, error, attempt}
 */
export function logError(scraper, details) {
  fs.mkdirSync(ERRORS_DIR, { recursive: true });

  const line = JSON.stringify({
    timestamp: new Date().toISOString(),
    scraper,
    ...details
  });

  const file = path.join(ERRORS_DIR, `${today()}.jsonl`);
  fs.appendFileSync(file, line + '\n');
}

/**
 * Lee todos los errores de una fecha (YYYY-MM-DD).
 * Útil para retry scripts o para el panel de admin.
 *
 * @returns {Array<object>} array de errores parseados
 */
export function readErrors(date = today()) {
  const file = path.join(ERRORS_DIR, `${date}.jsonl`);
  if (!fs.existsSync(file)) return [];

  return fs.readFileSync(file, 'utf8')
    .split('\n')
    .filter(Boolean)
    .map(line => {
      try { return JSON.parse(line); }
      catch { return { malformed: line }; }
    });
}
