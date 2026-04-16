/**
 * Almacenamiento de PDFs descargados, respetando la convención del buscador:
 *
 *   uploads/<Órgano>/<Año>/<archivo>.pdf
 *
 * Usa write-to-temp + rename atómico para garantizar que un archivo con nombre
 * final SIEMPRE implica descarga completa. Si el proceso muere a mitad de
 * escritura, queda un `.pdf.tmp` que se descarta en la siguiente corrida.
 */

import fs from 'fs';
import path from 'path';

const UPLOADS_DIR = path.join(process.cwd(), 'uploads');

// Caracteres prohibidos en nombres de archivo en Windows/Linux/macOS.
// Los reemplazamos por "_" para que los PDFs sean portables entre sistemas.
const UNSAFE_CHARS = /[\\/:*?"<>|]/g;

/** Sanitiza un string para que sea seguro como nombre de archivo. */
export function safeName(s) {
  return String(s ?? '').replace(UNSAFE_CHARS, '_').trim();
}

/** Construye la ruta completa donde vivirá un archivo, sin crear nada todavía. */
export function pathFor(organo, año, filename) {
  const dir = path.join(UPLOADS_DIR, safeName(organo), String(año));
  const full = path.join(dir, safeName(filename));
  return { dir, full };
}

/** ¿Ya tenemos este archivo descargado? */
export function exists(organo, año, filename) {
  return fs.existsSync(pathFor(organo, año, filename).full);
}

/**
 * Guarda un buffer en disco de forma atómica.
 * Escribe a `<archivo>.tmp`, luego hace rename al nombre final.
 * Si el proceso muere a mitad, el `.tmp` queda suelto pero el nombre final
 * NUNCA contiene bytes parciales.
 *
 * @returns {string} la ruta completa del archivo ya guardado
 */
export function saveAtomic(organo, año, filename, buffer) {
  const { dir, full } = pathFor(organo, año, filename);
  fs.mkdirSync(dir, { recursive: true });

  const tmp = full + '.tmp';
  fs.writeFileSync(tmp, buffer);
  fs.renameSync(tmp, full);

  return full;
}

/**
 * Limpia `.tmp` huérfanos que quedaron de corridas anteriores muertas.
 * Se puede llamar al arrancar un scraper para dejar el árbol limpio.
 */
export function cleanupTmp(organo) {
  const base = path.join(UPLOADS_DIR, safeName(organo));
  if (!fs.existsSync(base)) return 0;

  let removed = 0;
  function walk(dir) {
    for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
      const full = path.join(dir, entry.name);
      if (entry.isDirectory()) walk(full);
      else if (entry.name.endsWith('.tmp')) {
        fs.unlinkSync(full);
        removed++;
      }
    }
  }
  walk(base);
  return removed;
}

/** Devuelve el file_path relativo (el que usa el buscador/MCP) para un archivo. */
export function relativePath(organo, año, filename) {
  return path.join(safeName(organo), String(año), safeName(filename));
}

/**
 * Construye un Set con los nombres "base" (filename SIN extensión) de todas
 * las sentencias presentes en cualquier nivel bajo `uploads/<organo>/`.
 *
 * Se incluyen archivos .pdf, .docx y .doc, con el basename normalizado a
 * minúsculas para comparación case-insensitive. Eso permite al scraper tratar
 * una sentencia como "ya existente" sin importar en qué formato esté guardada
 * ni dónde en el árbol viva. Ejemplo:
 *
 *   uploads/<organo>/2017/SC5208-2017.pdf        → "sc5208-2017"
 *   uploads/<organo>/.../TodosLosAños/X.docx     → "x"
 *   uploads/<organo>/2020/Y.DOC                  → "y"
 *
 * Sidecars AppleDouble de macOS (._*) se ignoran, consistente con
 * scripts/ingest-bulk.js y scripts/ocr-saltados.js.
 *
 * @returns {Set<string>} basenames (lowercase, sin extensión) ya presentes
 */
export function buildBasenameIndex(organo) {
  const base = path.join(UPLOADS_DIR, safeName(organo));
  const names = new Set();
  if (!fs.existsSync(base)) return names;

  const SUPPORTED_EXT = /\.(pdf|docx|doc)$/i;

  function walk(dir) {
    for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
      // Ignorar sidecars AppleDouble de macOS (consistente con ingest-bulk.js)
      if (entry.name.startsWith('._')) continue;
      const full = path.join(dir, entry.name);
      if (entry.isDirectory()) walk(full);
      else if (entry.isFile() && SUPPORTED_EXT.test(entry.name)) {
        names.add(entry.name.replace(SUPPORTED_EXT, '').toLowerCase());
      }
    }
  }
  walk(base);
  return names;
}
