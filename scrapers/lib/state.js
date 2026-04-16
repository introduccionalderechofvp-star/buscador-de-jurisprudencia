/**
 * Estado persistente y lock files para scrapers.
 *
 * Dos responsabilidades independientes:
 *
 * 1. Estado por corporación/sala: qué página terminamos, cuándo fue la última
 *    corrida, cuántos docs hemos bajado, etc. Se lee al arrancar para poder
 *    reanudar, se escribe tras cada checkpoint. Writes atómicos con rename.
 *
 * 2. Lock files: evitar que dos instancias del mismo scraper corran a la vez.
 *    El lock contiene el PID del proceso dueño. Si otro proceso encuentra un
 *    lock cuyo PID ya no existe (lock huérfano de un crash), lo limpia y
 *    continúa.
 */

import fs from 'fs';
import path from 'path';

const STATE_DIR = path.join(process.cwd(), 'scrapers', 'state');

function ensureDir() {
  fs.mkdirSync(STATE_DIR, { recursive: true });
}

function statePath(name) {
  return path.join(STATE_DIR, `${name}.json`);
}

function lockPath(name) {
  return path.join(STATE_DIR, `${name}.lock`);
}

// ─── Estado persistente ───────────────────────────────────────────────────────

/** Lee el estado de un scraper. Devuelve null si no existe. */
export function readState(name) {
  ensureDir();
  try {
    return JSON.parse(fs.readFileSync(statePath(name), 'utf8'));
  } catch {
    return null;
  }
}

/** Escribe el estado de forma atómica (temp + rename). */
export function writeState(name, state) {
  ensureDir();
  const file = statePath(name);
  const tmp = file + '.tmp';
  fs.writeFileSync(tmp, JSON.stringify(state, null, 2));
  fs.renameSync(tmp, file);
}

// ─── Lock files ───────────────────────────────────────────────────────────────

/** Determina si un PID está vivo (sin matarlo). */
function isPidAlive(pid) {
  try {
    // signal 0 es un no-op que solo verifica que el proceso existe y es accesible
    process.kill(pid, 0);
    return true;
  } catch {
    return false;
  }
}

/**
 * Intenta adquirir el lock para `name`.
 *
 * - Si no hay lock → lo crea con nuestro PID y devuelve { acquired: true }.
 * - Si hay lock pero el PID dueño ya murió (lock huérfano) → lo limpia y
 *   lo recrea con nuestro PID.
 * - Si hay lock con un PID vivo → devuelve { acquired: false, heldByPid }.
 *
 * Siempre llama a releaseLock(name) al terminar, idealmente en un try/finally.
 */
export function acquireLock(name) {
  ensureDir();
  const file = lockPath(name);

  if (fs.existsSync(file)) {
    let heldByPid = null;
    try {
      const data = JSON.parse(fs.readFileSync(file, 'utf8'));
      heldByPid = data.pid;
    } catch {
      // Lock corrupto — lo tratamos como huérfano
    }

    if (heldByPid && isPidAlive(heldByPid)) {
      return { acquired: false, heldByPid };
    }

    // Lock huérfano (PID muerto o archivo corrupto). Limpiar.
    console.error(`[state] Lock huérfano detectado (PID ${heldByPid ?? 'desconocido'}). Limpiando.`);
    try { fs.unlinkSync(file); } catch {}
  }

  const data = {
    pid: process.pid,
    acquired_at: new Date().toISOString(),
    hostname: process.env.HOSTNAME || 'unknown'
  };
  fs.writeFileSync(file, JSON.stringify(data));
  return { acquired: true };
}

/** Libera el lock. Silencioso si no existe (idempotente). */
export function releaseLock(name) {
  try {
    fs.unlinkSync(lockPath(name));
  } catch {}
}
