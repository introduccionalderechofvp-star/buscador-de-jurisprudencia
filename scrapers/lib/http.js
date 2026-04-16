/**
 * Cliente HTTP con reintentos exponenciales, circuit breaker y presupuesto diario.
 *
 * Diseñado para scrapers que corren contra APIs públicas de cortes: defensivo
 * ante errores transitorios (429/5xx), respetuoso con los servidores (User-Agent
 * identificable, respeta Retry-After), y protegido contra bugs que podrían
 * disparar tráfico desbocado (budget diario persistido a disco).
 */

import fs from 'fs';
import path from 'path';

const USER_AGENT =
  process.env.SCRAPER_USER_AGENT || 'buscador-jurisprudencia-scraper/1.0';

const MAX_REQUESTS_PER_DAY =
  Number(process.env.SCRAPER_MAX_REQUESTS_PER_DAY || 2000);

const CIRCUIT_BREAKER_THRESHOLD = 10;                  // fallos consecutivos para abrir el breaker
const CIRCUIT_BREAKER_COOLDOWN_MS = 30 * 60 * 1000;    // 30 min bloqueado tras abrir
const MAX_BACKOFF_MS = 32_000;

const BUDGET_FILE = path.join(process.cwd(), 'scrapers', 'state', 'budget.json');

// ─── Estado interno ───────────────────────────────────────────────────────────

let consecutiveFailures = 0;
let circuitOpenUntil = 0;

// ─── Presupuesto diario (persistido a disco) ──────────────────────────────────

function today() {
  return new Date().toISOString().slice(0, 10);  // YYYY-MM-DD
}

function loadBudget() {
  try {
    const data = JSON.parse(fs.readFileSync(BUDGET_FILE, 'utf8'));
    // Reset automático al cambiar de día
    if (data.date !== today()) return { date: today(), requests: 0 };
    return data;
  } catch {
    return { date: today(), requests: 0 };
  }
}

function saveBudget(budget) {
  fs.mkdirSync(path.dirname(BUDGET_FILE), { recursive: true });
  const tmp = BUDGET_FILE + '.tmp';
  fs.writeFileSync(tmp, JSON.stringify(budget));
  fs.renameSync(tmp, BUDGET_FILE);
}

// ─── Helpers ──────────────────────────────────────────────────────────────────

function sleep(ms) {
  return new Promise(r => setTimeout(r, ms));
}

function backoffDelay(attempt, retryAfterHeader) {
  if (retryAfterHeader) {
    const sec = Number(retryAfterHeader);
    if (!Number.isNaN(sec)) return Math.min(sec * 1000, MAX_BACKOFF_MS);
  }
  return Math.min(Math.pow(2, attempt) * 1000, MAX_BACKOFF_MS);
}

// ─── API pública ──────────────────────────────────────────────────────────────

/**
 * Hace un fetch con reintentos exponenciales ante errores transitorios.
 * Cuenta contra el presupuesto diario cada intento.
 *
 * @param {string} url
 * @param {object} options   — igual que fetch()
 * @param {object} [opts]
 * @param {number} [opts.maxAttempts=5]
 * @param {number} [opts.timeoutMs=30000]
 * @returns {Promise<Response>}
 */
export async function fetchWithRetry(url, options = {}, opts = {}) {
  const { maxAttempts = 5, timeoutMs = 30_000 } = opts;

  // Circuit breaker
  if (Date.now() < circuitOpenUntil) {
    const waitMin = Math.ceil((circuitOpenUntil - Date.now()) / 60_000);
    throw new Error(
      `Circuit breaker abierto por ${consecutiveFailures} fallos consecutivos. ` +
      `Reintenta en ${waitMin} min.`
    );
  }

  // Budget diario
  const budget = loadBudget();
  if (budget.requests >= MAX_REQUESTS_PER_DAY) {
    throw new Error(
      `Presupuesto diario agotado (${budget.requests}/${MAX_REQUESTS_PER_DAY}). ` +
      `Reintenta mañana o ajusta SCRAPER_MAX_REQUESTS_PER_DAY.`
    );
  }

  const requestOptions = {
    ...options,
    headers: {
      'User-Agent': USER_AGENT,
      ...(options.headers || {})
    }
  };

  let lastError;

  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    // Timeout por intento
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), timeoutMs);

    try {
      const res = await fetch(url, { ...requestOptions, signal: controller.signal });
      clearTimeout(timer);

      // Toda request cuenta, incluso las fallidas (eso es lo que cargamos al servidor)
      budget.requests++;
      saveBudget(budget);

      // Éxito
      if (res.ok) {
        consecutiveFailures = 0;
        return res;
      }

      // 429/5xx → error transitorio, reintenta
      if (res.status === 429 || res.status >= 500) {
        if (attempt < maxAttempts) {
          const wait = backoffDelay(attempt, res.headers.get('Retry-After'));
          console.error(
            `[http] ${res.status} ${res.statusText} — esperando ${Math.round(wait/1000)}s ` +
            `(intento ${attempt}/${maxAttempts})`
          );
          await sleep(wait);
          continue;
        }
        lastError = new Error(`HTTP ${res.status} ${res.statusText} tras ${maxAttempts} intentos`);
        break;
      }

      // 4xx no transitorio → falla inmediata, no reintentes
      lastError = new Error(`HTTP ${res.status} ${res.statusText}`);
      break;
    } catch (e) {
      clearTimeout(timer);
      lastError = e;

      // AbortError (timeout) o error de red → reintenta
      if (attempt < maxAttempts) {
        const wait = backoffDelay(attempt);
        console.error(
          `[http] ${e.name}: ${e.message} — esperando ${Math.round(wait/1000)}s ` +
          `(intento ${attempt}/${maxAttempts})`
        );
        await sleep(wait);
        continue;
      }
    }
  }

  // Todos los intentos agotados → registra fallo consecutivo
  consecutiveFailures++;
  if (consecutiveFailures >= CIRCUIT_BREAKER_THRESHOLD) {
    circuitOpenUntil = Date.now() + CIRCUIT_BREAKER_COOLDOWN_MS;
    console.error(
      `[http] Circuit breaker ACTIVADO tras ${consecutiveFailures} fallos consecutivos. ` +
      `Bloqueado por ${CIRCUIT_BREAKER_COOLDOWN_MS / 60_000} min.`
    );
    consecutiveFailures = 0;
  }

  throw lastError || new Error('Fetch falló sin error específico');
}

/** Conveniencia: fetch que devuelve JSON ya parseado. */
export async function fetchJSON(url, options = {}, retryOpts = {}) {
  const res = await fetchWithRetry(url, options, retryOpts);
  return res.json();
}

/** Conveniencia: fetch que devuelve un Buffer. Útil para descargar PDFs. */
export async function fetchBuffer(url, options = {}, retryOpts = {}) {
  const res = await fetchWithRetry(url, options, retryOpts);
  return Buffer.from(await res.arrayBuffer());
}

/** Para tests / diagnóstico. */
export function getBudgetStatus() {
  const b = loadBudget();
  return { ...b, limit: MAX_REQUESTS_PER_DAY };
}
