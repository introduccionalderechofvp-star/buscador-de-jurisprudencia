#!/usr/bin/env node
/**
 * tribunal-medellin-civil.js
 *
 * Scraper de Sala Civil del Tribunal Superior de Medellín, contra el portal
 * "Publicaciones Procesales" de la Rama Judicial:
 *
 *   https://publicacionesprocesales.ramajudicial.gov.co/web/publicaciones-procesales/
 *
 * Despacho: 050012203000 ("Sala Civil del Tribunal Superior de Medellín")
 * Estructura: 6098957 ("Notificaciones por Estado")
 *
 * El listado de "Notificaciones por Estado" no se publica como HTML semántico
 * sino como un blob JS de Liferay con objetos PublicacionesVO. Cada VO tiene
 * articleId + fechaRadicado + title. El parser extrae esos campos y construye
 * la URL del detalle.
 *
 * Para cada Notificación, el detalle expone:
 *   - El PDF del estado en sí (lista de notificaciones del día) -> SE OMITE
 *   - Las providencias (autos/sentencias) asociadas en PDF       -> SE DESCARGAN
 *
 * El portal solo expone publicaciones desde mayo 2024. Para historico hay un
 * portal separado que ya se cargó manualmente (vive bajo Historico/).
 *
 * Uso:
 *   node scrapers/tribunal-medellin-civil.js                    # incremental
 *   node scrapers/tribunal-medellin-civil.js --from 2026-04-01  # desde fecha
 *   node scrapers/tribunal-medellin-civil.js --max 5            # tope para test
 *   node scrapers/tribunal-medellin-civil.js --to 2026-05-01    # hasta fecha
 *
 * Modo incremental:
 *   - Lee `last_publication_date` del state. Si existe, lo usa como fechaInicio.
 *   - Si no existe, requiere --from explícito.
 *   - Dedupe por basename del archivo (no re-descarga lo que ya está en disco).
 */

import 'dotenv/config';
import { parseArgs } from 'node:util';
import path from 'node:path';

import { fetchWithRetry, fetchBuffer } from './lib/http.js';
import { readState, writeState, acquireLock, releaseLock } from './lib/state.js';
import { safeName, buildBasenameIndex, saveAtomic } from './lib/storage.js';

const ORGANO       = 'Sala Civil - Tribunal Superior de Medellín';
const STATE_NAME   = 'tribunal-medellin-civil';
const ID_DESPACHO  = '050012203000';
const ID_STRUCTURE = '6098957';   // "Notificaciones por Estado" en este portal
const PORTAL_BASE  = 'https://publicacionesprocesales.ramajudicial.gov.co';
const PORTLET_INSTANCE =
  'co_com_avanti_efectosProcesales_PublicacionesEfectosProcesalesPortletV2_INSTANCE_BIyXQFHVaYaq';
const PORTLET_PREFIX = `_${PORTLET_INSTANCE}_`;

const PAGE_SIZE = 75;
const REQUEST_DELAY_MS = 500;   // throttle entre detalles
const STATE_PDF_PATTERN = /estado[s]?\b/i;

// ─── Utilidades ───────────────────────────────────────────────────────────────

const sleep = ms => new Promise(r => setTimeout(r, ms));

async function fetchText(url) {
  const res = await fetchWithRetry(url, {
    headers: { 'Accept-Language': 'es-CO,es;q=0.9,en;q=0.8' }
  }, { timeoutMs: 60_000 });
  return res.text();
}

function buildSearchUrl({ page = 1, fromDate = '', toDate = '' }) {
  const url = new URL(`${PORTAL_BASE}/web/publicaciones-procesales/inicio`);
  const sp = url.searchParams;
  sp.set('p_p_id', PORTLET_INSTANCE);
  sp.set('p_p_lifecycle', '0');
  sp.set('p_p_state', 'normal');
  sp.set('p_p_mode', 'view');
  // filterStructures + idStructure restringe a "Notificaciones por Estado".
  // Sin esto el portal mezcla otras estructuras y el conteo es inconsistente.
  sp.set(`${PORTLET_PREFIX}action`, 'filterStructures');
  sp.set(`${PORTLET_PREFIX}idStructure`, ID_STRUCTURE);
  sp.set(`${PORTLET_PREFIX}verTotales`, 'true');
  sp.set(`${PORTLET_PREFIX}cur`, String(page));
  sp.set(`${PORTLET_PREFIX}delta`, String(PAGE_SIZE));
  sp.set(`${PORTLET_PREFIX}idDespacho`, ID_DESPACHO);
  sp.set(`${PORTLET_PREFIX}idDepto`, '');
  sp.set(`${PORTLET_PREFIX}idEntidad`, '');
  sp.set(`${PORTLET_PREFIX}idEspecialidad`, '');
  sp.set(`${PORTLET_PREFIX}idMuni`, '');
  sp.set(`${PORTLET_PREFIX}fechaInicio`, fromDate);
  sp.set(`${PORTLET_PREFIX}fechaFin`, toDate);
  sp.set(`${PORTLET_PREFIX}resetCur`, 'false');
  return url.href;
}

function buildDetailUrl(articleId) {
  const url = new URL(`${PORTAL_BASE}/web/publicaciones-procesales/inicio`);
  const sp = url.searchParams;
  sp.set('p_p_id', PORTLET_INSTANCE);
  sp.set('p_p_lifecycle', '0');
  sp.set('p_p_state', 'normal');
  sp.set('p_p_mode', 'view');
  sp.set(`${PORTLET_PREFIX}jspPage`, '/META-INF/resources/detail.jsp');
  sp.set(`${PORTLET_PREFIX}articleId`, articleId);
  return url.href;
}

function decodeJsEscapes(s) {
  // Liferay serializa los VO con escapes JS: \x3d, ó, etc.
  return String(s)
    .replace(/\\x([0-9a-fA-F]{2})/g, (_, h) => String.fromCharCode(parseInt(h, 16)))
    .replace(/\\u([0-9a-fA-F]{4})/g, (_, h) => String.fromCharCode(parseInt(h, 16)));
}

function decodeEntities(s) {
  return s
    .replace(/&amp;/g, '&').replace(/&lt;/g, '<').replace(/&gt;/g, '>')
    .replace(/&quot;/g, '"').replace(/&#39;/g, "'").replace(/&nbsp;/g, ' ');
}

/**
 * Extrae las entradas PublicacionesVO del blob JS del listado.
 *
 * Cada entrada tiene formato:
 *   PublicacionesVO [title=..., summary=..., radicado=null, fechaRadicado=YYYY-MM-DD,
 *                    entry=null, articleId=NNN, codDepto=..., ...]
 *
 * Los campos están separados por ", " pero los valores también pueden tener comas
 * (ej: en title), así que extraemos por nombre de campo conocido.
 */
function parsePublicacionesVO(html) {
  const entries = [];
  const blockRx = /PublicacionesVO\s*\\?\[([\s\S]*?)\\?\]/g;
  let m;
  while ((m = blockRx.exec(html))) {
    const body = decodeJsEscapes(m[1]);
    const articleId = body.match(/articleId\s*=\s*(\d+)/)?.[1];
    if (!articleId) continue;
    const fechaRadicado = body.match(/fechaRadicado\s*=\s*(\d{4}-\d{2}-\d{2})/)?.[1] || '';
    // title termina con ", summary=" — capturamos hasta ahí
    const titleMatch = body.match(/title\s*=\s*([\s\S]*?),\s*summary\s*=/);
    const title = titleMatch ? titleMatch[1].trim() : '';
    entries.push({ articleId, fechaRadicado, title });
  }
  return entries;
}

function extractAnchors(html) {
  const out = [];
  const rx = /<a\b[^>]*href\s*=\s*(?:"([^"]+)"|'([^']+)')[^>]*>([\s\S]*?)<\/a>/gi;
  let m;
  while ((m = rx.exec(html))) {
    const href = decodeEntities(m[1] || m[2]);
    const text = decodeEntities((m[3] || '').replace(/<[^>]+>/g, ' ').replace(/\s+/g, ' ').trim());
    out.push({ href, text });
  }
  return out;
}

function getPdfLinks(html, baseUrl) {
  const seen = new Set();
  const links = [];
  for (const a of extractAnchors(html)) {
    let abs;
    try { abs = new URL(a.href, baseUrl).href; } catch { continue; }
    const lower = abs.toLowerCase();
    if ((lower.includes('document_library/get_file') || lower.includes('.pdf')) && !seen.has(abs)) {
      seen.add(abs);
      const name = a.text || path.basename(new URL(abs).pathname) || 'archivo.pdf';
      links.push({ name, url: abs });
    }
  }
  return links;
}

function getTotalPages(html) {
  const m = html.match(/P[áa]gina\s+\d+\s+de\s+(\d+)/i);
  return m ? Number(m[1]) : 1;
}

function isStatePdf(name) {
  return STATE_PDF_PATTERN.test(name);
}

function basenameWithoutExt(filename) {
  return filename.replace(/\.(pdf|docx|doc|md|txt)$/i, '').trim();
}

// ─── Main ─────────────────────────────────────────────────────────────────────

async function main() {
  const { values } = parseArgs({
    options: {
      from:  { type: 'string', default: '' },
      to:    { type: 'string', default: '' },
      max:   { type: 'string', default: '999999' },
      force: { type: 'boolean', default: false }
    }
  });
  const max = Number(values.max);
  let fromDate = values.from;
  const toDate = values.to;

  console.log('═══════════════════════════════════════════════════════════════');
  console.log('  Scraper Tribunal Sup. Medellín — Sala Civil');
  console.log('═══════════════════════════════════════════════════════════════');

  const prevState = readState(STATE_NAME);
  if (!fromDate) {
    if (prevState?.last_publication_date) {
      fromDate = prevState.last_publication_date;
      console.log(`  Modo incremental: desde ${fromDate} (última corrida)`);
    } else {
      console.error('  ERROR: primera corrida requiere --from YYYY-MM-DD.');
      console.error('  Ejemplo: --from 2026-04-01');
      process.exit(1);
    }
  } else {
    console.log(`  Desde: ${fromDate}`);
  }
  if (toDate) console.log(`  Hasta: ${toDate}`);
  console.log(`  Despacho:    ${ID_DESPACHO} (${ORGANO})`);
  console.log(`  Estructura:  ${ID_STRUCTURE} (Notificaciones por Estado)`);
  console.log(`  Max archivos: ${max}`);
  console.log('');

  const lock = acquireLock(STATE_NAME);
  if (lock.heldByPid) {
    console.error(`  Ya hay otra instancia (PID ${lock.heldByPid}). Salgo.`);
    process.exit(0);
  }

  try {
    const onDisk = buildBasenameIndex(ORGANO);
    console.log(`  Basenames ya en disco: ${onDisk.size}\n`);

    let downloaded = 0;
    let skippedExist = 0;
    let skippedState = 0;
    let skippedNoPdf = 0;
    let errors = 0;
    let entriesSeen = 0;
    let maxPubDate = prevState?.last_publication_date || fromDate;
    const t0 = Date.now();

    const firstUrl = buildSearchUrl({ page: 1, fromDate, toDate });
    const firstHtml = await fetchText(firstUrl);
    const totalPages = getTotalPages(firstHtml);
    console.log(`  Páginas detectadas: ${totalPages}`);

    for (let page = 1; page <= totalPages && downloaded < max; page++) {
      const pageHtml = page === 1
        ? firstHtml
        : await fetchText(buildSearchUrl({ page, fromDate, toDate }));

      const entries = parsePublicacionesVO(pageHtml);
      console.log(`\n  Página ${page}/${totalPages} — ${entries.length} entradas`);
      if (!entries.length) continue;

      for (const entry of entries) {
        if (downloaded >= max) break;
        entriesSeen++;
        const { articleId, fechaRadicado, title } = entry;
        if (fechaRadicado && fechaRadicado > maxPubDate) maxPubDate = fechaRadicado;
        const año = fechaRadicado ? fechaRadicado.slice(0, 4) : String(new Date().getFullYear());

        const detailUrl = buildDetailUrl(articleId);
        let detailHtml;
        try {
          detailHtml = await fetchText(detailUrl);
        } catch (e) {
          errors++;
          console.log(`    [err detalle ${articleId}] ${e.message.slice(0, 80)}`);
          continue;
        }

        const pdfs = getPdfLinks(detailHtml, detailUrl);
        if (!pdfs.length) {
          skippedNoPdf++;
          continue;
        }

        for (const pdf of pdfs) {
          if (downloaded >= max) break;
          if (isStatePdf(pdf.name)) { skippedState++; continue; }

          const filename = safeName(pdf.name.endsWith('.pdf') ? pdf.name : `${pdf.name}.pdf`);
          const key = basenameWithoutExt(filename).toLowerCase();
          if (onDisk.has(key)) { skippedExist++; continue; }

          try {
            const buffer = await fetchBuffer(pdf.url, {
              headers: { 'Accept-Language': 'es-CO,es;q=0.9,en;q=0.8' }
            }, { timeoutMs: 60_000 });
            saveAtomic(ORGANO, año, filename, buffer);
            onDisk.add(key);
            downloaded++;
            const kb = (buffer.length / 1024).toFixed(1);
            console.log(`    [ok] ${año}/${filename} (${kb} KB) — ${fechaRadicado || 'sin fecha'}`);
          } catch (e) {
            errors++;
            console.log(`    [err] ${filename}: ${e.message.slice(0, 80)}`);
          }
          await sleep(REQUEST_DELAY_MS);
        }
      }
    }

    const elapsedMin = ((Date.now() - t0) / 1000 / 60).toFixed(1);
    console.log(`\n═══════════════════════════════════════════════════════════════`);
    console.log(`  RESUMEN — Sala Civil Tribunal Medellín`);
    console.log(`═══════════════════════════════════════════════════════════════`);
    console.log(`  Entradas procesadas:   ${entriesSeen}`);
    console.log(`  Descargados:           ${downloaded}`);
    console.log(`  Saltados (existían):   ${skippedExist}`);
    console.log(`  Saltados (estados):    ${skippedState}`);
    console.log(`  Saltados (sin PDFs):   ${skippedNoPdf}`);
    console.log(`  Errores:               ${errors}`);
    console.log(`  Última fecha vista:    ${maxPubDate}`);
    console.log(`  Tiempo:                ${elapsedMin} min`);

    writeState(STATE_NAME, {
      ...prevState,
      ultima_corrida_exitosa: new Date().toISOString(),
      ultima_corrida_descargados: downloaded,
      ultima_corrida_saltados: skippedExist,
      ultima_corrida_errores: errors,
      from_used: fromDate,
      last_publication_date: maxPubDate
    });
  } finally {
    releaseLock(STATE_NAME);
  }
}

main().catch(e => {
  console.error('FALLO FATAL:', e);
  process.exit(1);
});
