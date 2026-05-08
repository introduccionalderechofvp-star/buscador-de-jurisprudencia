#!/usr/bin/env node
/**
 * tribunal-medellin-civil.js
 *
 * Scraper de Sala Civil del Tribunal Superior de Medellín, contra el portal
 * "Publicaciones Procesales" de la Rama Judicial:
 *
 *   https://publicacionesprocesales.ramajudicial.gov.co/web/publicaciones-procesales/
 *
 * Este portal lista publicaciones de cualquier despacho judicial colombiano,
 * filtrable por idDespacho. Para Sala Civil del Tribunal Sup. de Medellín:
 *   idDespacho = 050012203000
 *
 * Para cada "publicación" (Notificación por Estado), el portal expone:
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
import fs from 'node:fs';

import { fetchWithRetry, fetchBuffer } from './lib/http.js';
import { readState, writeState, acquireLock, releaseLock } from './lib/state.js';
import { safeName, buildBasenameIndex, saveAtomic } from './lib/storage.js';

const ORGANO       = 'Sala Civil - Tribunal Superior de Medellín';
const STATE_NAME   = 'tribunal-medellin-civil';
const ID_DESPACHO  = '050012203000';
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
    headers: {
      'Accept-Language': 'es-CO,es;q=0.9,en;q=0.8'
    }
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
  sp.set(`${PORTLET_PREFIX}action`, 'busqueda');
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

function getTotalPages(html) {
  const m = html.match(/P[áa]gina\s+\d+\s+de\s+(\d+)/i);
  return m ? Number(m[1]) : 1;
}

function decodeEntities(s) {
  return s
    .replace(/&amp;/g, '&').replace(/&lt;/g, '<').replace(/&gt;/g, '>')
    .replace(/&quot;/g, '"').replace(/&#39;/g, "'").replace(/&nbsp;/g, ' ')
    .replace(/&aacute;/g, 'á').replace(/&eacute;/g, 'é').replace(/&iacute;/g, 'í')
    .replace(/&oacute;/g, 'ó').replace(/&uacute;/g, 'ú').replace(/&ntilde;/g, 'ñ')
    .replace(/&Aacute;/g, 'Á').replace(/&Eacute;/g, 'É').replace(/&Iacute;/g, 'Í')
    .replace(/&Oacute;/g, 'Ó').replace(/&Uacute;/g, 'Ú').replace(/&Ntilde;/g, 'Ñ');
}

function plainText(html) {
  return decodeEntities(html.replace(/<[^>]+>/g, ' ').replace(/\s+/g, ' ').trim());
}

function extractAnchors(html) {
  const out = [];
  const rx = /<a\b[^>]*href\s*=\s*(?:"([^"]+)"|'([^']+)')[^>]*>([\s\S]*?)<\/a>/gi;
  let m;
  while ((m = rx.exec(html))) {
    const href = decodeEntities(m[1] || m[2]);
    const text = plainText(m[3] || '');
    out.push({ href, text });
  }
  return out;
}

function getDetailLinks(html, baseUrl) {
  const seen = new Set();
  const links = [];
  for (const a of extractAnchors(html)) {
    let abs;
    try { abs = new URL(a.href, baseUrl).href; } catch { continue; }
    if (/detail\.jsp|articleId=/.test(abs) && !seen.has(abs)) {
      seen.add(abs);
      links.push(abs);
    }
  }
  return links;
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

function extractDetailMeta(html) {
  const text = plainText(html);
  let title = 'Publicación';
  const mTitle = text.match(/Notificaci[oó]n\s+por\s+Estado\s+No\.?\s*\d+/i);
  if (mTitle) title = mTitle[0].trim();

  let publicationDate = null;
  const mDate1 = text.match(/Fecha\s+de\s+Publicaci[oó]n:\s*(\d{4}-\d{2}-\d{2})/i);
  const mDate2 = !mDate1 ? text.match(/\b(\d{4}-\d{2}-\d{2})\b/) : null;
  if (mDate1) publicationDate = mDate1[1];
  else if (mDate2) publicationDate = mDate2[1];

  return { title, publicationDate };
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
    let skippedNoDate = 0;
    let errors = 0;
    let detailsSeen = new Set();
    let maxPubDate = prevState?.last_publication_date || fromDate;
    const t0 = Date.now();

    const firstUrl = buildSearchUrl({ page: 1, fromDate, toDate });
    const firstHtml = await fetchText(firstUrl);
    const totalPages = getTotalPages(firstHtml);
    console.log(`  Páginas detectadas: ${totalPages}`);

    for (let page = 1; page <= totalPages && downloaded < max; page++) {
      console.log(`\n  Página ${page}/${totalPages}...`);
      const pageUrl = buildSearchUrl({ page, fromDate, toDate });
      const pageHtml = page === 1 ? firstHtml : await fetchText(pageUrl);
      const detailUrls = getDetailLinks(pageHtml, pageUrl);
      if (!detailUrls.length) {
        console.log(`    (sin detalles)`);
        continue;
      }

      for (const detailUrl of detailUrls) {
        if (downloaded >= max) break;
        if (detailsSeen.has(detailUrl)) continue;
        detailsSeen.add(detailUrl);

        let meta;
        let detailHtml;
        try {
          detailHtml = await fetchText(detailUrl);
          meta = extractDetailMeta(detailHtml);
        } catch (e) {
          errors++;
          console.log(`    [err detalle] ${e.message.slice(0, 80)}`);
          continue;
        }

        if (!meta.publicationDate) skippedNoDate++;
        const pubDate = meta.publicationDate || '';
        if (pubDate && pubDate > maxPubDate) maxPubDate = pubDate;

        const pdfs = getPdfLinks(detailHtml, detailUrl);
        if (!pdfs.length) continue;

        const año = pubDate ? pubDate.slice(0, 4) : String(new Date().getFullYear());

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
            const fullPath = saveAtomic(ORGANO, año, filename, buffer);
            onDisk.add(key);
            downloaded++;
            const kb = (buffer.length / 1024).toFixed(1);
            console.log(`    [ok] ${año}/${filename} (${kb} KB)`);
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
    console.log(`  Descargados:           ${downloaded}`);
    console.log(`  Saltados (existían):   ${skippedExist}`);
    console.log(`  Saltados (estados):    ${skippedState}`);
    console.log(`  Saltados (sin fecha):  ${skippedNoDate}`);
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
