#!/usr/bin/env node
/**
 * audit-csj.js
 *
 * Recorre la API de la CSJ para una sala dada, lista TODOS los basenames que
 * existen en la API, y los compara contra los archivos en disco.
 *
 * NO DESCARGA NADA. Solo enumera y reporta el gap.
 *
 * Uso:
 *   node scripts/audit-csj.js Civil
 *   node scripts/audit-csj.js Laboral --query "tutela"
 *   node scripts/audit-csj.js Penal --max-pages 100
 *
 * Output:
 *   - Resumen en stdout (totales API/disco/missing)
 *   - Lista completa de basenames missing en /tmp/audit-csj-<sala>-missing.txt
 *
 * Acepta las mismas extensiones que el scraper (.pdf/.docx/.doc) más .md/.txt
 * para considerar un basename "ya en disco" (consistente con buildBasenameIndex).
 */

import { buildBasenameIndex } from '../scrapers/lib/storage.js';
import path from 'node:path';
import fs from 'node:fs';

const API_SEARCH = 'https://consultajurisprudencial.ramajudicial.gov.co:443/WebRelatoria/services/search/';
const PAGE_SIZE  = 10;

const args = process.argv.slice(2);
if (args.length < 1) {
  console.error('Uso: node scripts/audit-csj.js <sala> [--query "x"] [--max-pages N]');
  console.error('Salas válidas: Civil, Laboral, Penal, Tutelas');
  process.exit(1);
}

const sala = args[0];
const VALID_SALAS = ['Civil', 'Laboral', 'Penal', 'Tutelas'];
if (!VALID_SALAS.includes(sala)) {
  console.error(`Sala "${sala}" no válida. Opciones: ${VALID_SALAS.join(', ')}`);
  process.exit(1);
}

let query = 'a';
let maxPages = Infinity;
for (let i = 1; i < args.length; i++) {
  if (args[i] === '--query') { query = args[++i]; }
  else if (args[i] === '--max-pages') { maxPages = Number(args[++i]); }
}

const SUPPORTED_EXTS = new Set(['pdf', 'docx', 'doc']);

function buildQuery({ query, sala, start }) {
  const esc = s => String(s ?? '').replace(/"/g, '\\"');
  return `{
  getSearchResult(searchQuery:{
    query: "${esc(query)}"
    typeOfQuery: "${esc(sala)}"
    start: ${Number(start)}
    isExact: false
    magistrate: ""
    year: ""
    autoSentencia: "SENTENCIA"
    order: "NEW_FIRST"
    roomTutelas: ""
    addedQueries: []
  })
  {
    searchResults { title onlinePath doctor ano }
    numOfResults
  }
}`;
}

async function searchPage({ start }) {
  const res = await fetch(API_SEARCH, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ query: buildQuery({ query, sala, start }) })
  });
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  const data = await res.json();
  return data?.data?.getSearchResult;
}

function extractExt(onlinePath) {
  const m = String(onlinePath || '').toLowerCase().match(/\.([a-z0-9]+)$/);
  return m ? m[1] : null;
}

function extractBasename(doc) {
  // El title viene ya con el basename del archivo. Lo limpiamos a lowercase
  // y le quitamos extensiones para comparar con disk.
  const raw = doc.title || path.basename(doc.onlinePath || '');
  return raw.replace(/\.(pdf|docx|doc|md|txt)$/i, '').trim();
}

async function main() {
  console.log(`\nAuditoría — Sala ${sala}`);
  console.log(`Query: "${query}", max-pages: ${maxPages === Infinity ? 'sin límite' : maxPages}\n`);

  const organo = `Sala ${sala} - Corte Suprema de Justicia`;
  const onDisk = buildBasenameIndex(organo);
  console.log(`En disco (${organo}): ${onDisk.size} basenames únicos`);

  const apiBasenames = new Set();
  const apiUnsupported = new Set();
  let totalSeen = 0;
  let totalApi  = null;
  let start = 0;
  let page = 0;

  while (page < maxPages) {
    let result;
    try {
      result = await searchPage({ start });
    } catch (e) {
      console.error(`Error en página ${page+1}: ${e.message}`);
      break;
    }

    if (!result || !Array.isArray(result.searchResults)) break;
    if (totalApi === null) {
      totalApi = result.numOfResults;
      console.log(`Total API según numOfResults: ${totalApi}`);
    }

    if (result.searchResults.length === 0) break;
    page++;
    totalSeen += result.searchResults.length;

    for (const doc of result.searchResults) {
      const ext = extractExt(doc.onlinePath);
      const base = extractBasename(doc);
      if (!base) continue;

      if (!ext || !SUPPORTED_EXTS.has(ext)) {
        apiUnsupported.add(base.toLowerCase());
        continue;
      }
      apiBasenames.add(base.toLowerCase());
    }

    if (page % 20 === 0) {
      console.log(`  Página ${page} (${totalSeen} vistas) — ${apiBasenames.size} basenames únicos en API`);
    }

    start += result.searchResults.length;
    if (totalApi !== null && start >= totalApi) break;
    await new Promise(r => setTimeout(r, 80));   // throttle suave
  }

  console.log(`\n─── RESULTADOS ─────────────────────────────────────────`);
  console.log(`Páginas leídas:                 ${page}`);
  console.log(`Resultados crudos vistos:       ${totalSeen}`);
  console.log(`Basenames únicos en API:        ${apiBasenames.size}`);
  console.log(`Basenames con ext no soportada: ${apiUnsupported.size}`);
  console.log(`Basenames únicos en disco:      ${onDisk.size}`);

  // Comparación
  const missingFromDisk = [];
  for (const b of apiBasenames) {
    if (!onDisk.has(b)) missingFromDisk.push(b);
  }
  const onlyInDisk = [];
  for (const b of onDisk) {
    if (!apiBasenames.has(b)) onlyInDisk.push(b);
  }

  console.log(`\nFaltan en disco (en API pero no descargadas): ${missingFromDisk.length}`);
  console.log(`Solo en disco (no aparecen en API actual):    ${onlyInDisk.length}`);

  // Guardar listas
  const missingPath = `/tmp/audit-csj-${sala.toLowerCase()}-missing.txt`;
  fs.writeFileSync(missingPath, missingFromDisk.sort().join('\n'));
  console.log(`\nLista de missing: ${missingPath}`);

  if (missingFromDisk.length > 0) {
    console.log(`\nMuestra de las primeras 20:`);
    for (const b of missingFromDisk.slice(0, 20)) {
      console.log(`  - ${b}`);
    }
  }
}

main().catch(e => {
  console.error('FALLO:', e);
  process.exit(1);
});
