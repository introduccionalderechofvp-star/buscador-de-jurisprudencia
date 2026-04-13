#!/usr/bin/env node
/**
 * Servidor MCP — Buscador de jurisprudencia colombiana
 *
 * Expone dos herramientas a Claude (u otros clientes MCP):
 *   1. buscar_sentencias   — búsqueda híbrida (vector + léxica) sin rerank
 *   2. obtener_texto_completo — texto OCR completo de una sentencia
 *
 * Diseño económico: el rerank con Claude se omite a propósito porque el
 * LLM consumidor (Claude Code/Desktop) ya va a leer y evaluar los resultados
 * por sí mismo. Pagar un segundo Claude para pre-rankear sería redundante.
 *
 * Uso: se conecta vía stdio al cliente MCP que lo invoque (Claude Desktop,
 * Claude Code, etc.). No expone ningún puerto de red.
 *
 * Configuración por variables de entorno:
 *   BUSCADOR_API_URL  — URL base del servidor del buscador
 *                       (default: http://localhost:3000)
 */

import { Server } from '@modelcontextprotocol/sdk/server/index.js';
import { StdioServerTransport } from '@modelcontextprotocol/sdk/server/stdio.js';
import {
  CallToolRequestSchema,
  ListToolsRequestSchema,
} from '@modelcontextprotocol/sdk/types.js';

// ─── Configuración ───────────────────────────────────────────────────────────

const API_URL = process.env.BUSCADOR_API_URL || 'http://localhost:3000';
const DEFAULT_LIMIT = 20;
const MAX_LIMIT = 50;

// ─── Helpers ─────────────────────────────────────────────────────────────────

async function callJSON(url, options = {}) {
  const r = await fetch(url, options);
  const ct = r.headers.get('content-type') || '';
  if (!ct.includes('application/json')) {
    const body = await r.text();
    throw new Error(`Respuesta no-JSON (HTTP ${r.status}): ${body.slice(0, 200)}`);
  }
  const data = await r.json();
  if (!r.ok || data.error) {
    throw new Error(data.error || `HTTP ${r.status}`);
  }
  return data;
}

function textResponse(obj) {
  return {
    content: [{ type: 'text', text: JSON.stringify(obj, null, 2) }],
  };
}

function errorResponse(message) {
  return {
    content: [{ type: 'text', text: `Error: ${message}` }],
    isError: true,
  };
}

// ─── Definición de herramientas ──────────────────────────────────────────────

const TOOLS = [
  {
    name: 'buscar_sentencias',
    description:
      'Busca sentencias de jurisprudencia colombiana usando búsqueda híbrida ' +
      '(vectorial semántica + léxica con keywords) sobre la colección indexada en Qdrant. ' +
      'Devuelve fragmentos relevantes de los documentos junto con metadatos (filename, ' +
      'organo, file_path) que se pueden usar después para obtener el texto completo. ' +
      'No usa rerank con Claude (modo económico): el costo por llamada es ~$0.0001. ' +
      'Si los resultados no son suficientemente relevantes, considera reformular la consulta ' +
      'con sinónimos jurídicos o términos técnicos equivalentes y volver a buscar.',
    inputSchema: {
      type: 'object',
      properties: {
        query: {
          type: 'string',
          description:
            'Consulta jurídica en lenguaje natural. Puede ser una pregunta, un concepto, ' +
            'o palabras clave. Ejemplos: "responsabilidad civil médica en partos", ' +
            '"requisitos del mandamiento ejecutivo de obligación de dar inmueble", ' +
            '"nulidad por error en el consentimiento".',
        },
        organo: {
          type: 'string',
          description:
            'Opcional. Filtra por órgano judicial (debe coincidir con el nombre exacto ' +
            'usado en la indexación, ej. "Sala Civil - Corte Suprema de Justicia" o ' +
            '"Tribunal Superior de Medellín"). Omitir para buscar en todos los órganos.',
        },
        limit: {
          type: 'number',
          description: `Cuántos fragmentos devolver. Default ${DEFAULT_LIMIT}, máximo ${MAX_LIMIT}.`,
          default: DEFAULT_LIMIT,
        },
      },
      required: ['query'],
    },
  },
  {
    name: 'obtener_texto_completo',
    description:
      'Devuelve el texto OCR completo de un PDF de sentencia, no truncado. ' +
      'Útil para leer una sentencia entera después de identificarla con buscar_sentencias. ' +
      'No tiene costo en APIs externas (lectura local del archivo). ' +
      'IMPORTANTE: las sentencias largas pueden tener decenas de miles de tokens; ' +
      'úsalo selectivamente sobre los documentos más relevantes, no sobre toda la lista ' +
      'de resultados de una búsqueda.',
    inputSchema: {
      type: 'object',
      properties: {
        file_path: {
          type: 'string',
          description:
            'Ruta relativa del archivo dentro de uploads/, tal como aparece en el campo ' +
            '"file_path" de los resultados de buscar_sentencias. Ejemplo: ' +
            '"Sala Civil - Corte Suprema de Justicia/SC10189-2016 [2007-00105-01].pdf".',
        },
      },
      required: ['file_path'],
    },
  },
];

// ─── Implementación de herramientas ──────────────────────────────────────────

async function handleBuscarSentencias(args) {
  if (!args.query || typeof args.query !== 'string') {
    return errorResponse('Falta el parámetro "query" o no es un string.');
  }

  const limit = Math.min(Math.max(Number(args.limit) || DEFAULT_LIMIT, 1), MAX_LIMIT);

  let data;
  try {
    data = await callJSON(`${API_URL}/api/search`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        query: args.query,
        organo: args.organo || undefined,
        limit,
        advanced: false, // sin expansión Claude (modo económico)
        rerank: false,   // sin rerank Claude (modo económico)
      }),
    });
  } catch (e) {
    return errorResponse(`Falló la búsqueda en el servidor: ${e.message}`);
  }

  const results = (data.results || []).map((r, i) => ({
    rank: i + 1,
    filename: r.filename,
    organo: r.organo,
    file_path: r.file_path,
    score: typeof r.score === 'number' ? Number(r.score.toFixed(4)) : null,
    text_excerpt: r.text || '',
  }));

  return textResponse({
    query: args.query,
    organo_filter: args.organo || null,
    total_results: results.length,
    note:
      'Los text_excerpt son fragmentos truncados (≤1200 chars). Para leer el texto ' +
      'completo de una sentencia, usa la herramienta obtener_texto_completo con su file_path.',
    results,
  });
}

async function handleObtenerTextoCompleto(args) {
  if (!args.file_path || typeof args.file_path !== 'string') {
    return errorResponse('Falta el parámetro "file_path" o no es un string.');
  }

  let data;
  try {
    data = await callJSON(
      `${API_URL}/api/document/text?path=${encodeURIComponent(args.file_path)}`
    );
  } catch (e) {
    return errorResponse(`No se pudo obtener el texto completo: ${e.message}`);
  }

  return textResponse({
    filename: data.filename,
    file_path: data.file_path,
    organo: data.organo,
    num_pages: data.num_pages,
    num_chars: data.num_chars,
    full_text: data.full_text,
  });
}

// ─── Bootstrap del servidor MCP ──────────────────────────────────────────────

const server = new Server(
  {
    name: 'buscador-jurisprudencia',
    version: '1.0.0',
  },
  {
    capabilities: { tools: {} },
  }
);

server.setRequestHandler(ListToolsRequestSchema, async () => ({
  tools: TOOLS,
}));

server.setRequestHandler(CallToolRequestSchema, async (request) => {
  const { name, arguments: args = {} } = request.params;
  switch (name) {
    case 'buscar_sentencias':
      return handleBuscarSentencias(args);
    case 'obtener_texto_completo':
      return handleObtenerTextoCompleto(args);
    default:
      return errorResponse(`Herramienta desconocida: ${name}`);
  }
});

const transport = new StdioServerTransport();
await server.connect(transport);

// Log a stderr (no a stdout, que es el canal del protocolo MCP)
console.error(`[MCP] Buscador de jurisprudencia conectado · API: ${API_URL}`);
