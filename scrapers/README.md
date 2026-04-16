# Scrapers de jurisprudencia

Módulo responsable de descargar automáticamente providencias de las cortes
colombianas al `uploads/` del buscador. Una vez descargadas, los scripts
existentes (`scripts/ocr-saltados.js` + `scripts/ingest-bulk.js`) las procesan
e indexan en Qdrant.

## Arquitectura

```
                  ┌──────────────────────────┐
                  │  scrapers/<corporación>  │
                  │     (un archivo por      │
                  │      cada corte)         │
                  └────────────┬─────────────┘
                               │
                               ▼
                  ┌──────────────────────────┐
                  │    scrapers/lib/*        │
                  │  (utilidades compartidas │
                  │   entre todos los        │
                  │   scrapers)              │
                  └────────────┬─────────────┘
                               │ escribe PDFs a
                               ▼
             uploads/<Órgano>/<Año>/<archivo>.pdf
                               │
                               │ (después se dispara)
                               ▼
             scripts/ocr-saltados.js → scripts/ingest-bulk.js
                               │
                               ▼
                         Qdrant indexa
                               │
                               ▼
                 buscables vía buscador web + MCP
```

## Módulos compartidos (`lib/`)

### `lib/http.js` — cliente HTTP defensivo
- Retry exponencial con backoff (2s → 4s → 8s → 16s → 32s) para 429/5xx
- Respeta el header `Retry-After` cuando la API lo envía
- Circuit breaker: tras 10 fallos consecutivos, se bloquea 30 min
- Presupuesto diario (`SCRAPER_MAX_REQUESTS_PER_DAY`, default 2000) persistido
  en `scrapers/state/budget.json`, se resetea automáticamente al cambiar de día
- User-Agent identificable (`SCRAPER_USER_AGENT`, default
  `buscador-jurisprudencia-scraper/1.0`)
- Timeout por intento (30s default)

### `lib/storage.js` — escritura atómica a `uploads/`
- `saveAtomic(organo, año, filename, buffer)`: escribe a `<archivo>.tmp` y
  hace rename al nombre final. Un archivo con nombre final **siempre** implica
  descarga completa
- `exists(organo, año, filename)`: dedup por existencia en disco
- `cleanupTmp(organo)`: limpia `.tmp` huérfanos de crashes anteriores
- Sanitización de nombres (remueve `\ / : * ? " < > |`)

### `lib/state.js` — estado persistente + locks
- `readState(name)` / `writeState(name, state)`: estado JSON por scraper,
  writes atómicos (temp + rename). Usado para checkpoints de paginación
- `acquireLock(name)`: intenta crear un lock file con el PID. Detecta locks
  huérfanos (PID muerto) y los limpia automáticamente. Devuelve
  `{acquired: true}` o `{acquired: false, heldByPid}`
- `releaseLock(name)`: llamar en `finally` para asegurar limpieza

### `lib/errors.js` — log estructurado JSONL
- `logError(scraper, details)`: agrega una línea JSON a
  `scrapers/errors/<fecha>.jsonl`. Permite post-mortem y retry dirigido
- `readErrors(date)`: parsea todas las líneas de una fecha

## Variables de entorno reconocidas

| Variable | Default | Propósito |
|---|---|---|
| `SCRAPER_USER_AGENT` | `buscador-jurisprudencia-scraper/1.0` | Header `User-Agent` en todas las requests |
| `SCRAPER_MAX_REQUESTS_PER_DAY` | `2000` | Tope diario de requests (budget) |

Ninguna de estas es obligatoria. El scraper funciona con defaults.

## Estructura de carpetas

```
scrapers/
├── lib/                    ← utilidades compartidas (este módulo)
│   ├── http.js
│   ├── storage.js
│   ├── state.js
│   └── errors.js
├── state/                  ← estado persistente (creado en runtime)
│   ├── budget.json         ← contador de requests del día
│   ├── <scraper>.json      ← estado por corporación/sala
│   └── <scraper>.lock      ← lock file (solo mientras corre)
├── errors/                 ← logs de errores (creado en runtime)
│   └── YYYY-MM-DD.jsonl    ← un archivo por día
├── <corporación>.js        ← un scraper por corte (próximos commits)
└── README.md               ← este archivo
```

Las carpetas `state/` y `errors/` se crean automáticamente la primera vez que
se usan. No es necesario crearlas manualmente.

## Patrón típico de un scraper

```js
import { fetchJSON, fetchBuffer } from './lib/http.js';
import { exists, saveAtomic } from './lib/storage.js';
import { readState, writeState, acquireLock, releaseLock } from './lib/state.js';
import { logError } from './lib/errors.js';

const lock = acquireLock('mi-scraper');
if (!lock.acquired) {
  console.error(`Otra instancia corriendo (PID ${lock.heldByPid})`);
  process.exit(1);
}

try {
  // ... scrape ...
  for (const doc of docs) {
    if (exists(organo, año, doc.filename)) continue;
    try {
      const buffer = await fetchBuffer(doc.url);
      saveAtomic(organo, año, doc.filename, buffer);
    } catch (e) {
      logError('mi-scraper', { doc: doc.url, error: e.message });
    }
  }
  writeState('mi-scraper', { last_run: new Date().toISOString() });
} finally {
  releaseLock('mi-scraper');
}
```

## Scrapers disponibles

_Ninguno todavía. El primero (Corte Suprema de Justicia) viene en el próximo
commit._
