# MCP — Buscador de jurisprudencia

Servidor MCP (Model Context Protocol) que conecta clientes LLM como
**Claude Desktop** y **Claude Code** con tu buscador semántico de
jurisprudencia colombiana.

Una vez instalado, podrás pedirle a Claude cosas como:

> *"Encuéntrame las 5 sentencias más relevantes de la Corte Suprema sobre
> responsabilidad médica en partos en los últimos 5 años, lee el texto completo
> de las 3 más relevantes, y construye una tabla comparativa de los criterios
> que cada una usa para resolver."*

Y Claude lo hace automáticamente, llamando a tu buscador y leyendo los PDFs.

---

## Herramientas que expone

| Herramienta | Qué hace | Costo por llamada |
|---|---|---|
| `buscar_sentencias` | Búsqueda híbrida (vector + léxica) sin rerank | ~$0.0001 |
| `obtener_texto_completo` | Texto OCR completo de un PDF identificado | $0 (lectura local) |

**No incluye rerank de Claude a propósito**: el LLM consumidor (Claude Code/Desktop)
ya va a leer y evaluar los resultados, así que pagar un segundo Claude para
pre-rankear sería un desperdicio. Si necesitas resultados rankeados con razones,
usa la interfaz web del buscador que sí tiene esa opción.

---

## Requisitos previos

1. **Node.js 18 o superior** instalado en la máquina donde correrá el MCP
   (normalmente tu laptop, no el VPS).
2. **Tu servidor del buscador corriendo y accesible** desde donde correrá el MCP.
   - Si el MCP corre en tu laptop y el buscador en tu VPS, necesitas que el
     puerto del buscador (3000 por default) sea accesible vía la URL pública del
     VPS, **o** abrir un túnel SSH (ver más abajo).

---

## Instalación

### 1. Clonar el repo (si aún no lo tienes en tu máquina local)

```bash
git clone https://github.com/introduccionalderechofvp-star/buscador-de-jurisprudencia.git
cd buscador-de-jurisprudencia/mcp
```

### 2. Instalar dependencias

```bash
npm install
```

Esto instala `@modelcontextprotocol/sdk` (oficial de Anthropic).

### 3. Probar que arranca

```bash
BUSCADOR_API_URL=http://tu-vps.com:3000 node server.js
```

Si todo va bien verás en stderr:

```
[MCP] Buscador de jurisprudencia conectado · API: http://tu-vps.com:3000
```

El proceso queda esperando en stdio. Lo matas con `Ctrl+C` — eso solo era
una prueba para verificar que arranca sin errores.

---

## Configurar Claude Desktop

1. Localiza tu archivo de configuración:
   - **macOS:** `~/Library/Application Support/Claude/claude_desktop_config.json`
   - **Windows:** `%APPDATA%\Claude\claude_desktop_config.json`
   - **Linux:** `~/.config/Claude/claude_desktop_config.json`

2. Si el archivo no existe, créalo. Si existe, edítalo añadiendo el bloque
   `mcpServers` (o agregando esta entrada al bloque existente):

```json
{
  "mcpServers": {
    "buscador-jurisprudencia": {
      "command": "node",
      "args": ["/ruta/absoluta/a/buscador-de-jurisprudencia/mcp/server.js"],
      "env": {
        "BUSCADOR_API_URL": "http://TU-VPS-O-LOCALHOST:3000"
      }
    }
  }
}
```

3. **Reemplaza** `/ruta/absoluta/a/buscador-de-jurisprudencia/mcp/server.js`
   con la ruta real en tu máquina (usa `pwd` dentro de la carpeta `mcp/`
   para encontrarla).

4. **Reemplaza** `TU-VPS-O-LOCALHOST:3000` con la URL del servidor del
   buscador. Por ejemplo `http://srv1555638.hstgr.cloud:3000` o `http://localhost:3000`.

5. **Reinicia Claude Desktop** completamente (cierra y vuelve a abrir).

6. Verifica: en una conversación, debería aparecer un icono de herramientas
   en la parte inferior del compositor. Al hacer clic, deberías ver
   `buscador-jurisprudencia` con `buscar_sentencias` y `obtener_texto_completo`.

---

## Configurar Claude Code

1. En cualquier lugar dentro del repo donde estés trabajando, ejecuta:

```bash
claude mcp add buscador-jurisprudencia node /ruta/absoluta/a/mcp/server.js \
  --env BUSCADOR_API_URL=http://TU-VPS:3000
```

2. O edita tu `.mcp.json` manualmente con la misma estructura del ejemplo
   de Claude Desktop arriba.

3. Verifica con `claude mcp list`. Deberías ver `buscador-jurisprudencia`.

---

## Conexión segura al VPS (recomendado)

Si tu buscador corre en el VPS pero no quieres exponer el puerto 3000
públicamente, usa un **túnel SSH** desde tu laptop:

```bash
ssh -L 3000:localhost:3000 root@srv1555638.hstgr.cloud -N
```

Esto crea un túnel persistente: cualquier petición a `http://localhost:3000`
en tu laptop se redirige al puerto 3000 del VPS por SSH (cifrado).

Con el túnel activo, configura el MCP con `BUSCADOR_API_URL=http://localhost:3000`
y todo funciona localmente sin abrir puertos.

**Tip:** puedes ponerlo en background con `-f` y `-N`:
```bash
ssh -f -N -L 3000:localhost:3000 root@srv1555638.hstgr.cloud
```

---

## Ejemplos de uso

Una vez configurado en Claude Desktop o Code, prueba con prompts como:

> *"Busca jurisprudencia sobre culpa médica en cesáreas, y resúmeme las
> tres tendencias jurisprudenciales más importantes de los últimos 5 años."*

> *"Encuéntrame todas las sentencias de la Sala Civil que traten sobre
> el artículo 1617 del Código Civil. De la más reciente, léeme el texto
> completo y extráeme los considerandos donde se interpreta ese artículo."*

> *"Compara cómo SC10189-2016 y SC11444-2016 tratan la responsabilidad
> contractual. Lee ambas completas y construye una tabla con las semejanzas
> y diferencias en su razonamiento."*

Claude va a:
1. Llamar `buscar_sentencias` con la consulta apropiada
2. Leer los fragmentos relevantes
3. Decidir cuáles documentos necesita completos
4. Llamar `obtener_texto_completo` para esos
5. Sintetizar la respuesta citando las sentencias

---

## Costos típicos

Para una sesión de investigación intensa (~10 búsquedas + ~5 textos completos):

| Componente | Costo |
|---|---|
| 10 llamadas a `buscar_sentencias` (embeddings OpenAI) | ~$0.001 |
| 5 llamadas a `obtener_texto_completo` (lectura local) | $0 |
| Tokens de Claude leyendo y sintetizando (~300K input) | ~$0.50 |
| **Total estimado** | **~$0.50** |

Comparado con una hora de un abogado junior buscando en Westlaw/vLex,
es ridículamente barato.

---

## Troubleshooting

**El MCP arranca pero Claude no lo ve:**
- Reinicia Claude Desktop por completo (no solo la ventana — cierra el proceso).
- Verifica que la ruta en `args` sea absoluta y exista.
- Mira los logs de Claude Desktop por errores.

**Errores de "Falló la búsqueda en el servidor":**
- Verifica que tu servidor del buscador esté corriendo (`pm2 list` en el VPS).
- Verifica que `BUSCADOR_API_URL` apunta al sitio correcto.
- Si usas túnel SSH, verifica que esté activo (`lsof -i :3000`).

**El servidor responde JSON mal formado:**
- Probablemente Cloudflare u otro proxy está devolviendo HTML de error.
- Usa la URL directa al puerto 3000, no detrás de un proxy.

**Quiero ver qué llamadas hace Claude:**
- En Claude Desktop, los logs del MCP suelen estar en
  `~/Library/Logs/Claude/mcp-server-*.log` (macOS).
- En Claude Code, las llamadas a herramientas se muestran en pantalla.
