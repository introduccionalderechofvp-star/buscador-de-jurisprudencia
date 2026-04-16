/**
 * Conversión de documentos Word (.docx/.doc) a PDF usando LibreOffice headless.
 *
 * El scraper necesita esto porque algunas cortes publican sentencias solo en
 * formato Word (ej. Sala Penal de la CSJ). Para mantener el corpus homogéneo
 * (todo .pdf en disco, un solo extractor de texto downstream), convertimos al
 * momento de la descarga.
 *
 * Requisito: LibreOffice instalado en el sistema.
 *   apt-get install -y libreoffice-core libreoffice-writer
 *
 * La ubicación del binario se puede overridear con LIBREOFFICE_BIN en .env.
 *
 * Diseño:
 *   - Preflight check LAZY: la primera invocación de convertDocxBufferToPdf
 *     verifica que libreoffice está en PATH. Si no, lanza un error con
 *     instrucciones claras de instalación. Resultado cacheado para siguientes
 *     invocaciones.
 *   - UserInstallation único por invocación: LibreOffice bloquea el directorio
 *     de perfil durante la ejecución, así que no se pueden correr dos
 *     instancias compartiendo perfil. Usar un temp dir por conversión evita
 *     problemas si algún día corremos scrapers en paralelo.
 *   - Cleanup garantizado con try/finally: el temp dir se borra siempre,
 *     incluso si LibreOffice crashea.
 */

import { execFileSync } from 'node:child_process';
import fs from 'node:fs';
import path from 'node:path';
import os from 'node:os';

const LIBREOFFICE_BIN = process.env.LIBREOFFICE_BIN || 'libreoffice';
const CONVERSION_TIMEOUT_MS = 90_000;   // 90s por conversión (suficiente para docs de 100+ páginas)
const PREFLIGHT_TIMEOUT_MS  = 10_000;

let preflightOk = null;   // null = no verificado todavía, true = ok, false = ya falló

function preflight() {
  if (preflightOk === true)  return;
  if (preflightOk === false) {
    throw new Error(
      `LibreOffice no está disponible (binario "${LIBREOFFICE_BIN}"). ` +
      `Instálalo con: apt-get install -y libreoffice-core libreoffice-writer`
    );
  }

  try {
    execFileSync(LIBREOFFICE_BIN, ['--version'], {
      stdio: ['ignore', 'pipe', 'pipe'],
      timeout: PREFLIGHT_TIMEOUT_MS
    });
    preflightOk = true;
  } catch (e) {
    preflightOk = false;
    throw new Error(
      `LibreOffice no responde (binario "${LIBREOFFICE_BIN}"): ${e.message}. ` +
      `Instálalo con: apt-get install -y libreoffice-core libreoffice-writer`
    );
  }
}

/**
 * Convierte un Buffer de .docx o .doc a un Buffer de .pdf.
 *
 * @param {Buffer} docBuffer — contenido binario del archivo fuente
 * @param {'docx'|'doc'} [sourceExt='docx'] — extensión del archivo fuente
 * @returns {Promise<Buffer>} el PDF resultante como Buffer
 * @throws {Error} si LibreOffice no está instalado o la conversión falla
 */
export async function convertDocxBufferToPdf(docBuffer, sourceExt = 'docx') {
  preflight();

  if (!docBuffer || docBuffer.length === 0) {
    throw new Error('Buffer de entrada vacío');
  }

  const tmpDir      = fs.mkdtempSync(path.join(os.tmpdir(), 'docx2pdf-'));
  const profileDir  = path.join(tmpDir, 'lo-profile');
  const inputPath   = path.join(tmpDir, `input.${sourceExt}`);
  const outputPath  = path.join(tmpDir, 'input.pdf');

  try {
    fs.writeFileSync(inputPath, docBuffer);

    execFileSync(
      LIBREOFFICE_BIN,
      [
        '--headless',
        '--convert-to', 'pdf',
        '--outdir', tmpDir,
        inputPath
      ],
      {
        stdio: ['ignore', 'pipe', 'pipe'],
        timeout: CONVERSION_TIMEOUT_MS,
        env: {
          ...process.env,
          // Aislar el perfil de usuario de LibreOffice por conversión.
          // Evita conflictos si dos procesos corren concurrentemente.
          UserInstallation: `file://${profileDir}`
        }
      }
    );

    if (!fs.existsSync(outputPath)) {
      throw new Error('LibreOffice terminó sin error pero no generó PDF');
    }
    const pdfBuffer = fs.readFileSync(outputPath);
    if (pdfBuffer.length === 0) {
      throw new Error('PDF generado está vacío');
    }
    return pdfBuffer;
  } finally {
    // Cleanup garantizado incluso si la conversión falla
    try {
      fs.rmSync(tmpDir, { recursive: true, force: true });
    } catch {}
  }
}

/**
 * Verifica disponibilidad de LibreOffice sin intentar conversión.
 * Útil para arrancar un scraper y fallar rápido si no está instalado.
 */
export function isLibreOfficeAvailable() {
  try {
    preflight();
    return true;
  } catch {
    return false;
  }
}
