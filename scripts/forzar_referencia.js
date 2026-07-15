// scripts/forzar_referencia.js
// Fuerza el procesamiento ETL-FTP de una o mas referencias especificas,
// ignorando el filtro de FechaDeModificacion del proceso automatico.
// Descarga el XML del SFTP, lo parsea y lo inserta/actualiza en MySQL.
//
// Uso:     node scripts/forzar_referencia.js <REF1> [REF2] [REF3] ...
// Ejemplo: node scripts/forzar_referencia.js MI250033-00
// Ejemplo: node scripts/forzar_referencia.js MI250033-00 AP260018-00 MI260040-00

require('dotenv').config();
const sql = require('mssql');
const mysql = require('mysql2/promise');
const path = require('path');

const { mssqlConfig, mysqlConfig } = require('../src/config/database');
const { ftpConfig } = require('../src/config/ftp');
const { UPSERT_FTP_ADICIONAL } = require('../src/queries/gastosComprobados');
const { SFTPService } = require('../src/services/sftpClient');
const { parseConceptosGastos, validarConcepto } = require('../src/services/xmlParser');
const { upsertChunks } = require('../src/services/mysqlHelpers');

// Query SIN filtro de fecha ni de FechaDeModificacion.
// Solo filtra por referencia, tipo de documento y concepto de almacenaje/demora.
const Q_FORZAR = `
SELECT
    d.nombreSistema,
    d.nombreOriginal,
    g.id_referencia,
    g.concepto,
    g.Adicional,
    r.facturada,
    r.FechaDeModificacion,
    r.NumeroDeReferencia
FROM gastoscomprobados g
INNER JOIN Documentos d ON g.id_gastoComprobado = d.id_propio
INNER JOIN referencias r ON g.id_referencia = r.id_referencias
WHERE r.NumeroDeReferencia IN (REFERENCIAS)
  AND d.id_tipoDocumento = 8888
  AND g.concepto IN ('MANIOBRAS', 'MANIOBRAS Y ALMACENAJES', 'ALMACENAJES', 'DEMORAS')
  AND d.nombreOriginal LIKE '%.xml'
`;

function linea() { console.log('------------------------------------------------------------'); }
function titulo(t) {
  console.log('\n============================================================');
  console.log(t);
  console.log('============================================================');
}

async function forzar() {
  const refs = process.argv.slice(2);

  if (refs.length === 0) {
    console.log('Uso: node scripts/forzar_referencia.js <REF1> [REF2] ...');
    console.log('Ejemplo: node scripts/forzar_referencia.js MI250033-00');
    process.exit(1);
  }

  titulo(`PROCESAMIENTO FORZADO: ${refs.join(', ')}`);
  console.log('ATENCION: Este script ignora el filtro de fecha del ETL automatico.');

  let mssqlPool, my, sftpClient;
  const valoresTotal = [];
  const errores = [];
  const startTime = Date.now();

  try {
    // --- Conexiones ---
    console.log('\n[1] Conectando a bases de datos...');
    mssqlPool = await sql.connect(mssqlConfig);
    my = await mysql.createConnection(mysqlConfig);
    await my.query("SET time_zone = '-06:00'");
    console.log('    SQL Server OK | MySQL OK');

    console.log('[2] Conectando a SFTP...');
    sftpClient = new SFTPService();
    await sftpClient.connect();
    console.log('    SFTP OK');

    // --- Consulta en SQL Server (sin filtro de fecha) ---
    titulo('PASO 1: Consultando SQL Server (sin filtro de fecha)');
    const placeholders = refs.map(r => `'${r}'`).join(', ');
    const query = Q_FORZAR.replace('REFERENCIAS', placeholders);
    const req = new sql.Request(mssqlPool);
    const rs = await req.query(query);
    const gastos = rs.recordset;

    console.log(`\n  ${gastos.length} registro(s) encontrados`);

    if (gastos.length === 0) {
      console.log('\n  ADVERTENCIA: No se encontraron registros para las referencias indicadas.');
      console.log('  Verifica que existan en gastoscomprobados con concepto de almacenaje/demora.');
      return;
    }

    for (const g of gastos) {
      console.log(`  - [${g.NumeroDeReferencia}] ${g.nombreOriginal} (concepto: ${g.concepto}, facturada: ${g.facturada})`);
      if (!g.facturada) {
        console.log(`    AVISO: facturada=0, se procesara de todas formas (forzado).`);
      }
    }

    // --- Descargar XMLs y parsear ---
    titulo('PASO 2: Descargando XMLs desde SFTP y extrayendo conceptos');

    for (let i = 0; i < gastos.length; i++) {
      const gasto = gastos[i];
      const numRef = gasto.NumeroDeReferencia;
      const fileName = gasto.nombreSistema;
      const remotePath = path.posix.join(ftpConfig.basePath, numRef, 'GASTOS COMPROBADOS', fileName);

      linea();
      console.log(`[${i + 1}/${gastos.length}] Ref: ${numRef}`);
      console.log(`  Archivo  : ${gasto.nombreOriginal}`);
      console.log(`  Ruta FTP : ${remotePath}`);
      console.log(`  Concepto DB: ${gasto.concepto}`);
      console.log(`  FechaModif : ${gasto.FechaDeModificacion ? new Date(gasto.FechaDeModificacion).toISOString() : 'null'}`);

      let xmlBuffer = null;
      try {
        xmlBuffer = await sftpClient.downloadFile(remotePath);
      } catch (e) {
        console.log(`  [X] Error descargando: ${e.message}`);
      }

      if (!xmlBuffer) {
        // Listar carpeta para ayudar a diagnosticar
        const carpeta = path.posix.join(ftpConfig.basePath, numRef, 'GASTOS COMPROBADOS');
        try {
          const lista = await sftpClient.client.list(carpeta);
          if (lista.length > 0) {
            console.log('  Carpeta existe. Archivos disponibles:');
            lista.forEach(f => console.log(`    - ${f.name} (${f.size} bytes)`));
          } else {
            console.log('  Carpeta existe pero esta vacia.');
          }
        } catch (e) {
          console.log(`  La carpeta "${carpeta}" no existe o no es accesible.`);
        }
        errores.push({ referencia: numRef, archivo: fileName, error: 'No se pudo descargar XML del SFTP' });
        continue;
      }

      console.log(`  [OK] Descargado (${xmlBuffer.length} bytes)`);

      // Parsear conceptos
      const conceptos = parseConceptosGastos(xmlBuffer);
      console.log(`  Conceptos ALMACENAJE/DEMORA en XML: ${conceptos.length}`);

      if (conceptos.length === 0) {
        console.log('  [!] No se encontraron conceptos de ALMACENAJE o DEMORA en el XML.');
        console.log('      Vista previa del XML:');
        console.log(xmlBuffer.toString('utf-8').substring(0, 800));
        errores.push({ referencia: numRef, archivo: fileName, error: 'XML sin conceptos ALMACENAJE/DEMORA' });
        continue;
      }

      for (const c of conceptos) {
        const alerta = validarConcepto(c, numRef, fileName);
        if (alerta) {
          console.warn(`  [!] ${alerta}`);
          errores.push({ referencia: numRef, archivo: fileName, error: alerta });
          continue;
        }
        console.log(`  [OK] ${c.concepto}: $${c.importe.toFixed(2)} | "${c.descripcion.substring(0, 60)}"`);
        valoresTotal.push([
          numRef,
          fileName,
          c.importe,
          c.concepto,
          c.descripcion,
          gasto.Adicional || ''
        ]);
      }
    }
    linea();

    // --- Insertar en MySQL ---
    titulo('PASO 3: Insertando / Actualizando en MySQL');

    if (valoresTotal.length === 0) {
      console.log('\n  No hay registros para insertar. Revisa los errores arriba.');
    } else {
      console.log(`\n  ${valoresTotal.length} registro(s) a insertar/actualizar...`);
      await my.beginTransaction();
      const res = await upsertChunks(my, UPSERT_FTP_ADICIONAL, valoresTotal, 500, {
        label: 'forzar_referencia',
        idIndex: 0
      });
      await my.commit();

      const nuevos = res.totals.records - res.totals.duplicates;
      console.log(`\n  Resultado:`);
      console.log(`    Nuevos          : ${nuevos}`);
      console.log(`    Actualizados    : ${res.totals.changedRows}`);
      console.log(`    Sin cambios     : ${res.totals.duplicates - res.totals.changedRows}`);
      console.log(`    Total procesados: ${res.totals.records}`);
    }

    // --- Resumen ---
    const duration = Date.now() - startTime;
    titulo('RESUMEN FINAL');
    console.log(`  Referencias solicitadas : ${refs.join(', ')}`);
    console.log(`  Registros en SQL Server : ${gastos.length}`);
    console.log(`  Conceptos extraidos     : ${valoresTotal.length}`);
    console.log(`  Errores                 : ${errores.length}`);
    console.log(`  Duracion                : ${(duration / 1000).toFixed(1)}s`);

    if (errores.length > 0) {
      console.log('\n  Detalle de errores:');
      errores.forEach(e => console.log(`    - [${e.referencia}] ${e.error}`));
    }

    if (valoresTotal.length > 0) {
      console.log('\n  [OK] Procesamiento completado exitosamente.');
    } else {
      console.log('\n  [!] No se insertaron registros. Revisa los errores arriba.');
    }

  } catch (err) {
    console.error('\n[ERROR FATAL]', err.message);
    try { if (my) await my.rollback(); } catch (e) { /* noop */ }
  } finally {
    try { if (sftpClient) await sftpClient.disconnect(); } catch (e) { /* noop */ }
    try { if (my) await my.end(); } catch (e) { /* noop */ }
    try { if (mssqlPool) await mssqlPool.close(); } catch (e) { /* noop */ }
  }
}

forzar();
