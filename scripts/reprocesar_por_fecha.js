// scripts/reprocesar_por_fecha.js
// Reprocesa gastos comprobados de referencias cuya FechaApertura cae en un rango.
// Util para recuperar referencias faltantes en un periodo especifico.
//
// Uso:     node scripts/reprocesar_por_fecha.js <DESDE> <HASTA>
// Ejemplo: node scripts/reprocesar_por_fecha.js 2026-01-01 2026-02-28
// Ejemplo: node scripts/reprocesar_por_fecha.js 2026-03-01 2026-03-31

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

// Filtra por FechaApertura de la referencia (fecha en que se abrió el expediente)
const Q_POR_FECHA = `
SELECT
    d.nombreSistema,
    d.nombreOriginal,
    g.id_referencia,
    g.concepto,
    g.Adicional,
    r.facturada,
    r.FechaApertura,
    r.FechaDeModificacion,
    r.NumeroDeReferencia
FROM gastoscomprobados g
INNER JOIN Documentos d ON g.id_gastoComprobado = d.id_propio
INNER JOIN referencias r ON g.id_referencia = r.id_referencias
WHERE r.FechaApertura >= @desde
  AND r.FechaApertura <  @hasta
  AND r.facturada = 1
  AND d.id_tipoDocumento = 8888
  AND g.concepto IN ('MANIOBRAS', 'MANIOBRAS Y ALMACENAJES', 'ALMACENAJES', 'DEMORAS')
  AND d.nombreOriginal LIKE '%.xml'
ORDER BY r.FechaApertura, r.NumeroDeReferencia
`;

function linea() { console.log('------------------------------------------------------------'); }
function titulo(t) {
  console.log('\n============================================================');
  console.log(t);
  console.log('============================================================');
}

async function reprocesarPorFecha() {
  const [, , argDesde, argHasta] = process.argv;

  if (!argDesde || !argHasta) {
    console.log('Uso: node scripts/reprocesar_por_fecha.js <DESDE> <HASTA>');
    console.log('Ejemplo: node scripts/reprocesar_por_fecha.js 2026-01-01 2026-02-28');
    process.exit(1);
  }

  // Convertir fechas; HASTA se convierte al día siguiente para incluir todo el día
  const desde = new Date(argDesde + 'T00:00:00');
  const hastaExclusivo = new Date(argHasta + 'T00:00:00');
  hastaExclusivo.setDate(hastaExclusivo.getDate() + 1);

  if (isNaN(desde) || isNaN(hastaExclusivo)) {
    console.error('Fechas invalidas. Usa formato YYYY-MM-DD.');
    process.exit(1);
  }

  titulo(`REPROCESO POR FECHA: ${argDesde} al ${argHasta}`);
  console.log('Filtra por FechaApertura de la referencia (facturada=1).');

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

    // --- Consulta SQL Server ---
    titulo(`PASO 1: Consultando SQL Server (${argDesde} → ${argHasta})`);
    const req = new sql.Request(mssqlPool);
    req.input('desde', sql.DateTime, desde);
    req.input('hasta', sql.DateTime, hastaExclusivo);
    const rs = await req.query(Q_POR_FECHA);
    const gastos = rs.recordset;

    console.log(`\n  ${gastos.length} registro(s) encontrados`);

    if (gastos.length === 0) {
      console.log('\n  No se encontraron gastos comprobados en ese rango de fechas.');
      console.log('  Verifica que existan referencias con FechaApertura en ese periodo,');
      console.log('  con facturada=1 y con documentos XML de almacenaje/demora.');
      return;
    }

    // Mostrar resumen de referencias encontradas
    const refs = [...new Set(gastos.map(g => g.NumeroDeReferencia))];
    console.log(`  Referencias unicas: ${refs.length}`);
    refs.slice(0, 20).forEach(r => console.log(`    - ${r}`));
    if (refs.length > 20) console.log(`    ... y ${refs.length - 20} mas`);

    // --- Descargar XMLs y parsear ---
    titulo('PASO 2: Descargando XMLs desde SFTP y extrayendo conceptos');

    for (let i = 0; i < gastos.length; i++) {
      const gasto = gastos[i];
      const numRef = gasto.NumeroDeReferencia;
      const fileName = gasto.nombreSistema;
      const remotePath = path.posix.join(ftpConfig.basePath, numRef, 'GASTOS COMPROBADOS', fileName);

      linea();
      console.log(`[${i + 1}/${gastos.length}] Ref: ${numRef}`);
      console.log(`  Apertura : ${gasto.FechaApertura ? new Date(gasto.FechaApertura).toISOString().slice(0, 10) : 'null'}`);
      console.log(`  Archivo  : ${gasto.nombreOriginal}`);
      console.log(`  Ruta FTP : ${remotePath}`);
      console.log(`  Concepto DB: ${gasto.concepto}`);

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
        } catch {
          console.log(`  La carpeta "${carpeta}" no existe o no es accesible.`);
        }
        errores.push({ referencia: numRef, archivo: fileName, error: 'No se pudo descargar XML del SFTP' });
        continue;
      }

      console.log(`  [OK] Descargado (${xmlBuffer.length} bytes)`);

      const conceptos = parseConceptosGastos(xmlBuffer);
      console.log(`  Conceptos ALMACENAJE/DEMORA en XML: ${conceptos.length}`);

      if (conceptos.length === 0) {
        console.log('  [!] No se encontraron conceptos de ALMACENAJE o DEMORA en el XML.');
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
        label: 'reprocesar_por_fecha',
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

    // --- Resumen final ---
    const duration = Date.now() - startTime;
    titulo('RESUMEN FINAL');
    console.log(`  Rango solicitado        : ${argDesde} al ${argHasta}`);
    console.log(`  Referencias unicas      : ${refs.length}`);
    console.log(`  Registros en SQL Server : ${gastos.length}`);
    console.log(`  Conceptos extraidos     : ${valoresTotal.length}`);
    console.log(`  Errores                 : ${errores.length}`);
    console.log(`  Duracion                : ${(duration / 1000).toFixed(1)}s`);

    if (errores.length > 0) {
      console.log('\n  Detalle de errores:');
      errores.forEach(e => console.log(`    - [${e.referencia}] ${e.error}`));
    }

    if (valoresTotal.length > 0) {
      console.log('\n  [OK] Reproceso completado exitosamente.');
    } else {
      console.log('\n  [!] No se insertaron registros. Revisa los errores arriba.');
    }

  } catch (err) {
    console.error('\n[FATAL]', err.message);
    try { if (my) await my.rollback(); } catch {}
    throw err;

  } finally {
    if (sftpClient) await sftpClient.disconnect();
    if (my) await my.end();
    if (mssqlPool) await mssqlPool.close();
  }
}

reprocesarPorFecha().catch(err => {
  console.error('Fallo:', err.message);
  process.exit(1);
});
