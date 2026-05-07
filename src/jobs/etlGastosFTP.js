// src/jobs/etlGastosFTP.js - Job ETL para procesar gastos comprobados vía FTP

const sql = require('mssql');
const mysql = require('mysql2/promise');
const path = require('path');

const { mssqlConfig, mysqlConfig } = require('../config/database');
const { ftpConfig } = require('../config/ftp');
const { Q_GASTOS_COMPROBADOS, UPSERT_FTP_ADICIONAL } = require('../queries/gastosComprobados');
const { SFTPService } = require('../services/sftpClient');
const { parseConceptosGastos } = require('../services/xmlParser');
const { upsertChunks } = require('../services/mysqlHelpers');
const { chunk } = require('../utils/arrays');

/**
 * Ejecuta el ETL completo de gastos comprobados desde FTP
 */
async function runEtlGastosFTP() {
  let mssqlPool, my, ftpClient;
  const valoresTotal = [];
  const errores = [];
  const startTime = Date.now();

  try {
    console.log('[ETL-FTP] ==================================================');
    console.log('[ETL-FTP] INICIANDO PROCESO ETL-FTP GASTOS COMPROBADOS');
    console.log('[ETL-FTP] ==================================================');
    console.log(`[ETL-FTP] Hora inicio: ${new Date().toISOString()}`);
    console.log(`[ETL-FTP] FTP Host: ${ftpConfig.host}:${ftpConfig.port}`);
    console.log(`[ETL-FTP] FTP Base Path: ${ftpConfig.basePath}`);

    // 1. Conectar a bases de datos y FTP
    console.log('\n[ETL-FTP] PASO 1: Conectando a bases de datos...');
    
    console.log('[ETL-FTP] → Conectando a SQL Server...');
    mssqlPool = await sql.connect(mssqlConfig);
    console.log('[ETL-FTP] SQL Server conectado');
    
    console.log('[ETL-FTP] → Conectando a MySQL...');
    my = await mysql.createConnection(mysqlConfig);
    await my.query("SET time_zone = '-06:00'");
    console.log('[ETL-FTP] MySQL conectado');

    console.log('[ETL-FTP] → Conectando a SFTP (puerto 22)...');
    ftpClient = new SFTPService();
    await ftpClient.connect();
    console.log('[ETL-FTP] SFTP conectado');

    // 2. Obtener gastos comprobados de SQL Server
    console.log('\n[ETL-FTP] PASO 2: Consultando gastos comprobados en SQL Server...');
    console.log('[ETL-FTP] → Ejecutando query...');
    
    const req = new sql.Request(mssqlPool);
    const rs = await req.query(Q_GASTOS_COMPROBADOS);
    const gastos = rs.recordset;

    console.log(`[ETL-FTP] ${gastos.length} gastos comprobados encontrados`);
    
    if (gastos.length > 0) {
      console.log('[ETL-FTP] → Muestra de registros:');
      for (let i = 0; i < Math.min(3, gastos.length); i++) {
        const g = gastos[i];
        console.log(`[ETL-FTP]   [${i + 1}] Ref: ${g.NumeroDeReferencia}, Archivo: ${g.nombreOriginal}, Concepto: ${g.concepto}`);
      }
      if (gastos.length > 3) {
        console.log(`[ETL-FTP]   ... y ${gastos.length - 3} registros más`);
      }
    }

    if (gastos.length === 0) {
      console.log('[ETL-FTP] No hay registros para procesar');
      return { processed: 0, errors: 0 };
    }

    // 3. Procesar cada gasto: descargar XML y extraer conceptos
    console.log('\n[ETL-FTP] PASO 3: Descargando y procesando archivos XML desde FTP...');
    
    let procesados = 0;
    let conConceptos = 0;
    let sinConceptos = 0;
    let erroresFTP = 0;
    
    for (let i = 0; i < gastos.length; i++) {
      const gasto = gastos[i];
      const numRef = gasto.NumeroDeReferencia;
      const fileName = gasto.nombreSistema;

      console.log(`\n[ETL-FTP] [${i + 1}/${gastos.length}] Procesando: Ref=${numRef}, Archivo=${fileName} (original: ${gasto.nombreOriginal})`);

      // Construir ruta FTP: /Referencias/{NumeroDeReferencia}/GASTOS COMPROBADOS/{nombreSistema}
      const remotePath = path.posix.join(
        ftpConfig.basePath,
        numRef,
        'GASTOS COMPROBADOS',
        fileName
      );

      try {
        procesados++;
        
        // Descargar archivo XML
        console.log(`[ETL-FTP]   → Ruta FTP: ${remotePath}`);
        const xmlBuffer = await ftpClient.downloadFile(remotePath);

        if (!xmlBuffer) {
          erroresFTP++;
          const errorMsg = `Archivo no encontrado o error descargando de FTP: ${remotePath}`;
          console.log(`[ETL-FTP]  ${errorMsg}`);
          errores.push({ referencia: numRef, archivo: fileName, error: errorMsg });
          continue;
        }

        // Parsear XML y extraer conceptos
        console.log(`[ETL-FTP]   → Parseando XML...`);
        const conceptos = parseConceptosGastos(xmlBuffer);

        console.log(`[ETL-FTP]   → Conceptos crudos encontrados: ${conceptos.length}`);
        if (conceptos.length === 0) {
          sinConceptos++;
          console.log(`[ETL-FTP]   → No se encontraron conceptos Almacenaje/Demora en ${fileName}`);
          continue;
        }

        conConceptos++;
        console.log(`[ETL-FTP]   Conceptos encontrados: ${conceptos.map(c => `${c.concepto}=$${c.importe.toFixed(2)}`).join(', ')}`);

        // Crear un registro por cada concepto individual encontrado
        for (const c of conceptos) {
          valoresTotal.push([
            numRef,
            fileName,
            c.importe,
            c.concepto,
            c.descripcion,
            gasto.Adicional || ''
          ]);
        }

      } catch (err) {
        console.error(`[ETL-FTP] Error procesando ${numRef}:`, err.message);
        errores.push({ referencia: numRef, archivo: fileName, error: err.message });
      }
    }

    // 4. Resumen de datos preparados
    console.log(`\n[ETL-FTP] PASO 4: ${valoresTotal.length} registros listos para insertar/actualizar`);

    // 5. Insertar/Actualizar en MySQL
    if (valoresTotal.length > 0) {
      console.log('\n[ETL-FTP] PASO 5: Insertando/Actualizando en MySQL...');
      await my.beginTransaction();

      const res = await upsertChunks(my, UPSERT_FTP_ADICIONAL, valoresTotal, 500, {
        label: 'ftp_adicional',
        idIndex: 0
      });

      await my.commit();

      console.log(`[ETL-FTP] Upsert completado: ${res.totals.records} registros, ${res.totals.warnings} warnings`);
    } else {
      console.log('[ETL-FTP] No hay registros para insertar');
    }

    // 6. Resumen
    const duration = Date.now() - startTime;
    console.log('\n[ETL-FTP] ==================================================');
    console.log('[ETL-FTP] RESUMEN FINAL');
    console.log('[ETL-FTP] ==================================================');
    console.log(`[ETL-FTP] Duración total: ${(duration / 1000).toFixed(1)}s`);
    console.log(`[ETL-FTP] Archivos encontrados en SQL Server: ${gastos.length}`);
    console.log(`[ETL-FTP] Archivos intentados procesar: ${procesados}`);
    console.log(`[ETL-FTP] Archivos con conceptos Almacenaje/Demora: ${conConceptos}`);
    console.log(`[ETL-FTP] Archivos sin conceptos: ${sinConceptos}`);
    console.log(`[ETL-FTP] Errores descarga FTP: ${erroresFTP}`);
    console.log(`[ETL-FTP] Registros insertados/actualizados en MySQL: ${valoresTotal.length}`);
    console.log(`[ETL-FTP] Total errores: ${errores.length}`);

    if (errores.length > 0) {
      console.log('\n[ETL-FTP] ERRORES DETALLADOS:');
      for (const err of errores.slice(0, 10)) {
        console.log(`[ETL-FTP]   - Ref ${err.referencia}: ${err.error}`);
      }
      if (errores.length > 10) {
        console.log(`[ETL-FTP]   ... y ${errores.length - 10} errores más`);
      }
    }

    console.log('[ETL-FTP] ==================================================');
    console.log('[ETL-FTP] PROCESO COMPLETADO');
    console.log('[ETL-FTP] ==================================================');

    return {
      processed: gastos.length,
      procesados: procesados,
      conConceptos: conConceptos,
      sinConceptos: sinConceptos,
      erroresFTP: erroresFTP,
      inserted: valoresTotal.length,
      errors: errores.length,
      duration: duration,
      errorDetails: errores
    };

  } catch (err) {
    const duration = Date.now() - startTime;
    console.error('\n[ETL-FTP] ==================================================');
    console.error('[ETL-FTP] ERROR FATAL DEL PROCESO');
    console.error('[ETL-FTP] ==================================================');
    console.error(`[ETL-FTP] Tiempo hasta error: ${(duration / 1000).toFixed(1)}s`);
    console.error(`[ETL-FTP] Error: ${err.message}`);
    console.error(`[ETL-FTP] Stack: ${err.stack}`);
    
    try {
      if (my) {
        console.error('[ETL-FTP] → Haciendo rollback de transacción...');
        await my.rollback();
        console.error('[ETL-FTP] → Rollback completado');
      }
    } catch (e) { 
      console.error('[ETL-FTP] → Error haciendo rollback:', e.message);
    }
    throw err;

  } finally {
    // Cerrar conexiones
    try {
      if (ftpClient) await ftpClient.disconnect();
    } catch (e) { 
      console.error('[ETL-FTP] Error cerrando SFTP:', e.message);
    }
    try {
      if (my) await my.end();
    } catch (e) { /* noop */ }
    try {
      if (mssqlPool) await mssqlPool.close();
    } catch (e) { /* noop */ }
  }
}

module.exports = { runEtlGastosFTP };
