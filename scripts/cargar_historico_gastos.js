// scripts/cargar_historico_gastos.js - Cargar histórico de gastos desde 1 ene 2026
// Si FTP no está disponible, puede procesar archivos XML locales

require('dotenv').config();
const sql = require('mssql');
const mysql = require('mysql2/promise');
const fs = require('fs');
const path = require('path');

const { mssqlConfig, mysqlConfig } = require('../src/config/database');
const { Q_GASTOS_COMPROBADOS, UPSERT_FTP_ADICIONAL } = require('../src/queries/gastosComprobados');
const { SFTPService } = require('../src/services/sftpClient');
const { parseConceptosGastos, validarConcepto } = require('../src/services/xmlParser');
const { upsertChunks } = require('../src/services/mysqlHelpers');

// Query completa sin restriccion de fecha para cargar todo el historico
const Q_HISTORICO_GASTOS = `
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
WHERE r.FechaDeModificacion >= '2026-01-01'
  AND r.facturada = 1
  AND d.id_tipoDocumento = 8888
  AND g.concepto IN ('MANIOBRAS', 'MANIOBRAS Y ALMACENAJES', 'ALMACENAJES', 'DEMORAS')
  AND d.nombreOriginal LIKE '%.xml'
`;

async function cargarHistorico() {
  let mssqlPool, my, sftpClient;
  const valoresTotal = [];
  const errores = [];
  const startTime = Date.now();

  // Modo: FTP o LOCAL
  const modo = process.argv[2] || 'ftp'; // 'ftp' o 'local'
  const carpetaLocal = process.argv[3]; // para modo local

  try {
    console.log('[HISTORICO] ============================================');
    console.log('[HISTORICO] CARGA HISTORICA DE GASTOS');
    console.log('[HISTORICO] Desde: 1 de Enero 2026');
    console.log('[HISTORICO] Modo:', modo.toUpperCase());
    console.log('[HISTORICO] ============================================');

    // Conectar BD
    console.log('\n[HISTORICO] Conectando a bases de datos...');
    mssqlPool = await sql.connect(mssqlConfig);
    my = await mysql.createConnection(mysqlConfig);
    await my.query("SET time_zone = '-06:00'");
    console.log('[HISTORICO] Bases de datos conectadas');

    // Conectar SFTP si es modo FTP
    if (modo === 'ftp') {
      console.log('[HISTORICO] Conectando a SFTP...');
      sftpClient = new SFTPService();
      await sftpClient.connect();
      console.log('[HISTORICO] SFTP conectado');
    } else {
      console.log('[HISTORICO] Modo LOCAL - no se requiere SFTP');
      if (!carpetaLocal || !fs.existsSync(carpetaLocal)) {
        console.error('[HISTORICO] Debes especificar carpeta valida con XMLs locales');
        console.log('[HISTORICO] Uso: node cargar_historico_gastos.js local /ruta/a/xmls');
        process.exit(1);
      }
    }

    // Obtener gastos
    console.log('\n[HISTORICO] Consultando gastos desde 1 ene 2026...');
    const req = new sql.Request(mssqlPool);
    const rs = await req.query(Q_HISTORICO_GASTOS);
    const gastos = rs.recordset;

    console.log(`[HISTORICO] ${gastos.length} gastos encontrados`);

    // Procesar cada gasto
    for (let i = 0; i < gastos.length; i++) {
      const gasto = gastos[i];
      const numRef = gasto.NumeroDeReferencia;
      const fileName = gasto.nombreSistema;

      console.log(`\n[HISTORICO] [${i + 1}/${gastos.length}] Ref=${numRef} - ${fileName} (original: ${gasto.nombreOriginal})`);

      try {
        let xmlBuffer;

        if (modo === 'ftp') {
          // Descargar desde SFTP usando nombreSistema
          const remotePath = path.posix.join(
            '/Referencias',
            numRef,
            'GASTOS COMPROBADOS',
            fileName
          );
          console.log(`[HISTORICO]   → Descargando: ${remotePath}`);
          xmlBuffer = await sftpClient.downloadFile(remotePath);
        } else {
          // Leer archivo local
          const localPath = path.join(carpetaLocal, fileName);
          console.log(`[HISTORICO]   → Leyendo local: ${localPath}`);
          if (fs.existsSync(localPath)) {
            xmlBuffer = fs.readFileSync(localPath);
          } else {
            console.log(`[HISTORICO]   Archivo no existe: ${localPath}`);
            continue;
          }
        }

        if (!xmlBuffer) {
          errores.push({ referencia: numRef, archivo: fileName, error: 'No se pudo obtener XML' });
          continue;
        }

        // Parsear XML
        const conceptos = parseConceptosGastos(xmlBuffer);
        console.log(`[HISTORICO]   → Conceptos: ${conceptos.length}`);

        if (conceptos.length === 0) {
          console.log(`[HISTORICO]   → Sin Almacenaje/Demora`);
          continue;
        }

        // Crear un registro por cada concepto individual encontrado
        for (const c of conceptos) {
          const alerta = validarConcepto(c, numRef, fileName);
          if (alerta) {
            console.warn(`[HISTORICO]   ${alerta}`);
            errores.push({ referencia: numRef, archivo: fileName, error: alerta });
            continue;
          }
          console.log(`[HISTORICO]   → ${c.concepto}: $${c.importe.toFixed(2)} | ${c.descripcion.substring(0, 50)}`);
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
        console.error(`[HISTORICO] Error:`, err.message);
        errores.push({ referencia: numRef, archivo: fileName, error: err.message });
      }
    }

    // Insertar en MySQL
    console.log(`\n[HISTORICO] ${valoresTotal.length} registros para MySQL`);

    if (valoresTotal.length > 0) {
      console.log('[HISTORICO] Insertando en MySQL...');
      await my.beginTransaction();
      const res = await upsertChunks(my, UPSERT_FTP_ADICIONAL, valoresTotal, 500, {
        label: 'ftp_adicional_historico',
        idIndex: 0
      });
      await my.commit();
      const nuevos = res.totals.records - res.totals.duplicates;
      console.log(`[HISTORICO] Resultado: ${nuevos} nuevos | ${res.totals.changedRows} actualizados | ${res.totals.duplicates - res.totals.changedRows} sin cambios | total procesados: ${res.totals.records}`);
    }

    // Resumen
    const duration = Date.now() - startTime;
    console.log('\n[HISTORICO] ============================================');
    console.log('[HISTORICO] RESUMEN HISTORICO');
    console.log('[HISTORICO] ============================================');
    console.log(`[HISTORICO] Duración: ${(duration / 1000).toFixed(1)}s`);
    console.log(`[HISTORICO] Gastos procesados: ${gastos.length}`);
    console.log(`[HISTORICO] Registros insertados: ${valoresTotal.length}`);
    console.log(`[HISTORICO] Errores: ${errores.length}`);

    if (errores.length > 0) {
      console.log('\n[HISTORICO] Errores:');
      errores.slice(0, 10).forEach(e => {
        console.log(`  - ${e.referencia}: ${e.error}`);
      });
    }

  } catch (err) {
    console.error('[HISTORICO] Error fatal:', err);
    try { if (my) await my.rollback(); } catch (e) {}
    throw err;

  } finally {
    if (sftpClient) await sftpClient.disconnect();
    if (my) await my.end();
    if (mssqlPool) await mssqlPool.close();
  }
}

console.log('Uso:');
console.log('  node cargar_historico_gastos.js ftp              # Descargar desde SFTP');
console.log('  node cargar_historico_gastos.js local /ruta/xmls # Procesar archivos locales');
console.log('');

cargarHistorico().catch(err => {
  console.error('[HISTORICO] Falló:', err.message);
  process.exit(1);
});
