// test_eventos.js - Script de prueba para verificar eventos
require('dotenv').config();
const sql = require('mssql');
const mysql = require('mysql2/promise');

const mssqlConfig = {
  server: process.env.MSSQL_SERVER,
  database: process.env.MSSQL_DB,
  user: process.env.MSSQL_USER,
  password: process.env.MSSQL_PASS,
  port: Number(process.env.MSSQL_PORT || 1433),
  connectionTimeout: Number(process.env.MSSQL_CONN_TIMEOUT_MS || 30000),
  requestTimeout: Number(process.env.MSSQL_REQUEST_TIMEOUT_MS || 300000),
  pool: { max: 5, min: 0, idleTimeoutMillis: 30000 },
  options: { encrypt: true, trustServerCertificate: true }
};

const mysqlConfig = {
  host: process.env.MYSQL_HOST1,
  user: process.env.MYSQL_USER1,
  password: process.env.MYSQL_PASS1,
  database: process.env.MYSQL_DB1,
  port: Number(process.env.MYSQL_PORT1 || 3306)
};

const Q_TEST = `
SELECT TOP 5
  r.id_referencias,
  r.NumeroDeReferencia,
  r.Operacion,
  MAX(CASE WHEN r.Operacion = 1 AND b.IdEvento = 47 THEN b.FechaHoraCapturada 
           WHEN r.Operacion = 2 AND be.IdEvento = 47 THEN be.FechaHoraCapturada END) AS ENTREGA_FAC,
  MAX(CASE WHEN r.Operacion = 1 AND b.IdEvento = 48 THEN b.FechaHoraCapturada 
           WHEN r.Operacion = 2 AND be.IdEvento = 48 THEN be.FechaHoraCapturada END) AS FECHA_FAC,
  MAX(CASE WHEN r.Operacion = 1 AND b.IdEvento = 49 THEN b.FechaHoraCapturada 
           WHEN r.Operacion = 2 AND be.IdEvento = 49 THEN be.FechaHoraCapturada END) AS ENTREGA_FAC_CLI,
  MAX(CASE WHEN r.Operacion = 1 AND b.IdEvento = 26 THEN b.FechaHoraCapturada 
           WHEN r.Operacion = 2 AND be.IdEvento = 26 THEN be.FechaHoraCapturada END) AS ENTREGA_CAPTURA,
  MAX(CASE WHEN r.Operacion = 1 AND b.IdEvento = 33 THEN b.FechaHoraCapturada 
           WHEN r.Operacion = 2 AND be.IdEvento = 33 THEN be.FechaHoraCapturada END) AS INICIO_CAPTURA,
  MAX(CASE WHEN r.Operacion = 1 AND b.IdEvento = 42 THEN b.FechaHoraCapturada 
           WHEN r.Operacion = 2 AND be.IdEvento = 42 THEN be.FechaHoraCapturada END) AS TERMINO_CAPTURA,
  MAX(CASE WHEN r.Operacion = 1 AND b.IdEvento = 36 THEN b.FechaHoraCapturada 
           WHEN r.Operacion = 2 AND be.IdEvento = 36 THEN be.FechaHoraCapturada END) AS PRIMER_RECONOCIMIENTO
FROM referencias r
LEFT JOIN BitacoraEventosImportacion b ON b.Referencia = r.id_referencias
LEFT JOIN BitacoraEventosExportacion be ON be.Referencia = r.id_referencias
WHERE r.Cancelada = 0
  AND (
    (r.Operacion = 1 AND b.IdEvento IN (47, 48, 49, 26, 33, 42, 36))
    OR
    (r.Operacion = 2 AND be.IdEvento IN (47, 48, 49, 26, 33, 42, 36))
  )
GROUP BY r.id_referencias, r.NumeroDeReferencia, r.Operacion
ORDER BY r.id_referencias DESC
`;

(async () => {
  let mssqlPool, my;
  try {
    console.log('🔍 PRUEBA DE EVENTOS - Verificando últimos 7 eventos\n');
    console.log('Conectando...');
    
    mssqlPool = await sql.connect(mssqlConfig);
    my = await mysql.createConnection(mysqlConfig);
    await my.query("SET time_zone = '-06:00'");
    
    console.log('✅ Conexiones establecidas\n');
    console.log('='.repeat(100));
    console.log('DATOS EN SQL SERVER (últimos 5 registros con eventos 47, 48, 49, 26, 33, 42, 36)');
    console.log('='.repeat(100));
    
    const req = new sql.Request(mssqlPool);
    const rs = await req.query(Q_TEST);
    
    if (rs.recordset.length === 0) {
      console.log('⚠️  No se encontraron registros con estos eventos en SQL Server');
    } else {
      for (const row of rs.recordset) {
        console.log(`\nID: ${row.id_referencias} | Ref: ${row.NumeroDeReferencia} | Op: ${row.Operacion === 1 ? 'IMP' : 'EXP'}`);
        console.log('  ENTREGA_FAC:          ', row.ENTREGA_FAC || 'NULL');
        console.log('  FECHA_FAC:            ', row.FECHA_FAC || 'NULL');
        console.log('  ENTREGA_FAC_CLI:      ', row.ENTREGA_FAC_CLI || 'NULL');
        console.log('  ENTREGA_CAPTURA:      ', row.ENTREGA_CAPTURA || 'NULL');
        console.log('  INICIO_CAPTURA:       ', row.INICIO_CAPTURA || 'NULL');
        console.log('  TERMINO_CAPTURA:      ', row.TERMINO_CAPTURA || 'NULL');
        console.log('  PRIMER_RECONOCIMIENTO:', row.PRIMER_RECONOCIMIENTO || 'NULL');
        
        // Comparar con MySQL
        const [mysqlRows] = await my.query(
          'SELECT ENTREGA_FAC, FECHA_FAC, ENTREGA_FAC_CLI, ENTREGA_CAPTURA, INICIO_CAPTURA, TERMINO_CAPTURA, PRIMER_RECONOCIMIENTO FROM general WHERE id_referencias = ?',
          [row.id_referencias]
        );
        
        if (mysqlRows.length > 0) {
          const mysqlRow = mysqlRows[0];
          console.log('\n  📊 COMPARACIÓN CON MYSQL:');
          
          const campos = [
            'ENTREGA_FAC', 'FECHA_FAC', 'ENTREGA_FAC_CLI', 'ENTREGA_CAPTURA',
            'INICIO_CAPTURA', 'TERMINO_CAPTURA', 'PRIMER_RECONOCIMIENTO'
          ];
          
          for (const campo of campos) {
            const sqlValue = row[campo];
            const mysqlValue = mysqlRow[campo];
            
            if (sqlValue && !mysqlValue) {
              console.log(`  ❌ ${campo}: SQL Server tiene valor pero MySQL es NULL`);
            } else if (!sqlValue && mysqlValue) {
              console.log(`  ⚠️  ${campo}: MySQL tiene valor pero SQL Server es NULL`);
            } else if (sqlValue && mysqlValue) {
              const sqlDate = new Date(sqlValue).getTime();
              const mysqlDate = new Date(mysqlValue).getTime();
              const diff = Math.abs(sqlDate - mysqlDate);
              
              if (diff < 1000) {
                console.log(`  ✅ ${campo}: Coincide`);
              } else {
                console.log(`  ⚠️  ${campo}: Diferencia de ${diff/1000} segundos`);
              }
            } else {
              console.log(`  ⚪ ${campo}: Ambos NULL`);
            }
          }
        } else {
          console.log('\n  ❌ Este registro NO existe en MySQL');
        }
        
        console.log('-'.repeat(100));
      }
    }
    
    console.log('\n' + '='.repeat(100));
    console.log('RESUMEN');
    console.log('='.repeat(100));
    console.log(`Total de registros encontrados en SQL Server: ${rs.recordset.length}`);
    
    await my.end();
    await mssqlPool.close();
    console.log('\n✅ PRUEBA COMPLETADA');
    
  } catch (err) {
    console.error('\n❌ ERROR:', err.message);
    console.error(err.stack);
    try { if (my) await my.end(); } catch (e) { }
    try { if (mssqlPool) await mssqlPool.close(); } catch (e) { }
    process.exit(1);
  }
})();
