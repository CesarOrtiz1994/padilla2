// test_conversion_fechas.js - Prueba de conversión de fechas
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

function convertirFecha(fecha) {
  if (!fecha) return null;
  
  const date = new Date(fecha);
  
  const year = date.getFullYear();
  const month = String(date.getMonth() + 1).padStart(2, '0');
  const day = String(date.getDate()).padStart(2, '0');
  const hours = String(date.getHours()).padStart(2, '0');
  const minutes = String(date.getMinutes()).padStart(2, '0');
  const seconds = String(date.getSeconds()).padStart(2, '0');
  
  return `${year}-${month}-${day} ${hours}:${minutes}:${seconds}`;
}

(async () => {
  let mssqlPool, my;
  try {
    console.log('🧪 PRUEBA DE CONVERSIÓN DE FECHAS\n');
    console.log('Conectando...');
    
    mssqlPool = await sql.connect(mssqlConfig);
    my = await mysql.createConnection(mysqlConfig);
    await my.query("SET time_zone = '-06:00'");
    
    console.log('✅ Conexiones establecidas\n');
    
    // Obtener un registro de ejemplo de SQL Server
    const req = new sql.Request(mssqlPool);
    const rs = await req.query(`
      SELECT TOP 1
        r.id_referencias,
        r.NumeroDeReferencia,
        MAX(CASE WHEN r.Operacion = 1 AND b.IdEvento = 48 THEN b.FechaHoraCapturada 
                 WHEN r.Operacion = 2 AND be.IdEvento = 48 THEN be.FechaHoraCapturada END) AS FECHA_FAC
      FROM referencias r
      LEFT JOIN BitacoraEventosImportacion b ON b.Referencia = r.id_referencias
      LEFT JOIN BitacoraEventosExportacion be ON be.Referencia = r.id_referencias
      WHERE r.Cancelada = 0
        AND (
          (r.Operacion = 1 AND b.IdEvento = 48)
          OR
          (r.Operacion = 2 AND be.IdEvento = 48)
        )
      GROUP BY r.id_referencias, r.NumeroDeReferencia
      ORDER BY r.id_referencias DESC
    `);
    
    if (rs.recordset.length === 0) {
      console.log('⚠️  No se encontraron registros con FECHA_FAC');
      await my.end();
      await mssqlPool.close();
      return;
    }
    
    const registro = rs.recordset[0];
    const fechaOriginal = registro.FECHA_FAC;
    const fechaConvertida = convertirFecha(fechaOriginal);
    
    console.log('='.repeat(80));
    console.log('PRUEBA DE CONVERSIÓN');
    console.log('='.repeat(80));
    console.log(`ID Referencia: ${registro.id_referencias}`);
    console.log(`Número: ${registro.NumeroDeReferencia}`);
    console.log(`\nFecha original de SQL Server:`);
    console.log(`  Valor: ${fechaOriginal}`);
    console.log(`  Tipo: ${typeof fechaOriginal}`);
    console.log(`  ISO: ${new Date(fechaOriginal).toISOString()}`);
    console.log(`\nFecha convertida para MySQL:`);
    console.log(`  Valor: ${fechaConvertida}`);
    console.log(`  Tipo: ${typeof fechaConvertida}`);
    
    // Crear tabla temporal para prueba
    console.log(`\n${'='.repeat(80)}`);
    console.log('PRUEBA DE INSERCIÓN EN MYSQL');
    console.log('='.repeat(80));
    
    await my.query(`
      CREATE TEMPORARY TABLE IF NOT EXISTS test_fechas (
        id INT PRIMARY KEY,
        fecha_original DATETIME,
        fecha_convertida DATETIME
      )
    `);
    
    // Insertar con fecha original (sin convertir)
    await my.query(
      'INSERT INTO test_fechas (id, fecha_original) VALUES (?, ?) ON DUPLICATE KEY UPDATE fecha_original = VALUES(fecha_original)',
      [registro.id_referencias, fechaOriginal]
    );
    
    // Insertar con fecha convertida
    await my.query(
      'UPDATE test_fechas SET fecha_convertida = ? WHERE id = ?',
      [fechaConvertida, registro.id_referencias]
    );
    
    // Leer de vuelta
    const [rows] = await my.query('SELECT * FROM test_fechas WHERE id = ?', [registro.id_referencias]);
    
    if (rows.length > 0) {
      const row = rows[0];
      console.log(`\nResultados en MySQL:`);
      console.log(`  Fecha original (sin convertir): ${row.fecha_original}`);
      console.log(`  Fecha convertida:               ${row.fecha_convertida}`);
      
      console.log(`\n${'='.repeat(80)}`);
      console.log('COMPARACIÓN');
      console.log('='.repeat(80));
      
      const sqlServerDate = new Date(fechaOriginal);
      const mysqlOriginal = new Date(row.fecha_original);
      const mysqlConvertida = new Date(row.fecha_convertida);
      
      const diffOriginal = Math.abs(sqlServerDate - mysqlOriginal) / 1000 / 3600;
      const diffConvertida = Math.abs(sqlServerDate - mysqlConvertida) / 1000 / 3600;
      
      console.log(`\nSQL Server:           ${sqlServerDate.toISOString()}`);
      console.log(`MySQL (sin convertir): ${mysqlOriginal.toISOString()} - Diferencia: ${diffOriginal.toFixed(1)} horas`);
      console.log(`MySQL (convertida):    ${mysqlConvertida.toISOString()} - Diferencia: ${diffConvertida.toFixed(1)} horas`);
      
      if (diffOriginal > 1) {
        console.log(`\n❌ SIN CONVERSIÓN: Hay ${diffOriginal.toFixed(1)} horas de diferencia`);
      } else {
        console.log(`\n✅ SIN CONVERSIÓN: Las fechas coinciden`);
      }
      
      if (diffConvertida > 1) {
        console.log(`❌ CON CONVERSIÓN: Hay ${diffConvertida.toFixed(1)} horas de diferencia`);
      } else {
        console.log(`✅ CON CONVERSIÓN: Las fechas coinciden`);
      }
    }
    
    console.log(`\n${'='.repeat(80)}`);
    
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
