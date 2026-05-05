// test_suma_6horas.js - Prueba de suma de 6 horas
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

function sumar6Horas(fecha) {
  if (!fecha) return null;
  
  const date = new Date(fecha);
  // Sumar 6 horas (6 * 60 * 60 * 1000 milisegundos)
  date.setTime(date.getTime() + (6 * 60 * 60 * 1000));
  
  return date;
}

(async () => {
  let mssqlPool, my;
  try {
    console.log('🧪 PRUEBA DE SUMA DE 6 HORAS\n');
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
    const fechaSumada = sumar6Horas(fechaOriginal);
    
    console.log('='.repeat(80));
    console.log('PRUEBA DE SUMA DE 6 HORAS');
    console.log('='.repeat(80));
    console.log(`ID Referencia: ${registro.id_referencias}`);
    console.log(`Número: ${registro.NumeroDeReferencia}`);
    console.log(`\nFecha original de SQL Server:`);
    console.log(`  ${fechaOriginal}`);
    console.log(`\nFecha después de sumar 6 horas:`);
    console.log(`  ${fechaSumada}`);
    
    // Crear tabla temporal para prueba
    console.log(`\n${'='.repeat(80)}`);
    console.log('PRUEBA DE INSERCIÓN EN MYSQL');
    console.log('='.repeat(80));
    
    await my.query(`
      CREATE TEMPORARY TABLE IF NOT EXISTS test_suma_fechas (
        id INT PRIMARY KEY,
        fecha_sin_sumar DATETIME,
        fecha_con_suma DATETIME
      )
    `);
    
    // Insertar sin sumar
    await my.query(
      'INSERT INTO test_suma_fechas (id, fecha_sin_sumar) VALUES (?, ?) ON DUPLICATE KEY UPDATE fecha_sin_sumar = VALUES(fecha_sin_sumar)',
      [registro.id_referencias, fechaOriginal]
    );
    
    // Insertar con suma
    await my.query(
      'UPDATE test_suma_fechas SET fecha_con_suma = ? WHERE id = ?',
      [fechaSumada, registro.id_referencias]
    );
    
    // Leer de vuelta
    const [rows] = await my.query('SELECT * FROM test_suma_fechas WHERE id = ?', [registro.id_referencias]);
    
    if (rows.length > 0) {
      const row = rows[0];
      console.log(`\nResultados en MySQL:`);
      console.log(`  Sin sumar 6 horas: ${row.fecha_sin_sumar}`);
      console.log(`  Con suma 6 horas:  ${row.fecha_con_suma}`);
      
      console.log(`\n${'='.repeat(80)}`);
      console.log('COMPARACIÓN CON FECHA ORIGINAL');
      console.log('='.repeat(80));
      
      const sqlServerDate = new Date(fechaOriginal);
      const mysqlSinSumar = new Date(row.fecha_sin_sumar);
      const mysqlConSuma = new Date(row.fecha_con_suma);
      
      console.log(`\nSQL Server (original):     ${sqlServerDate.toLocaleString('es-MX', { timeZone: 'America/Mexico_City' })}`);
      console.log(`MySQL (sin sumar):         ${mysqlSinSumar.toLocaleString('es-MX', { timeZone: 'America/Mexico_City' })}`);
      console.log(`MySQL (con suma 6 horas):  ${mysqlConSuma.toLocaleString('es-MX', { timeZone: 'America/Mexico_City' })}`);
      
      const diffSinSumar = Math.abs(sqlServerDate - mysqlSinSumar) / 1000 / 3600;
      const diffConSuma = Math.abs(sqlServerDate - mysqlConSuma) / 1000 / 3600;
      
      console.log(`\n${'='.repeat(80)}`);
      console.log('RESULTADO');
      console.log('='.repeat(80));
      
      if (diffSinSumar > 1) {
        console.log(`❌ SIN SUMAR: Diferencia de ${diffSinSumar.toFixed(1)} horas`);
      } else {
        console.log(`✅ SIN SUMAR: Las fechas coinciden (diferencia < 1 hora)`);
      }
      
      if (diffConSuma > 1) {
        console.log(`❌ CON SUMA: Diferencia de ${diffConSuma.toFixed(1)} horas`);
      } else {
        console.log(`✅ CON SUMA: Las fechas coinciden (diferencia < 1 hora)`);
      }
      
      console.log(`\n${'='.repeat(80)}`);
      if (diffConSuma < 1 && diffSinSumar > 1) {
        console.log('✅ ¡LA SUMA DE 6 HORAS FUNCIONA CORRECTAMENTE!');
      } else if (diffSinSumar < 1) {
        console.log('⚠️  Las fechas ya coinciden sin sumar (puede que ya estén corregidas)');
      } else {
        console.log('❌ La suma de 6 horas no resuelve el problema');
      }
      console.log('='.repeat(80));
    }
    
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
