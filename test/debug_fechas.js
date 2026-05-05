// debug_fechas.js - Debug para ver cómo vienen las fechas
require('dotenv').config();
const sql = require('mssql');

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

(async () => {
  let mssqlPool;
  try {
    console.log('🔍 DEBUG DE FECHAS\n');
    
    mssqlPool = await sql.connect(mssqlConfig);
    
    const req = new sql.Request(mssqlPool);
    const rs = await req.query(`
      SELECT TOP 1
        r.id_referencias,
        r.NumeroDeReferencia,
        b.FechaHoraCapturada,
        CONVERT(VARCHAR(23), b.FechaHoraCapturada, 121) AS FechaString
      FROM referencias r
      LEFT JOIN BitacoraEventosImportacion b ON b.Referencia = r.id_referencias
      WHERE r.Cancelada = 0 AND b.IdEvento = 48 AND b.FechaHoraCapturada IS NOT NULL
      ORDER BY r.id_referencias DESC
    `);
    
    if (rs.recordset.length === 0) {
      console.log('⚠️  No se encontraron registros');
      await mssqlPool.close();
      return;
    }
    
    const row = rs.recordset[0];
    
    console.log('='.repeat(80));
    console.log('FECHA DESDE SQL SERVER');
    console.log('='.repeat(80));
    console.log(`ID: ${row.id_referencias}`);
    console.log(`Ref: ${row.NumeroDeReferencia}`);
    console.log(`\nFechaHoraCapturada (objeto Date):`);
    console.log(`  Valor: ${row.FechaHoraCapturada}`);
    console.log(`  Tipo: ${typeof row.FechaHoraCapturada}`);
    console.log(`  Constructor: ${row.FechaHoraCapturada.constructor.name}`);
    console.log(`  ISO: ${row.FechaHoraCapturada.toISOString()}`);
    console.log(`  LocaleString: ${row.FechaHoraCapturada.toLocaleString('es-MX', { timeZone: 'America/Mexico_City' })}`);
    console.log(`\nFechaString (CONVERT VARCHAR):`);
    console.log(`  Valor: ${row.FechaString}`);
    console.log(`  Tipo: ${typeof row.FechaString}`);
    
    console.log(`\n${'='.repeat(80)}`);
    console.log('CONVERSIONES EN JAVASCRIPT');
    console.log('='.repeat(80));
    
    const date = new Date(row.FechaHoraCapturada);
    console.log(`\nnew Date(FechaHoraCapturada):`);
    console.log(`  toString: ${date.toString()}`);
    console.log(`  toISOString: ${date.toISOString()}`);
    console.log(`  getTime: ${date.getTime()}`);
    console.log(`  getTimezoneOffset: ${date.getTimezoneOffset()} minutos`);
    
    // Probar diferentes formas de formatear
    const year = date.getFullYear();
    const month = String(date.getMonth() + 1).padStart(2, '0');
    const day = String(date.getDate()).padStart(2, '0');
    const hours = String(date.getHours()).padStart(2, '0');
    const minutes = String(date.getMinutes()).padStart(2, '0');
    const seconds = String(date.getSeconds()).padStart(2, '0');
    
    const formatted = `${year}-${month}-${day} ${hours}:${minutes}:${seconds}`;
    console.log(`\nFormato manual: ${formatted}`);
    console.log(`String directo de SQL: ${row.FechaString}`);
    
    console.log(`\n${'='.repeat(80)}`);
    console.log('ZONA HORARIA DEL SISTEMA');
    console.log('='.repeat(80));
    console.log(`TZ env: ${process.env.TZ || 'no definido'}`);
    console.log(`Intl.DateTimeFormat().resolvedOptions().timeZone: ${Intl.DateTimeFormat().resolvedOptions().timeZone}`);
    
    await mssqlPool.close();
    console.log('\n✅ DEBUG COMPLETADO');
    
  } catch (err) {
    console.error('\n❌ ERROR:', err.message);
    console.error(err.stack);
    try { if (mssqlPool) await mssqlPool.close(); } catch (e) { }
    process.exit(1);
  }
})();
