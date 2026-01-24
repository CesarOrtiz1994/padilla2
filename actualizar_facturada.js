// actualizar_facturada.js - Actualización únicamente del campo facturada en la tabla general
require('dotenv').config();
const sql = require('mssql');
const mysql = require('mysql2/promise');

// ---------- Config de conexiones ----------
const mssqlConfig = {
  server: process.env.MSSQL_SERVER,
  database: process.env.MSSQL_DB,
  user: process.env.MSSQL_USER,
  password: process.env.MSSQL_PASS,
  port: Number(process.env.MSSQL_PORT || 1433),

  connectionTimeout: Number(process.env.MSSQL_CONN_TIMEOUT_MS || 30000),
  requestTimeout: Number(process.env.MSSQL_REQUEST_TIMEOUT_MS || 300000),

  pool: { max: 5, min: 0, idleTimeoutMillis: 30000 },

  options: {
    encrypt: true,
    trustServerCertificate: true
  }
};

// ----- MySQL conn -----
const mysqlConfig = {
  host: process.env.MYSQL_HOST1,
  user: process.env.MYSQL_USER1,
  password: process.env.MYSQL_PASS1,
  database: process.env.MYSQL_DB1,
  port: Number(process.env.MYSQL_PORT1 || 3306)
};

// ---------- Query para obtener facturada ----------
const Q_FACTURADA = `
SELECT
  r.id_referencias,
  r.facturada AS facturada
FROM referencias r
WHERE r.id_referencias IS NOT NULL
`;

function normalizeFacturada(value) {
  if (value === null || value === undefined) return null;
  if (typeof value === 'boolean') return value ? 1 : 0;
  if (typeof value === 'number') return value ? 1 : 0;
  if (typeof value === 'string') {
    const v = value.trim().toLowerCase();
    if (v === '1' || v === 'true' || v === 'si' || v === 'sí' || v === 'yes') return 1;
    if (v === '0' || v === 'false' || v === 'no') return 0;
  }
  // fallback: MySQL TINYINT(1) suele aceptar 0/1, para valores raros guardamos null
  const n = Number(value);
  if (!Number.isNaN(n)) return n ? 1 : 0;
  return null;
}

// ---------- Función para actualizar por lotes ----------
async function actualizarPorLotes(conn, datos, tamanoLote = 200) {
  console.log(`\n===== INICIANDO ACTUALIZACIÓN DE FACTURADA =====`);
  console.log(`Fecha y hora de inicio: ${new Date().toISOString()}`);
  console.log(`Total de registros a procesar: ${datos.length}`);
  console.log(`Tamaño de lote: ${tamanoLote}`);
  console.log(`Total de lotes: ${Math.ceil(datos.length / tamanoLote)}`);
  console.log(`==============================\n`);

  const tiempoInicio = Date.now();
  let actualizados = 0;
  let sinCambios = 0;
  let errores = 0;
  let erroresDetalle = [];

  for (let i = 0; i < datos.length; i += tamanoLote) {
    const lote = datos.slice(i, i + tamanoLote);
    console.log(`Procesando lote ${Math.floor(i / tamanoLote) + 1}/${Math.ceil(datos.length / tamanoLote)} (${lote.length} registros)`);

    for (const registro of lote) {
      let query;
      let params;
      try {
        query = 'UPDATE general SET facturada = ? WHERE id_referencias = ?';
        params = [normalizeFacturada(registro.facturada), registro.id_referencias];

        const [result] = await conn.query(query, params);
        if (result.affectedRows > 0) {
          actualizados++;
        } else {
          sinCambios++;
        }
      } catch (err) {
        console.error(`❌ Error al actualizar id_referencias=${registro.id_referencias}:`, err.message);
        errores++;
        erroresDetalle.push({
          id_referencias: registro.id_referencias,
          error: err.message,
          query,
          params
        });
      }
    }

    const loteActual = Math.floor(i / tamanoLote) + 1;
    const totalLotes = Math.ceil(datos.length / tamanoLote);
    const porcentaje = Math.round((loteActual / totalLotes) * 100);
    const tiempoTranscurrido = (Date.now() - tiempoInicio) / 1000;

    console.log(`✅ Lote ${loteActual}/${totalLotes} completado (${porcentaje}%)`);
    console.log(`   Tiempo transcurrido: ${tiempoTranscurrido.toFixed(2)} segundos`);
    console.log(`   Actualizados: ${actualizados}, Sin cambios: ${sinCambios}, Errores: ${errores}\n`);
  }

  const tiempoTotal = (Date.now() - tiempoInicio) / 1000;
  return { actualizados, sinCambios, errores, tiempoTotal, erroresDetalle };
}

// ---------- Runner principal ----------
(async () => {
  let mssqlPool, my;
  try {
    console.log('Conectando a las bases de datos...');
    mssqlPool = await sql.connect(mssqlConfig);
    my = await mysql.createConnection(mysqlConfig);
    await my.query("SET time_zone = '-06:00'");

    console.log('Obteniendo facturada desde SQL Server...');
    const req = new sql.Request(mssqlPool);
    const rs = await req.query(Q_FACTURADA);
    const rows = rs.recordset || [];

    console.log(`Se encontraron ${rows.length} registros.`);

    const resultado = await actualizarPorLotes(my, rows);

    console.log('\n\n===== RESUMEN FINAL =====');
    console.log(`⏰ Fecha y hora de finalización: ${new Date().toISOString()}`);
    console.log(`⏱ Tiempo total de ejecución: ${resultado.tiempoTotal.toFixed(2)} segundos`);
    console.log('📈 Estadísticas:');
    console.log(`   - Total procesados: ${rows.length}`);
    console.log(`   - Actualizados: ${resultado.actualizados}`);
    console.log(`   - Sin cambios (o no existe en MySQL): ${resultado.sinCambios}`);
    console.log(`   - Errores: ${resultado.errores}`);

    if (resultado.errores > 0) {
      console.log('\n⚠️ DETALLE DE ERRORES (primeros 10):');
      resultado.erroresDetalle.slice(0, 10).forEach((e, idx) => {
        console.log(`\nError #${idx + 1}:`);
        console.log(`   ID Referencia: ${e.id_referencias}`);
        console.log(`   Mensaje: ${e.error}`);
      });
    }

    await my.end();
    await mssqlPool.close();
    console.log('\n✅ PROCESO COMPLETADO.');
  } catch (err) {
    console.error('\n\n❌ ERROR FATAL EN EL PROCESO:');
    console.error(`Fecha y hora: ${new Date().toISOString()}`);
    console.error(`Mensaje: ${err.message}`);
    console.error(`Stack: ${err.stack}`);

    console.log('\nCerrando conexiones...');
    try { if (my) await my.end(); } catch (e) { }
    try { if (mssqlPool) await mssqlPool.close(); } catch (e) { }

    console.error('\n❌ PROCESO TERMINADO CON ERRORES');
    process.exit(1);
  }
})();
