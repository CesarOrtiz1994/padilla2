// actualizar_campos.js - Actualización de Clave_pedimento
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
  requestTimeout:    Number(process.env.MSSQL_REQUEST_TIMEOUT_MS || 300000),

  pool: { max: 5, min: 0, idleTimeoutMillis: 30000 },

  options: {
    encrypt: true,               // si usas Azure SQL => true
    trustServerCertificate: true // útil on-prem sin CA
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

// ---------- Query para obtener Clave_pedimento ----------
const Q_REGIMEN = `
SELECT
  r.id_referencias,
  re.regimen AS Clave_pedimento
FROM referencias r
LEFT JOIN regimen re ON re.id_regimen = r.id_regimen
WHERE r.Cancelada = 0
  AND re.regimen IS NOT NULL
`;

// ---------- Función para actualizar por lotes ----------
async function actualizarPorLotes(conn, datos, tamanoLote = 100) {
  console.log(`\n===== INICIANDO ACTUALIZACIÓN =====`);
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
      try {
        // Construir la consulta de actualización para Clave_pedimento
        let query = "UPDATE general SET Clave_pedimento = ?";
        const params = [registro.Clave_pedimento];
        let tieneValores = registro.Clave_pedimento != null;
        
        // Si no hay valores para actualizar, saltar este registro
        if (!tieneValores) {
          sinCambios++;
          continue;
        }
        
        // Agregar la condición WHERE
        query += " WHERE id_referencias = ?";
        params.push(registro.id_referencias);
        
        // Ejecutar la actualización
        const [result] = await conn.query(query, params);
        
        if (result.affectedRows > 0) {
          actualizados++;
        } else {
          sinCambios++;
        }
      } catch (err) {
        console.error(`❌ Error al actualizar registro ${registro.id_referencias}:`, err.message);
        errores++;
        erroresDetalle.push({
          id_referencias: registro.id_referencias,
          error: err.message,
          query: query,
          params: params
        });
      }
    }
    
    // Mostrar progreso después de cada lote
    const loteActual = Math.floor(i / tamanoLote) + 1;
    const totalLotes = Math.ceil(datos.length / tamanoLote);
    const porcentaje = Math.round((loteActual / totalLotes) * 100);
    const tiempoTranscurrido = (Date.now() - tiempoInicio) / 1000;
    
    console.log(`✅ Lote ${loteActual}/${totalLotes} completado (${porcentaje}%)`);  
    console.log(`   Tiempo transcurrido: ${tiempoTranscurrido.toFixed(2)} segundos`);  
    console.log(`   Actualizados hasta ahora: ${actualizados}, Sin cambios: ${sinCambios}, Errores: ${errores}\n`);  
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
    await my.query("SET time_zone = '+00:00'"); // evitar sorpresas de TZ
    
    console.log('Obteniendo datos de regimen...');
    const req = new sql.Request(mssqlPool);
    const rs = await req.query(Q_REGIMEN);
    const rows = rs.recordset;
    
    console.log(`Se encontraron ${rows.length} registros con información de régimen.`);
    
    // Filtrar registros que tienen valor de Clave_pedimento
    const registrosConRegimen = rows.filter(r => r.Clave_pedimento != null);
    
    console.log(`De los cuales ${registrosConRegimen.length} tienen un valor válido para Clave_pedimento.`);
    
    console.log('\n===== INICIANDO ACTUALIZACIÓN DE REGISTROS =====');
    console.log(`Fecha y hora: ${new Date().toISOString()}`);
    
    // Actualizar los registros en MySQL
    const resultado = await actualizarPorLotes(my, registrosConRegimen);
    
    console.log('\n\n===== RESUMEN FINAL =====');
    console.log(`⏰ Fecha y hora de finalización: ${new Date().toISOString()}`);
    console.log(`⏱ Tiempo total de ejecución: ${resultado.tiempoTotal.toFixed(2)} segundos`);
    console.log(`📈 Estadísticas:`);
    console.log(`   - Total de registros procesados: ${registrosConRegimen.length}`);
    console.log(`   - Registros actualizados: ${resultado.actualizados}`);
    console.log(`   - Registros sin cambios: ${resultado.sinCambios}`);
    console.log(`   - Errores: ${resultado.errores}`);
    
    // Mostrar detalles de errores si hay alguno
    if (resultado.errores > 0) {
      console.log('\n⚠️ DETALLE DE ERRORES:');
      resultado.erroresDetalle.forEach((err, index) => {
        console.log(`\nError #${index + 1}:`);
        console.log(`   ID Referencia: ${err.id_referencias}`);
        console.log(`   Mensaje: ${err.error}`);
        console.log(`   Query: ${err.query}`);
        console.log(`   Parámetros: ${JSON.stringify(err.params)}`);
      });
    }
    
    // Cierres
    await my.end();
    await mssqlPool.close();
    console.log('\n✅ PROCESO COMPLETADO.');
    
  } catch (err) {
    console.error('\n\n❌ ERROR FATAL EN EL PROCESO:');
    console.error(`Fecha y hora: ${new Date().toISOString()}`);
    console.error(`Mensaje: ${err.message}`);
    console.error(`Stack: ${err.stack}`);
    
    // Intentar cerrar las conexiones
    console.log('\nCerrando conexiones...');
    try { 
      if (my) {
        await my.end();
        console.log('- Conexión MySQL cerrada correctamente');
      }
    } catch (e) { 
      console.error('- Error al cerrar conexión MySQL:', e.message);
    }
    
    try { 
      if (mssqlPool) {
        await mssqlPool.close();
        console.log('- Conexión MSSQL cerrada correctamente');
      }
    } catch (e) { 
      console.error('- Error al cerrar conexión MSSQL:', e.message);
    }
    
    console.error('\n❌ PROCESO TERMINADO CON ERRORES');
    process.exit(1);
  }
})();
