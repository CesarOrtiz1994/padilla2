// actualizar_campos.js
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
  host: process.env.MYSQL_HOST,
  user: process.env.MYSQL_USER,
  password: process.env.MYSQL_PASS,
  database: process.env.MYSQL_DB,
  port: Number(process.env.MYSQL_PORT || 3306)
};

// ---------- Query para obtener los eventos ----------
const Q_EVENTOS = `
SELECT
  r.id_referencias,
  MAX(CASE WHEN b.IdEvento = 22 THEN b.FechaHoraCapturada END) AS ENTREGA_GLOSA,
  MAX(CASE WHEN b.IdEvento = 26 THEN b.FechaHoraCapturada END) AS ENTREGA_CAPTURA,
  MAX(CASE WHEN b.IdEvento = 33 THEN b.FechaHoraCapturada END) AS INICIO_CAPTURA,
  MAX(CASE WHEN b.IdEvento = 42 THEN b.FechaHoraCapturada END) AS TERMINO_CAPTURA,
  MAX(CASE WHEN b.IdEvento = 36 THEN b.FechaHoraCapturada END) AS PRIMER_RECONOCIMIENTO
FROM referencias r
LEFT JOIN BitacoraEventosImportacion b ON b.Referencia = r.id_referencias
WHERE r.Cancelada = 0
GROUP BY r.id_referencias
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
        // Construir la consulta de actualización dinámicamente
        let query = "UPDATE general SET ";
        const params = [];
        let tieneValores = false;
        
        // Solo incluir campos que tienen valores
        if (registro.ENTREGA_GLOSA) {
          query += "ENTREGA_GLOSA = ?, ";
          params.push(registro.ENTREGA_GLOSA);
          tieneValores = true;
        }
        
        if (registro.ENTREGA_CAPTURA) {
          query += "ENTREGA_CAPTURA = ?, ";
          params.push(registro.ENTREGA_CAPTURA);
          tieneValores = true;
        }
        
        if (registro.INICIO_CAPTURA) {
          query += "INICIO_CAPTURA = ?, ";
          params.push(registro.INICIO_CAPTURA);
          tieneValores = true;
        }
        
        if (registro.TERMINO_CAPTURA) {
          query += "TERMINO_CAPTURA = ?, ";
          params.push(registro.TERMINO_CAPTURA);
          tieneValores = true;
        }
        
        if (registro.PRIMER_RECONOCIMIENTO) {
          query += "PRIMER_RECONOCIMIENTO = ?, ";
          params.push(registro.PRIMER_RECONOCIMIENTO);
          tieneValores = true;
        }
        
        // Si no hay valores para actualizar, saltar este registro
        if (!tieneValores) {
          sinCambios++;
          continue;
        }
        
        // Eliminar la última coma y espacio
        query = query.slice(0, -2);
        
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
    
    console.log('Obteniendo datos de eventos...');
    const req = new sql.Request(mssqlPool);
    const rs = await req.query(Q_EVENTOS);
    const rows = rs.recordset;
    
    console.log(`Se encontraron ${rows.length} registros con eventos.`);
    
    // Filtrar registros que tienen al menos un evento
    const registrosConEventos = rows.filter(r => 
      r.ENTREGA_GLOSA || r.ENTREGA_CAPTURA || r.INICIO_CAPTURA || 
      r.TERMINO_CAPTURA || r.PRIMER_RECONOCIMIENTO
    );
    
    console.log(`De los cuales ${registrosConEventos.length} tienen al menos uno de los eventos buscados.`);
    
    console.log('\n===== INICIANDO ACTUALIZACIÓN DE REGISTROS =====');
    console.log(`Fecha y hora: ${new Date().toISOString()}`);
    
    // Actualizar los registros en MySQL
    const resultado = await actualizarPorLotes(my, registrosConEventos);
    
    console.log('\n\n===== RESUMEN FINAL =====');
    console.log(`⏰ Fecha y hora de finalización: ${new Date().toISOString()}`);
    console.log(`⏱ Tiempo total de ejecución: ${resultado.tiempoTotal.toFixed(2)} segundos`);
    console.log(`📈 Estadísticas:`);
    console.log(`   - Total de registros procesados: ${registrosConEventos.length}`);
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
