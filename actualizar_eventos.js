// actualizar_eventos.js - Actualización de eventos de BitacoraEventosImportacion y BitacoraEventosExportacion
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

// ---------- Query para obtener eventos ----------
const Q_EVENTOS = `
SELECT
  r.id_referencias,
  MAX(CASE 
      WHEN r.Operacion = 1 AND b.IdEvento = 6 THEN b.FechaHoraCapturada 
      WHEN r.Operacion = 2 AND be.IdEvento = 6 THEN be.FechaHoraCapturada 
  END) AS LLEGADA_MERCAN,
  MAX(CASE 
      WHEN r.Operacion = 1 AND b.IdEvento = 18 THEN b.FechaHoraCapturada 
      WHEN r.Operacion = 2 AND be.IdEvento = 18 THEN be.FechaHoraCapturada 
  END) AS ENTREGA_CLASIFICA,
  MAX(CASE 
      WHEN r.Operacion = 1 AND b.IdEvento = 19 THEN b.FechaHoraCapturada 
      WHEN r.Operacion = 2 AND be.IdEvento = 19 THEN be.FechaHoraCapturada 
  END) AS INICIO_CLASIFICA,
  MAX(CASE 
      WHEN r.Operacion = 1 AND b.IdEvento = 20 THEN b.FechaHoraCapturada 
      WHEN r.Operacion = 2 AND be.IdEvento = 20 THEN be.FechaHoraCapturada 
  END) AS TERMINO_CLASIFICA,
  MAX(CASE 
      WHEN r.Operacion = 1 AND b.IdEvento = 69 THEN b.FechaHoraCapturada 
      WHEN r.Operacion = 2 AND be.IdEvento = 69 THEN be.FechaHoraCapturada 
  END) AS INICIO_GLOSA,
  MAX(CASE 
      WHEN r.Operacion = 1 AND b.IdEvento = 70 THEN b.FechaHoraCapturada 
      WHEN r.Operacion = 2 AND be.IdEvento = 70 THEN be.FechaHoraCapturada 
  END) AS TERMINO_GLOSA,
  MAX(CASE 
      WHEN r.Operacion = 1 AND b.IdEvento = 22 THEN b.FechaHoraCapturada 
      WHEN r.Operacion = 2 AND be.IdEvento = 22 THEN be.FechaHoraCapturada 
  END) AS ENTREGA_GLOSA,
  MAX(CASE 
      WHEN r.Operacion = 1 AND b.IdEvento = 29 THEN b.FechaHoraCapturada 
      WHEN r.Operacion = 2 AND be.IdEvento = 29 THEN be.FechaHoraCapturada 
  END) AS PAGO_PEDIMENTO,
  MAX(CASE 
      WHEN r.Operacion = 1 AND b.IdEvento = 32 THEN b.FechaHoraCapturada 
      WHEN r.Operacion = 2 AND be.IdEvento = 32 THEN be.FechaHoraCapturada 
  END) AS DESPACHO_MERCAN,
  MAX(CASE 
      WHEN r.Operacion = 1 AND b.IdEvento = 47 THEN b.FechaHoraCapturada 
      WHEN r.Operacion = 2 AND be.IdEvento = 47 THEN be.FechaHoraCapturada 
  END) AS ENTREGA_FAC,
  MAX(CASE 
      WHEN r.Operacion = 1 AND b.IdEvento = 48 THEN b.FechaHoraCapturada 
      WHEN r.Operacion = 2 AND be.IdEvento = 48 THEN be.FechaHoraCapturada 
  END) AS FECHA_FAC,
  MAX(CASE 
      WHEN r.Operacion = 1 AND b.IdEvento = 49 THEN b.FechaHoraCapturada 
      WHEN r.Operacion = 2 AND be.IdEvento = 49 THEN be.FechaHoraCapturada 
  END) AS ENTREGA_FAC_CLI,
  MAX(CASE 
      WHEN r.Operacion = 1 AND b.IdEvento = 26 THEN b.FechaHoraCapturada 
      WHEN r.Operacion = 2 AND be.IdEvento = 26 THEN be.FechaHoraCapturada 
  END) AS ENTREGA_CAPTURA,
  MAX(CASE 
      WHEN r.Operacion = 1 AND b.IdEvento = 33 THEN b.FechaHoraCapturada 
      WHEN r.Operacion = 2 AND be.IdEvento = 33 THEN be.FechaHoraCapturada 
  END) AS INICIO_CAPTURA,
  MAX(CASE 
      WHEN r.Operacion = 1 AND b.IdEvento = 42 THEN b.FechaHoraCapturada 
      WHEN r.Operacion = 2 AND be.IdEvento = 42 THEN be.FechaHoraCapturada 
  END) AS TERMINO_CAPTURA,
  MAX(CASE 
      WHEN r.Operacion = 1 AND b.IdEvento = 36 THEN b.FechaHoraCapturada 
      WHEN r.Operacion = 2 AND be.IdEvento = 36 THEN be.FechaHoraCapturada 
  END) AS PRIMER_RECONOCIMIENTO
FROM referencias r
LEFT JOIN BitacoraEventosImportacion b ON b.Referencia = r.id_referencias
LEFT JOIN BitacoraEventosExportacion be ON be.Referencia = r.id_referencias
WHERE r.Cancelada = 0
GROUP BY r.id_referencias
`;

// ---------- Query para verificar registros existentes en MySQL ----------
const Q_EXISTENTES = `
SELECT id_referencias FROM general
`;

// ---------- Función para sumar 6 horas a la fecha ----------
function sumar6Horas(fecha) {
  if (!fecha) return null;
  
  const date = new Date(fecha);
  // Sumar 6 horas (6 * 60 * 60 * 1000 milisegundos)
  date.setTime(date.getTime() + (6 * 60 * 60 * 1000));
  
  return date;
}

// ---------- Función para actualizar por lotes ----------
async function actualizarPorLotes(conn, datos, tamanoLote = 100) {
  console.log(`\n===== INICIANDO ACTUALIZACIÓN DE EVENTOS =====`);
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
        // Construir la consulta de actualización para los eventos
        query = "UPDATE general SET ";
        params = [];
        let tieneValores = false;
        
        // Agregar cada campo que no sea null
        const camposEventos = [
          'LLEGADA_MERCAN', 'ENTREGA_CLASIFICA', 'INICIO_CLASIFICA', 'TERMINO_CLASIFICA',
          'INICIO_GLOSA', 'TERMINO_GLOSA', 'ENTREGA_GLOSA', 'PAGO_PEDIMENTO', 'DESPACHO_MERCAN',
          'ENTREGA_FAC', 'FECHA_FAC', 'ENTREGA_FAC_CLI', 'ENTREGA_CAPTURA', 'INICIO_CAPTURA',
          'TERMINO_CAPTURA', 'PRIMER_RECONOCIMIENTO'
        ];
        
        const actualizaciones = [];
        
        for (const campo of camposEventos) {
          if (registro[campo] !== null && registro[campo] !== undefined) {
            actualizaciones.push(`${campo} = ?`);
            // Sumar 6 horas antes de insertar
            params.push(sumar6Horas(registro[campo]));
            tieneValores = true;
          }
        }
        
        // Si no hay valores para actualizar, saltar este registro
        if (!tieneValores) {
          sinCambios++;
          continue;
        }
        
        // Completar la consulta
        query += actualizaciones.join(', ');
        
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
    await my.query("SET time_zone = '-06:00'"); // America/Mexico_City (UTC-6)
    
    console.log('Obteniendo datos de eventos...');
    const req = new sql.Request(mssqlPool);
    const rs = await req.query(Q_EVENTOS);
    const rows = rs.recordset;
    
    console.log(`Se encontraron ${rows.length} registros con información de eventos.`);
    
    // Obtener IDs de registros existentes en MySQL
    console.log('Obteniendo IDs de registros existentes en MySQL...');
    const [existentesResult] = await my.query(Q_EXISTENTES);
    const idsExistentes = new Set(existentesResult.map(r => r.id_referencias));
    
    console.log(`Se encontraron ${idsExistentes.size} registros existentes en MySQL.`);
    
    // Filtrar registros que existen en MySQL
    const registrosExistentes = rows.filter(r => idsExistentes.has(r.id_referencias));
    
    console.log(`De los cuales ${registrosExistentes.length} existen en la base de datos MySQL.`);
    
    console.log('\n===== INICIANDO ACTUALIZACIÓN DE REGISTROS =====');
    console.log(`Fecha y hora: ${new Date().toISOString()}`);
    
    // Actualizar los registros en MySQL
    const resultado = await actualizarPorLotes(my, registrosExistentes);
    
    console.log('\n\n===== RESUMEN FINAL =====');
    console.log(`⏰ Fecha y hora de finalización: ${new Date().toISOString()}`);
    console.log(`⏱ Tiempo total de ejecución: ${resultado.tiempoTotal.toFixed(2)} segundos`);
    console.log(`📈 Estadísticas:`);
    console.log(`   - Total de registros procesados: ${registrosExistentes.length}`);
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
