// actualizar_canceladas.js - Actualización de la columna Cancelada en registros históricos
require('dotenv').config();
const sql = require('mssql');
const mysql = require('mysql2/promise');

// Función para crear una barra de progreso simple
function crearBarraProgreso(porcentaje, longitud = 30) {
  const completado = Math.floor(porcentaje * longitud / 100);
  const restante = longitud - completado;
  const barraCompletada = '█'.repeat(completado);
  const barraRestante = '░'.repeat(restante);
  return `[${barraCompletada}${barraRestante}] ${porcentaje.toFixed(1)}%`;
}

// Función para formatear tiempo en formato legible
function formatearTiempo(segundos) {
  if (segundos < 60) return `${segundos.toFixed(1)} segundos`;
  const minutos = Math.floor(segundos / 60);
  const segs = Math.floor(segundos % 60);
  return `${minutos} min ${segs} seg`;
}

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

// ---------- Query para obtener estado de Cancelada y toda la información necesaria ----------
const Q_COMPLETO = `
SELECT
  r.id_referencias,
  r.NumeroDeReferencia,
  r.Cancelada,
  p.Pedimento,
  r.Operacion,
  re.regimen            AS Clave_pedimento,
  a_origen.descripcion  AS a_despacho,
  a_llegada.descripcion AS a_llegada,
  c_i.nombre            AS C_Imp_Exp,
  r.facturada           AS facturada,
  c_f.nombre            AS Facturar_a,
  aa.nombre             AS Agente_Aduanal,
  u.nombre              AS Ejecutivo,
  mt.descripcion        AS medio_trasporte,
  r.FechaApertura       AS APERTURA,
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
  END) AS PRIMER_RECONOCIMIENTO,
  MAX(p.ADV1)           AS Total_Adv,
  MAX(p.DTA1)           AS Total_DTA,
  MAX(p.IVA1)           AS Total_IVA,
  MAX(p.TOTALIMPUESTOS) AS Total_Imp
FROM referencias r
LEFT JOIN PedimentosEncabezado p ON p.id_referencia = r.id_referencias
LEFT JOIN regimen re ON re.id_regimen = r.id_regimen
LEFT JOIN aduana a_origen ON a_origen.id_Aduana = r.id_aduana
LEFT JOIN aduana a_llegada ON a_llegada.id_Aduana = r.Id_AduanaLlegada
LEFT JOIN clientes c_i ON c_i.id_cliente = r.id_cliente
LEFT JOIN clientes c_f ON c_f.id_cliente = r.concargo
LEFT JOIN agentesaduanales aa ON aa.id_agenteaduanal = r.id_agenteaduanal
LEFT JOIN usuarios u ON u.id_usuario = r.IdEjecutivo
LEFT JOIN MediosDeTransporte mt ON mt.IDMedioDeTransporte = p.IDTransporteEnt_Sal
LEFT JOIN BitacoraEventosImportacion b ON b.Referencia = r.id_referencias
LEFT JOIN BitacoraEventosExportacion be ON be.Referencia = r.id_referencias
GROUP BY
  r.id_referencias, r.NumeroDeReferencia, r.Cancelada, p.Pedimento, r.Operacion, re.regimen,
  a_origen.descripcion, a_llegada.descripcion, c_i.nombre, r.facturada, c_f.nombre,
  aa.nombre, u.nombre, r.FechaApertura
`;

// ---------- Query para verificar registros existentes en MySQL ----------
const Q_EXISTENTES = `
SELECT id_referencias FROM general
`;

// ---------- Función para sumar 6 horas a la fecha ----------
function sumar6Horas(fecha) {
  if (!fecha) return null;
  
  const date = new Date(fecha);
  // Descartar fechas inválidas tipo 1899-12-31
  if (date.getFullYear() <= 1900) return null;
  // Sumar 6 horas (6 * 60 * 60 * 1000 milisegundos)
  date.setTime(date.getTime() + (6 * 60 * 60 * 1000));
  
  return date;
}

// Función para convertir valores money a números para MySQL
function safeMoneyValue(value, fieldName = 'desconocido') {
  try {
    // Caso especial: si es null o undefined
    if (value === null || value === undefined) {
      return null; // Mantener null para preservar la semántica original
    }
    
    // Si es un objeto (como en algunos resultados de SQL Server)
    if (typeof value === 'object') {
      // Intentar extraer un valor numérico si existe
      if (value.value !== undefined) {
        value = value.value;
      } else {
        return null;
      }
    }
    
    // Convertir a número si es string
    let numValue;
    if (typeof value === 'string') {
      // Eliminar caracteres no numéricos excepto punto decimal y signo negativo
      const cleanValue = value.replace(/[^\d.-]/g, '');
      numValue = parseFloat(cleanValue);
    } else {
      numValue = Number(value);
    }
    
    // Verificar si es un número válido
    if (isNaN(numValue)) {
      return null;
    }
    
    // Devolver el valor original sin modificar
    return numValue;
  } catch (err) {
    return null; // Valor por defecto en caso de error
  }
}

// ---------- Función para actualizar por lotes ----------
async function actualizarPorLotes(conn, datosExistentes, datosNuevos, tamanoLote = 100) {
  console.log(`\n===== INICIANDO ACTUALIZACIÓN DE CANCELADAS =====`);
  console.log(`Fecha y hora de inicio: ${new Date().toISOString()}`);
  console.log(`Total de registros existentes a actualizar: ${datosExistentes.length}`);
  console.log(`Total de registros nuevos a insertar: ${datosNuevos.length}`);
  console.log(`Tamaño de lote: ${tamanoLote}`);
  console.log(`Total de lotes (actualización): ${Math.ceil(datosExistentes.length / tamanoLote)}`);
  console.log(`Total de lotes (inserción): ${Math.ceil(datosNuevos.length / tamanoLote)}`);
  console.log(`==============================\n`);
  
  const tiempoInicio = Date.now();
  let actualizados = 0;
  let insertados = 0;
  let sinCambios = 0;
  let errores = 0;
  let erroresDetalle = [];
  let canceladas = 0;
  let activas = 0;
  
  // 1. Primero actualizar los registros existentes
  console.log('\n----- ACTUALIZANDO REGISTROS EXISTENTES -----');
  
  for (let i = 0; i < datosExistentes.length; i += tamanoLote) {
    const lote = datosExistentes.slice(i, i + tamanoLote);
    console.log(`Procesando lote de actualización ${Math.floor(i / tamanoLote) + 1}/${Math.ceil(datosExistentes.length / tamanoLote)} (${lote.length} registros)`);
    
    for (const registro of lote) {
      try {
        // Construir la consulta de actualización para Cancelada
        const query = "UPDATE general SET Cancelada = ? WHERE id_referencias = ?";
        const params = [registro.Cancelada, registro.id_referencias];
        
        // Ejecutar la actualización
        const [result] = await conn.query(query, params);
        
        if (result.affectedRows > 0) {
          actualizados++;
          if (registro.Cancelada === 1) {
            canceladas++;
          } else {
            activas++;
          }
        } else {
          sinCambios++;
        }
      } catch (err) {
        console.error(`❌ Error al actualizar registro ${registro.id_referencias}:`, err.message);
        errores++;
        erroresDetalle.push({
          id_referencias: registro.id_referencias,
          NumeroDeReferencia: registro.NumeroDeReferencia,
          error: err.message,
          operacion: 'actualizar'
        });
      }
    }
    
    // Mostrar progreso después de cada lote
    const loteActual = Math.floor(i / tamanoLote) + 1;
    const totalLotes = Math.ceil(datosExistentes.length / tamanoLote);
    const porcentaje = (loteActual / totalLotes) * 100;
    const tiempoTranscurrido = (Date.now() - tiempoInicio) / 1000;
    
    // Estimar tiempo restante
    const tiempoEstimadoTotal = tiempoTranscurrido / (porcentaje / 100);
    const tiempoRestante = tiempoEstimadoTotal - tiempoTranscurrido;
    
    console.log(`\n✅ Lote de actualización ${loteActual}/${totalLotes} completado`);
    console.log(`   ${crearBarraProgreso(porcentaje)}`);
    console.log(`   Tiempo transcurrido: ${formatearTiempo(tiempoTranscurrido)}`);
    console.log(`   Tiempo restante estimado: ${formatearTiempo(tiempoRestante)}`);
    console.log(`   Actualizados: ${actualizados} (Canceladas: ${canceladas}, Activas: ${activas})`);
    console.log(`   Sin cambios: ${sinCambios}, Errores: ${errores}`);
  }
  
  // 2. Luego insertar los registros nuevos
  if (datosNuevos.length > 0) {
    console.log('\n----- INSERTANDO REGISTROS NUEVOS -----');
    
    for (let i = 0; i < datosNuevos.length; i += tamanoLote) {
      const lote = datosNuevos.slice(i, i + tamanoLote);
      console.log(`Procesando lote de inserción ${Math.floor(i / tamanoLote) + 1}/${Math.ceil(datosNuevos.length / tamanoLote)} (${lote.length} registros)`);
      
      for (const registro of lote) {
        try {
          // Construir la consulta de inserción con todos los campos
          const query = `
            INSERT INTO general (
              id_referencias, NumeroDeReferencia, Pedimento, Operacion, Clave_pedimento,
              a_despacho, a_llegada, C_Imp_Exp, facturada, Facturar_a, Agente_Aduanal, Ejecutivo,
              APERTURA, LLEGADA_MERCAN, ENTREGA_CLASIFICA, INICIO_CLASIFICA, TERMINO_CLASIFICA,
              INICIO_GLOSA, TERMINO_GLOSA, ENTREGA_GLOSA, PAGO_PEDIMENTO, DESPACHO_MERCAN,
              ENTREGA_FAC, FECHA_FAC, ENTREGA_FAC_CLI, ENTREGA_CAPTURA, INICIO_CAPTURA,
              TERMINO_CAPTURA, PRIMER_RECONOCIMIENTO, Total_Adv, Total_DTA, Total_IVA, Total_Imp,
              Cancelada
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
          `;
          
          const params = [
            registro.id_referencias,
            registro.NumeroDeReferencia,
            registro.Pedimento,
            registro.Operacion,
            registro.Clave_pedimento,
            registro.a_despacho,
            registro.a_llegada,
            registro.C_Imp_Exp,
            registro.facturada,
            registro.Facturar_a,
            registro.Agente_Aduanal,
            registro.Ejecutivo,
            sumar6Horas(registro.APERTURA),
            sumar6Horas(registro.LLEGADA_MERCAN),
            sumar6Horas(registro.ENTREGA_CLASIFICA),
            sumar6Horas(registro.INICIO_CLASIFICA),
            sumar6Horas(registro.TERMINO_CLASIFICA),
            sumar6Horas(registro.INICIO_GLOSA),
            sumar6Horas(registro.TERMINO_GLOSA),
            sumar6Horas(registro.ENTREGA_GLOSA),
            sumar6Horas(registro.PAGO_PEDIMENTO),
            sumar6Horas(registro.DESPACHO_MERCAN),
            sumar6Horas(registro.ENTREGA_FAC),
            sumar6Horas(registro.FECHA_FAC),
            sumar6Horas(registro.ENTREGA_FAC_CLI),
            sumar6Horas(registro.ENTREGA_CAPTURA),
            sumar6Horas(registro.INICIO_CAPTURA),
            sumar6Horas(registro.TERMINO_CAPTURA),
            sumar6Horas(registro.PRIMER_RECONOCIMIENTO),
            safeMoneyValue(registro.Total_Adv, 'Total_Adv'),
            safeMoneyValue(registro.Total_DTA, 'Total_DTA'),
            safeMoneyValue(registro.Total_IVA, 'Total_IVA'),
            safeMoneyValue(registro.Total_Imp, 'Total_Imp'),
            registro.Cancelada
          ];
          
          // Ejecutar la inserción
          const [result] = await conn.query(query, params);
          
          if (result.affectedRows > 0) {
            insertados++;
            if (registro.Cancelada === 1) {
              canceladas++;
            } else {
              activas++;
            }
          }
        } catch (err) {
          console.error(`❌ Error al insertar registro ${registro.id_referencias}:`, err.message);
          errores++;
          erroresDetalle.push({
            id_referencias: registro.id_referencias,
            NumeroDeReferencia: registro.NumeroDeReferencia,
            error: err.message,
            operacion: 'insertar'
          });
        }
      }
      
      // Mostrar progreso después de cada lote
      const loteActual = Math.floor(i / tamanoLote) + 1;
      const totalLotes = Math.ceil(datosNuevos.length / tamanoLote);
      const porcentaje = (loteActual / totalLotes) * 100;
      const tiempoTranscurrido = (Date.now() - tiempoInicio) / 1000;
      
      // Estimar tiempo restante
      const tiempoEstimadoTotal = tiempoTranscurrido / (porcentaje / 100);
      const tiempoRestante = tiempoEstimadoTotal - tiempoTranscurrido;
      
      console.log(`\n✅ Lote de inserción ${loteActual}/${totalLotes} completado`);
      console.log(`   ${crearBarraProgreso(porcentaje)}`);
      console.log(`   Tiempo transcurrido: ${formatearTiempo(tiempoTranscurrido)}`);
      console.log(`   Tiempo restante estimado: ${formatearTiempo(tiempoRestante)}`);
      console.log(`   Insertados: ${insertados} (Canceladas: ${canceladas}, Activas: ${activas})`);
      console.log(`   Errores: ${errores}`);
    }
  } else {
    console.log('\nℹ️ No hay registros nuevos para insertar.');
  }
  
  const tiempoTotal = (Date.now() - tiempoInicio) / 1000;
  return { 
    actualizados, 
    insertados,
    sinCambios, 
    errores, 
    tiempoTotal, 
    erroresDetalle,
    canceladas,
    activas
  };
}


// ---------- Runner principal ----------
(async () => {
  let mssqlPool, my;
  try {
    console.log('Conectando a las bases de datos...');
    mssqlPool = await sql.connect(mssqlConfig);
    my = await mysql.createConnection(mysqlConfig);
    await my.query("SET time_zone = '-06:00'"); // America/Mexico_City (UTC-6)
    
    console.log('Obteniendo datos completos de referencias...');
    const req = new sql.Request(mssqlPool);
    const rs = await req.query(Q_COMPLETO);
    const rows = rs.recordset;
    
    console.log(`Se encontraron ${rows.length} registros en total.`);
    
    // Contar registros cancelados y activos
    const canceladas = rows.filter(r => r.Cancelada === 1).length;
    const activas = rows.filter(r => r.Cancelada === 0).length;
    
    console.log(`De los cuales ${canceladas} están cancelados y ${activas} están activos.`);
    
    // Obtener IDs de registros existentes en MySQL
    console.log('Obteniendo IDs de registros existentes en MySQL...');
    const [existentesResult] = await my.query(Q_EXISTENTES);
    const idsExistentes = new Set(existentesResult.map(r => r.id_referencias));
    
    console.log(`Se encontraron ${idsExistentes.size} registros existentes en MySQL.`);
    
    // Separar registros existentes y nuevos
    const registrosExistentes = [];
    const registrosNuevos = [];
    
    for (const registro of rows) {
      if (idsExistentes.has(registro.id_referencias)) {
        registrosExistentes.push(registro);
      } else {
        registrosNuevos.push(registro);
      }
    }
    
    console.log(`De los cuales ${registrosExistentes.length} existen en la base de datos MySQL.`);
    console.log(`Y ${registrosNuevos.length} son registros nuevos que serán insertados.`);
    
    console.log('\n===== INICIANDO ACTUALIZACIÓN DE REGISTROS =====');
    console.log(`Fecha y hora: ${new Date().toISOString()}`);
    
    // Actualizar los registros en MySQL
    const resultado = await actualizarPorLotes(my, registrosExistentes, registrosNuevos);
    
    console.log('\n\n===== RESUMEN FINAL =====');
    console.log(`⏰ Fecha y hora de finalización: ${new Date().toISOString()}`);
    console.log(`⏱ Tiempo total de ejecución: ${formatearTiempo(resultado.tiempoTotal)}`);
    console.log(`📊 Estadísticas:`);
    console.log(`   - Total de registros procesados: ${registrosExistentes.length + registrosNuevos.length}`);
    console.log(`   - Registros actualizados: ${resultado.actualizados}`);
    console.log(`   - Registros insertados: ${resultado.insertados}`);
    console.log(`   - Referencias canceladas: ${resultado.canceladas}`);
    console.log(`   - Referencias activas: ${resultado.activas}`);
    console.log(`   - Registros sin cambios: ${resultado.sinCambios}`);
    console.log(`   - Errores: ${resultado.errores}`);
    
    // Mostrar detalles de errores si hay alguno
    if (resultado.errores > 0) {
      console.log('\n⚠️ DETALLE DE ERRORES:');
      resultado.erroresDetalle.forEach((err, index) => {
        console.log(`\nError #${index + 1}:`);
        console.log(`   ID Referencia: ${err.id_referencias}`);
        console.log(`   Número de Referencia: ${err.NumeroDeReferencia}`);
        console.log(`   Mensaje: ${err.error}`);
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
