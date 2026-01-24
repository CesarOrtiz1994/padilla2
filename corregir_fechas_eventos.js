// corregir_fechas_eventos.js - Corrección de fechas de eventos con diferencia de zona horaria
require('dotenv').config();
const mysql = require('mysql2/promise');

// ----- MySQL conn -----
const mysqlConfig = {
  host: process.env.MYSQL_HOST1,
  user: process.env.MYSQL_USER1,
  password: process.env.MYSQL_PASS1,
  database: process.env.MYSQL_DB1,
  port: Number(process.env.MYSQL_PORT1 || 3306)
};

// Campos de eventos a corregir
const CAMPOS_EVENTOS = [
  'LLEGADA_MERCAN', 'ENTREGA_CLASIFICA', 'INICIO_CLASIFICA', 'TERMINO_CLASIFICA',
  'INICIO_GLOSA', 'TERMINO_GLOSA', 'ENTREGA_GLOSA', 'PAGO_PEDIMENTO', 'DESPACHO_MERCAN',
  'ENTREGA_FAC', 'FECHA_FAC', 'ENTREGA_FAC_CLI', 'ENTREGA_CAPTURA', 'INICIO_CAPTURA',
  'TERMINO_CAPTURA', 'PRIMER_RECONOCIMIENTO', 'APERTURA'
];

// Función para crear una barra de progreso
function crearBarraProgreso(porcentaje, longitud = 30) {
  const completado = Math.floor(porcentaje * longitud / 100);
  const restante = longitud - completado;
  const barraCompletada = '█'.repeat(completado);
  const barraRestante = '░'.repeat(restante);
  return `[${barraCompletada}${barraRestante}] ${porcentaje.toFixed(1)}%`;
}

// Función para formatear tiempo
function formatearTiempo(segundos) {
  if (segundos < 60) return `${segundos.toFixed(1)} segundos`;
  const minutos = Math.floor(segundos / 60);
  const segs = Math.floor(segundos % 60);
  return `${minutos} min ${segs} seg`;
}

// ---------- Función para corregir fechas por lotes ----------
async function corregirFechasPorLotes(conn, tamanoLote = 500) {
  console.log(`\n===== INICIANDO CORRECCIÓN DE FECHAS =====`);
  console.log(`Fecha y hora de inicio: ${new Date().toISOString()}`);
  console.log(`Tamaño de lote: ${tamanoLote}`);
  console.log(`Campos a corregir: ${CAMPOS_EVENTOS.length}`);
  console.log(`==============================\n`);
  
  const tiempoInicio = Date.now();
  let registrosActualizados = 0;
  let registrosSinCambios = 0;
  let errores = 0;
  let erroresDetalle = [];
  
  // Obtener total de registros
  const [countResult] = await conn.query('SELECT COUNT(*) as total FROM general');
  const totalRegistros = countResult[0].total;
  
  console.log(`Total de registros en la tabla: ${totalRegistros}\n`);
  
  // Procesar en lotes
  let offset = 0;
  let loteActual = 0;
  const totalLotes = Math.ceil(totalRegistros / tamanoLote);
  
  while (offset < totalRegistros) {
    loteActual++;
    console.log(`Procesando lote ${loteActual}/${totalLotes} (offset: ${offset})`);
    
    // Obtener lote de registros
    const [registros] = await conn.query(
      `SELECT id_referencias, ${CAMPOS_EVENTOS.join(', ')} 
       FROM general 
       LIMIT ? OFFSET ?`,
      [tamanoLote, offset]
    );
    
    // Procesar cada registro del lote
    for (const registro of registros) {
      try {
        const actualizaciones = [];
        const params = [];
        let tieneActualizaciones = false;
        
        // Revisar cada campo de fecha
        for (const campo of CAMPOS_EVENTOS) {
          const fechaActual = registro[campo];
          
          // Si el campo tiene valor, sumarle 6 horas
          if (fechaActual !== null && fechaActual !== undefined) {
            actualizaciones.push(`${campo} = DATE_ADD(${campo}, INTERVAL 6 HOUR)`);
            tieneActualizaciones = true;
          }
        }
        
        // Si hay campos para actualizar
        if (tieneActualizaciones) {
          const query = `UPDATE general SET ${actualizaciones.join(', ')} WHERE id_referencias = ?`;
          params.push(registro.id_referencias);
          
          const [result] = await conn.query(query, params);
          
          if (result.affectedRows > 0) {
            registrosActualizados++;
          } else {
            registrosSinCambios++;
          }
        } else {
          registrosSinCambios++;
        }
      } catch (err) {
        console.error(`❌ Error al actualizar registro ${registro.id_referencias}:`, err.message);
        errores++;
        erroresDetalle.push({
          id_referencias: registro.id_referencias,
          error: err.message
        });
      }
    }
    
    // Mostrar progreso
    const porcentaje = (loteActual / totalLotes) * 100;
    const tiempoTranscurrido = (Date.now() - tiempoInicio) / 1000;
    const tiempoEstimadoTotal = tiempoTranscurrido / (porcentaje / 100);
    const tiempoRestante = tiempoEstimadoTotal - tiempoTranscurrido;
    
    console.log(`✅ Lote ${loteActual}/${totalLotes} completado`);
    console.log(`   ${crearBarraProgreso(porcentaje)}`);
    console.log(`   Tiempo transcurrido: ${formatearTiempo(tiempoTranscurrido)}`);
    console.log(`   Tiempo restante estimado: ${formatearTiempo(tiempoRestante)}`);
    console.log(`   Actualizados: ${registrosActualizados}, Sin cambios: ${registrosSinCambios}, Errores: ${errores}\n`);
    
    offset += tamanoLote;
  }
  
  const tiempoTotal = (Date.now() - tiempoInicio) / 1000;
  return { 
    registrosActualizados, 
    registrosSinCambios, 
    errores, 
    tiempoTotal, 
    erroresDetalle,
    totalRegistros
  };
}

// ---------- Runner principal ----------
(async () => {
  let my;
  try {
    console.log('Conectando a MySQL...');
    my = await mysql.createConnection(mysqlConfig);
    
    console.log('✅ Conexión establecida');
    console.log('\n⚠️  ADVERTENCIA: Este script sumará 6 horas a TODAS las fechas de eventos.');
    console.log('⚠️  Asegúrate de ejecutarlo solo UNA VEZ para corregir el problema de zona horaria.\n');
    
    // Esperar 3 segundos para que el usuario pueda cancelar si es necesario
    console.log('Iniciando en 3 segundos... (Ctrl+C para cancelar)');
    await new Promise(resolve => setTimeout(resolve, 3000));
    
    // Ejecutar corrección
    const resultado = await corregirFechasPorLotes(my);
    
    console.log('\n\n===== RESUMEN FINAL =====');
    console.log(`⏰ Fecha y hora de finalización: ${new Date().toISOString()}`);
    console.log(`⏱ Tiempo total de ejecución: ${formatearTiempo(resultado.tiempoTotal)}`);
    console.log(`📊 Estadísticas:`);
    console.log(`   - Total de registros procesados: ${resultado.totalRegistros}`);
    console.log(`   - Registros actualizados: ${resultado.registrosActualizados}`);
    console.log(`   - Registros sin cambios: ${resultado.registrosSinCambios}`);
    console.log(`   - Errores: ${resultado.errores}`);
    
    // Mostrar detalles de errores si hay alguno
    if (resultado.errores > 0) {
      console.log('\n⚠️ DETALLE DE ERRORES:');
      resultado.erroresDetalle.forEach((err, index) => {
        console.log(`\nError #${index + 1}:`);
        console.log(`   ID Referencia: ${err.id_referencias}`);
        console.log(`   Mensaje: ${err.error}`);
      });
    }
    
    // Cierre
    await my.end();
    console.log('\n✅ PROCESO COMPLETADO.');
    console.log('✅ Las fechas han sido corregidas sumando 6 horas.');
    
  } catch (err) {
    console.error('\n\n❌ ERROR FATAL EN EL PROCESO:');
    console.error(`Fecha y hora: ${new Date().toISOString()}`);
    console.error(`Mensaje: ${err.message}`);
    console.error(`Stack: ${err.stack}`);
    
    // Intentar cerrar la conexión
    console.log('\nCerrando conexión...');
    try { 
      if (my) {
        await my.end();
        console.log('- Conexión MySQL cerrada correctamente');
      }
    } catch (e) { 
      console.error('- Error al cerrar conexión MySQL:', e.message);
    }
    
    console.error('\n❌ PROCESO TERMINADO CON ERRORES');
    process.exit(1);
  }
})();
