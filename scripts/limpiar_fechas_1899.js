// limpiar_fechas_1899.js - Limpia fechas 1899-12-31 existentes en MySQL, poniéndolas como NULL
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

// Campos de fecha a limpiar
const CAMPOS_FECHA = [
  'APERTURA', 'LLEGADA_MERCAN', 'ENTREGA_CLASIFICA', 'INICIO_CLASIFICA', 'TERMINO_CLASIFICA',
  'INICIO_GLOSA', 'TERMINO_GLOSA', 'ENTREGA_GLOSA', 'PAGO_PEDIMENTO', 'DESPACHO_MERCAN',
  'ENTREGA_FAC', 'FECHA_FAC', 'ENTREGA_FAC_CLI', 'ENTREGA_CAPTURA', 'INICIO_CAPTURA',
  'TERMINO_CAPTURA', 'PRIMER_RECONOCIMIENTO'
];

(async () => {
  let my;
  try {
    console.log('Conectando a MySQL...');
    my = await mysql.createConnection(mysqlConfig);

    console.log('\n===== LIMPIEZA DE FECHAS 1899-12-31 =====');
    console.log(`Fecha y hora: ${new Date().toISOString()}`);
    console.log(`Campos a revisar: ${CAMPOS_FECHA.length}\n`);

    let totalActualizados = 0;

    for (const campo of CAMPOS_FECHA) {
      const query = `UPDATE general SET ${campo} = NULL WHERE ${campo} IS NOT NULL AND YEAR(${campo}) <= 1900`;
      const [result] = await my.query(query);

      if (result.affectedRows > 0) {
        console.log(`✅ ${campo}: ${result.affectedRows} registros limpiados`);
        totalActualizados += result.affectedRows;
      } else {
        console.log(`⚪ ${campo}: sin fechas 1899`);
      }
    }

    console.log(`\n===== RESUMEN =====`);
    console.log(`Total de registros limpiados: ${totalActualizados}`);

    await my.end();
    console.log('\n✅ PROCESO COMPLETADO.');

  } catch (err) {
    console.error('\n❌ ERROR:', err.message);
    console.error(err.stack);
    try { if (my) await my.end(); } catch (e) { }
    process.exit(1);
  }
})();
