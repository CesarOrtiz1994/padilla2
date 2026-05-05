// index.js - Orquestador principal del ETL
require('dotenv').config();
const sql = require('mssql');
const mysql = require('mysql2/promise');

const { mssqlConfig, mysqlConfig } = require('./src/config/database');
const { ACOLCHADO_DIAS, DEBUG_REF_ID } = require('./src/config/constants');
const { getCheckpoint, setCheckpoint } = require('./src/services/checkpoint');
const { debugReferencia } = require('./src/services/debug');
const { runEtlGeneral } = require('./src/jobs/etlGeneral');
const { runEtlFacturas } = require('./src/jobs/etlFacturas');

// ---------- Orquestador principal ----------
(async () => {
  let mssqlPool, my;
  try {
    console.log('Conectando...');
    mssqlPool = await sql.connect(mssqlConfig);
    my = await mysql.createConnection(mysqlConfig);
    await my.query("SET time_zone = '-06:00'");

    // 1) Lee checkpoint y calcula ventana
    const lastDt = await getCheckpoint(my);
    const desde = new Date(lastDt.getTime() - ACOLCHADO_DIAS * 86400000);

    if (DEBUG_REF_ID) {
      console.log(`DEBUG: last_dt=${lastDt.toISOString()} ACOLCHADO_DIAS=${ACOLCHADO_DIAS}`);
    }

    await debugReferencia(mssqlPool, my, desde);

    // 2) Ejecutar jobs ETL
    await my.beginTransaction();

    const resGen = await runEtlGeneral(mssqlPool, my, desde);
    const resFac = await runEtlFacturas(mssqlPool, my, desde);

    // 3) Actualizar checkpoint
    if (resGen.maxApertura) await setCheckpoint(my, resGen.maxApertura);

    await my.commit();

    // 4) Logs de métricas
    const statsGen = resGen.stats;
    const statsFac = resFac.stats;
    const insertedGen = statsGen.records - statsGen.duplicates;
    const insertedFac = statsFac.records - statsFac.duplicates;

    console.log([
      'OK',
      `general: selected=${resGen.selected} dropped=${resGen.dropped} prepared=${resGen.prepared} ` +
      `upsert_total=${statsGen.records} inserted=${insertedGen} ` +
      `updated_attempted=${statsGen.duplicates} updated_changed=${statsGen.changedRows} warnings=${statsGen.warnings}`,
      `facturas: selected=${resFac.selected} dropped=${resFac.dropped} prepared=${resFac.prepared} ` +
      `upsert_total=${statsFac.records} inserted=${insertedFac} ` +
      `updated_attempted=${statsFac.duplicates} updated_changed=${statsFac.changedRows} warnings=${statsFac.warnings}`,
      `watermark->${resGen.maxApertura ? resGen.maxApertura.toISOString() : 'N/A'}`,
      `desde:${desde.toISOString()}`
    ].join(' | '));

    // 5) Mostrar warnings si hay
    const genWarnings = Object.keys(resGen.warningsSummary || {}).length;
    const facWarnings = Object.keys(resFac.warningsSummary || {}).length;
    if (genWarnings > 0 || facWarnings > 0) {
      console.log('\n WARNINGS DETECTADOS - Revisar logs arriba para detalles y soluciones sugeridas');
    }

    // 6) Cierres
    await my.end();
    await mssqlPool.close();
  } catch (err) {
    try { if (my) await my.rollback(); } catch (e) { /* noop */ }
    console.error('ETL ERROR:', err);
    try { if (my) await my.end(); } catch (e) { /* noop */ }
    try { if (mssqlPool) await mssqlPool.close(); } catch (e) { /* noop */ }
    process.exit(1);
  }
})();
