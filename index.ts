import 'dotenv/config';
import * as sql from 'mssql';
import mysql from 'mysql2/promise';
import { mssqlConfig, mysqlConfig } from './src/config/database';
import { ACOLCHADO_DIAS, DEBUG_REF_ID } from './src/config/constants';
import { getCheckpoint, setCheckpoint } from './src/services/checkpoint';
import { debugReferencia } from './src/services/debug';
import { runEtlGeneral } from './src/jobs/etlGeneral';
import { runEtlFacturas } from './src/jobs/etlFacturas';

(async () => {
  let mssqlPool: sql.ConnectionPool | undefined;
  let my: mysql.Connection | undefined;
  try {
    console.log('Conectando...');
    mssqlPool = await sql.connect(mssqlConfig);
    my = await mysql.createConnection(mysqlConfig);
    await my.query("SET time_zone = '-06:00'");

    const lastDt = await getCheckpoint(my);
    const desde = new Date(lastDt.getTime() - ACOLCHADO_DIAS * 86400000);

    if (DEBUG_REF_ID) {
      console.log(`DEBUG: last_dt=${lastDt.toISOString()} ACOLCHADO_DIAS=${ACOLCHADO_DIAS}`);
    }

    await debugReferencia(mssqlPool, my, desde);

    await my.beginTransaction();

    const resGen = await runEtlGeneral(mssqlPool, my, desde);
    const resFac = await runEtlFacturas(mssqlPool, my, desde);

    if (resGen.maxApertura) await setCheckpoint(my, resGen.maxApertura);

    await my.commit();

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
      `desde:${desde.toISOString()}`,
    ].join(' | '));

    const genWarnings = Object.keys(resGen.warningsSummary ?? {}).length;
    const facWarnings = Object.keys(resFac.warningsSummary ?? {}).length;
    if (genWarnings > 0 || facWarnings > 0) {
      console.log('\nWARNINGS DETECTADOS - Revisar logs arriba para detalles y soluciones sugeridas');
    }

    await my.end();
    await mssqlPool.close();
  } catch (err) {
    try { if (my) await my.rollback(); } catch { /* noop */ }
    console.error('ETL ERROR:', err);
    try { if (my) await my.end(); } catch { /* noop */ }
    try { if (mssqlPool) await mssqlPool.close(); } catch { /* noop */ }
    process.exit(1);
  }
})();
