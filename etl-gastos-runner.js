// etl-gastos-runner.js - Runner standalone para ETL de gastos comprobados
// Este archivo es ejecutado por scheduler-gastos.js

require('dotenv').config();
const { runEtlGastosFTP } = require('./src/jobs/etlGastosFTP');

(async () => {
  try {
    const result = await runEtlGastosFTP();
    console.log('[Runner] ETL-FTP completado exitosamente');
    process.exit(0);
  } catch (err) {
    console.error('[Runner] ETL-FTP falló:', err.message);
    process.exit(1);
  }
})();
