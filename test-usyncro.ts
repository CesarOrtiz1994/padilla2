// Ejecución manual del ETL Usyncro — mismo comportamiento que el cron (hoy + ayer)
import 'dotenv/config';
import { runEtlUsyncro } from './src/jobs/etlUsyncro';

(async () => {
  try {
    await runEtlUsyncro();
    process.exit(0);
  } catch (err) {
    console.error('[Manual] ETL-Usyncro falló:', (err as Error).message);
    process.exit(1);
  }
})();
