import 'dotenv/config';
import { runEtlUsyncro } from './src/jobs/etlUsyncro';

process.on('uncaughtException', (err: NodeJS.ErrnoException) => {
  if (err.code === 'ECONNRESET') {
    console.warn('[Runner-Usyncro] ECONNRESET ignorado');
    return;
  }
  console.error('[Runner-Usyncro] Error no capturado:', err);
  process.exit(1);
});

(async () => {
  try {
    await runEtlUsyncro();
    console.log('[Runner-Usyncro] ETL-Usyncro completado exitosamente');
    process.exit(0);
  } catch (err) {
    console.error('[Runner-Usyncro] ETL-Usyncro falló:', (err as Error).message);
    process.exit(1);
  }
})();
