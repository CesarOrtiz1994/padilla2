import 'dotenv/config';
import { runEtlGastosFTP } from './src/jobs/etlGastosFTP';

// El servidor SFTP puede cerrar la conexión abruptamente al desconectar; no es un error del proceso
process.on('uncaughtException', (err: NodeJS.ErrnoException) => {
  if (err.code === 'ECONNRESET') {
    console.warn('[Runner] SFTP ECONNRESET ignorado (conexión cerrada por el servidor remoto)');
    return;
  }
  console.error('[Runner] Error no capturado:', err);
  process.exit(1);
});

(async () => {
  try {
    await runEtlGastosFTP();
    console.log('[Runner] ETL-FTP completado exitosamente');
    process.exit(0);
  } catch (err) {
    console.error('[Runner] ETL-FTP falló:', (err as Error).message);
    process.exit(1);
  }
})();
