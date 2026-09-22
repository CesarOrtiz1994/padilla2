// Ejecuta el proceso real del cron de Usyncro (crea/sincroniza registros)
// para referencias específicas, ignorando el filtro de FechaApertura.
//
// Uso:     npx tsx forzar-usyncro.ts <REF1> [REF2] ...
// Ejemplo: npx tsx forzar-usyncro.ts VE260297-00
import 'dotenv/config';
import { runEtlUsyncro } from './src/jobs/etlUsyncro';

const refs = process.argv.slice(2);

if (refs.length === 0) {
  console.error('Uso: npx tsx forzar-usyncro.ts <REF1> [REF2] ...');
  process.exit(1);
}

process.on('uncaughtException', (err: NodeJS.ErrnoException) => {
  if (err.code === 'ECONNRESET') {
    console.warn('[Forzar-Usyncro] ECONNRESET ignorado');
    return;
  }
  console.error('[Forzar-Usyncro] Error no capturado:', err);
  process.exit(1);
});

(async () => {
  try {
    await runEtlUsyncro(refs);
    console.log('[Forzar-Usyncro] Completado exitosamente');
    process.exit(0);
  } catch (err) {
    console.error('[Forzar-Usyncro] Falló:', (err as Error).message);
    process.exit(1);
  }
})();
