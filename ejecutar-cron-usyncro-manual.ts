// Corre el ETL de Usyncro EXACTAMENTE como lo hace el cron (scheduler-usyncro.ts),
// en modo normal (sin forzar), para poder ver el log completo en vivo y detectar
// si algo en el propio cron está mal (no solo si una referencia pasa los filtros).
//
// Antes de correr, imprime si cada referencia "vigilada" entraría en esta corrida
// (ventana de fecha / rescate de pendientes) y por qué. Al terminar, confirma si
// quedó creada en usyncro_registros.
//
// Uso:     npx tsx ejecutar-cron-usyncro-manual.ts [REF1 REF2 ...]
// Si no se pasan referencias, usa la lista por defecto (las de la imagen reportada).
import 'dotenv/config';
import * as sql from 'mssql';
import mysql from 'mysql2/promise';
import { mssqlConfig, mysqlConfig } from './src/config/database';
import { CLIENTES_MAP } from './src/config/integracion';
import { usyncroConfig } from './src/config/usyncro';
import { Q_USYNCRO_REFERENCIAS, qUsyncroReferenciasPorLista } from './src/queries/usyncro';
import { getReferenciasPendientes, getRegistro } from './src/services/usyncroStore';
import { runEtlUsyncro } from './src/jobs/etlUsyncro';

const DEFAULT_REFS = [
  'AP260301-00', 'MI260233-00', 'NL260411-00', 'QR260405-00', 'TL260079-00',
  'VE260229-0A', 'VE260299-00', 'VE260300-00', 'VE260301-00', 'VE260302-00',
];

const refsVigiladas = process.argv.slice(2).length > 0 ? process.argv.slice(2) : DEFAULT_REFS;
const SEP = '─'.repeat(60);

process.on('uncaughtException', (err: NodeJS.ErrnoException) => {
  if (err.code === 'ECONNRESET') {
    console.warn('[Ejecutar-Cron-Manual] ECONNRESET ignorado');
    return;
  }
  console.error('[Ejecutar-Cron-Manual] Error no capturado:', err);
  process.exit(1);
});

async function reportarEstado(my: mysql.Connection, titulo: string): Promise<void> {
  console.log(`\n${SEP}`);
  console.log(titulo);
  console.log(SEP);
  for (const ref of refsVigiladas) {
    const registro = await getRegistro(my, ref);
    if (!registro) {
      console.log(`  ${ref.padEnd(14)} → sin registro (no creada)`);
    } else if (!registro.record_id) {
      console.log(`  ${ref.padEnd(14)} → placeholder reservado, sin record_id`);
    } else {
      console.log(`  ${ref.padEnd(14)} → record_id=${registro.record_id} | completado=${registro.fecha_completado ?? 'no'}`);
    }
  }
}

(async () => {
  let mssqlPool: sql.ConnectionPool | undefined;
  let my: mysql.Connection | undefined;

  try {
    mssqlPool = await sql.connect(mssqlConfig);
    my = await mysql.createConnection(mysqlConfig);

    console.log(`${'═'.repeat(60)}`);
    console.log('EJECUCIÓN MANUAL DEL CRON DE USYNCRO (modo normal, sin forzar)');
    console.log(`${'═'.repeat(60)}`);
    console.log(`Referencias vigiladas: ${refsVigiladas.join(', ')}`);

    // ── ¿Cuáles de las vigiladas entrarían en esta corrida? ────────────────
    const desde = new Date(Date.now() - usyncroConfig.dias * 86400000);
    const req = new sql.Request(mssqlPool);
    req.input('fApertura', sql.DateTime, desde);
    const rs = await req.query<{ NumeroDeReferencia: string; id_cliente: number; Cancelada: number }>(Q_USYNCRO_REFERENCIAS);
    const enVentana = new Set(rs.recordset.map(r => r.NumeroDeReferencia));

    const pendientes = new Set(await getReferenciasPendientes(my, usyncroConfig.graciaInvoicesDias));

    console.log(`\n${SEP}`);
    console.log(`¿ENTRARÍAN EN ESTA CORRIDA? (ventana=${usyncroConfig.dias}d desde ${desde.toISOString().split('T')[0]})`);
    console.log(SEP);
    for (const ref of refsVigiladas) {
      if (enVentana.has(ref)) {
        console.log(`  ${ref.padEnd(14)} ✔ dentro de la ventana de fecha`);
      } else if (pendientes.has(ref)) {
        console.log(`  ${ref.padEnd(14)} ✔ rescatada como pendiente (fuera de ventana)`);
      } else {
        // Verificar por qué no: no existe, cliente no habilitado, cancelada, o fuera de ventana sin ser pendiente
        const req2 = new sql.Request(mssqlPool);
        req2.input('ref0', sql.VarChar, ref);
        const rs2 = await req2.query<{ NumeroDeReferencia: string; id_cliente: number; Cancelada: number }>(qUsyncroReferenciasPorLista(1));
        if (!rs2.recordset.length) {
          console.log(`  ${ref.padEnd(14)} ✘ no encontrada en SQL Server, cliente no habilitado, o Cancelada=1`);
        } else {
          console.log(`  ${ref.padEnd(14)} ✘ fuera de ventana de fecha y no está pendiente en MySQL (no se tocará hoy)`);
        }
      }
    }

    await reportarEstado(my, 'ESTADO ANTES DE CORRER (usyncro_registros)');

    // ── Correr el ETL real, igual que el cron ───────────────────────────────
    console.log(`\n${'═'.repeat(60)}`);
    console.log('INICIANDO ETL (idéntico al cron programado)...');
    console.log(`${'═'.repeat(60)}\n`);

    await my.end();
    await mssqlPool.close();
    mssqlPool = undefined;
    my = undefined;

    await runEtlUsyncro();

    // Reabrir conexiones para el reporte final
    mssqlPool = await sql.connect(mssqlConfig);
    my = await mysql.createConnection(mysqlConfig);

    await reportarEstado(my, 'ESTADO DESPUÉS DE CORRER (usyncro_registros)');

    console.log('\n[Ejecutar-Cron-Manual] Completado.');
    process.exit(0);
  } catch (err) {
    console.error('[Ejecutar-Cron-Manual] Falló:', (err as Error).message);
    process.exit(1);
  } finally {
    if (my) await my.end();
    if (mssqlPool) await mssqlPool.close();
  }
})();
