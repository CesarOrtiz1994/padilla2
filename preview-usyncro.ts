// Muestra cuántas referencias pasarían los filtros del cron Usyncro sin tocar la API ni MySQL
import 'dotenv/config';
import * as sql from 'mssql';
import { mssqlConfig } from './src/config/database';
import { CLIENTES_MAP, CLIENTES_INTEGRACION } from './src/config/integracion';
import { usyncroConfig } from './src/config/usyncro';

(async () => {
  let mssqlPool: sql.ConnectionPool | undefined;
  try {
    const desde = new Date(Date.now() - usyncroConfig.dias * 86400000);
    const idList = [...CLIENTES_MAP.keys()].join(',');

    console.log(`\n${'═'.repeat(60)}`);
    console.log('FILTRO USYNCRO — sin tocar API ni BD local');
    console.log(`${'═'.repeat(60)}`);
    console.log(`Ventana      : últimos ${usyncroConfig.dias} días (desde ${desde.toISOString().split('T')[0]})`);
    console.log(`Clientes     : ${idList}`);
    console.log(`${'─'.repeat(60)}\n`);

    mssqlPool = await sql.connect(mssqlConfig);
    const req = new sql.Request(mssqlPool);
    req.input('fApertura', sql.DateTime, desde);

    const rs = await req.query<{
      NumeroDeReferencia: string;
      id_referencias: number;
      id_cliente: number;
      Operacion: number;
      FechaApertura: Date | null;
      Cancelada: number;
      descripcion_aduana: string | null;
      patente_agente: string | null;
      pedimento: string | null;
      clearance_date: Date | null;
    }>(`
      SELECT
        r.NumeroDeReferencia,
        r.id_referencias,
        r.id_cliente,
        r.Operacion,
        r.FechaApertura,
        r.Cancelada,
        a.descripcion AS descripcion_aduana,
        aa.patente    AS patente_agente,
        (SELECT TOP 1 pe.Pedimento FROM PedimentosEncabezado pe WHERE pe.id_referencia = r.id_referencias) AS pedimento,
        CASE
          WHEN r.Operacion = 1 THEN (SELECT bi.FechaHoraEvento FROM BitacoraEventosImportacion bi WHERE bi.Referencia = r.id_referencias AND bi.IdEvento = 29)
          WHEN r.Operacion = 2 THEN (SELECT be.FechaHoraEvento FROM BitacoraEventosExportacion  be WHERE be.Referencia = r.id_referencias AND be.IdEvento = 29)
        END AS clearance_date
      FROM referencias r
      LEFT JOIN aduana a ON a.id_Aduana = r.id_aduana
      LEFT JOIN agentesaduanales aa ON aa.id_agenteaduanal = r.id_agenteaduanal
      WHERE r.id_cliente IN (${idList})
        AND r.FechaApertura > @fApertura
        AND r.Cancelada = 0
    `);

    const refs = rs.recordset;
    type RefRow = (typeof refs)[0];
    console.log(`Total referencias que pasarían: ${refs.length}\n`);

    const porCliente = new Map<number, RefRow[]>();
    for (const r of refs) {
      if (!porCliente.has(r.id_cliente)) porCliente.set(r.id_cliente, []);
      porCliente.get(r.id_cliente)!.push(r);
    }

    for (const cliente of CLIENTES_INTEGRACION) {
      const lista = porCliente.get(cliente.id_cliente) ?? [];
      if (!lista.length) continue;
      console.log(`${'─'.repeat(60)}`);
      console.log(`${cliente.importador} (id=${cliente.id_cliente}) — ${lista.length} referencia(s)`);
      for (const r of lista) {
        const op  = r.Operacion === 1 ? 'IMP' : 'EXP';
        const ape = r.FechaApertura ? new Date(r.FechaApertura).toISOString().split('T')[0] : 'sin fecha';
        const ped = r.pedimento?.trim() || '—';
        const cl  = r.clearance_date ? new Date(r.clearance_date).toISOString().split('T')[0] : '—';
        console.log(`  ${r.NumeroDeReferencia}  [${op}]  apertura=${ape}  pedimento=${ped}  clearance=${cl}`);
      }
    }

    console.log(`\n${'═'.repeat(60)}\n`);

  } catch (err) {
    console.error('[ERROR]', (err as Error).message);
    process.exit(1);
  } finally {
    if (mssqlPool) await mssqlPool.close();
  }
})();
