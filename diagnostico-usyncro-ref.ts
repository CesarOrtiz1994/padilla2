// Diagnóstico: verifica si una referencia pasa los filtros del cron Usyncro
import 'dotenv/config';
import * as sql from 'mssql';
import mysql from 'mysql2/promise';
import { mssqlConfig, mysqlConfig } from './src/config/database';
import { CLIENTES_MAP } from './src/config/integracion';
import { usyncroConfig } from './src/config/usyncro';
import { esReferenciaParcial, diasTranscurridos, armarCustomsNumber } from './src/utils/referencias';

const SEP = '─'.repeat(60);
const numRef = process.argv[2];
const ignorarFecha = process.argv.includes('--ignorar-fecha');

if (!numRef) {
  console.error('Uso: npx tsx diagnostico-usyncro-ref.ts <NumeroDeReferencia> [--ignorar-fecha]');
  process.exit(1);
}

(async () => {
  let mssqlPool: sql.ConnectionPool | undefined;
  let my: mysql.Connection | undefined;
  try {
    mssqlPool = await sql.connect(mssqlConfig);
    my = await mysql.createConnection(mysqlConfig);

    const idList = [...CLIENTES_MAP.keys()].join(',');
    const desde = new Date(Date.now() - usyncroConfig.dias * 86400000);

    console.log(`\n${'═'.repeat(60)}`);
    console.log(`DIAGNÓSTICO USYNCRO — ${numRef}`);
    console.log(`${'═'.repeat(60)}`);
    console.log(`Ventana cron: últimos ${usyncroConfig.dias} días (desde ${desde.toISOString().split('T')[0]})`);
    console.log(`Clientes habilitados: ${idList}`);
    if (ignorarFecha) console.log('⚠ Modo --ignorar-fecha: el filtro de FechaApertura se omite en el resultado final');
    console.log('');

    // ── 1. Existe en SQL Server ───────────────────────────────────────────
    console.log(SEP);
    console.log('1. DATOS EN SQL SERVER');
    console.log(SEP);

    const req = new sql.Request(mssqlPool);
    req.input('ref', sql.VarChar, numRef);
    const rs = await req.query<{
      NumeroDeReferencia: string;
      id_referencias: number;
      id_cliente: number;
      Operacion: number;
      FechaApertura: Date | null;
      Cancelada: number;
      Primer_Reconocimiento: number | null;
      descripcion_aduana: string | null;
      codigo_aduana: string | null;
      patente_agente: string | null;
      pedimento: string | null;
      clearance_date: Date | null;
    }>(`
      SELECT TOP 1
        r.NumeroDeReferencia, r.id_referencias, r.id_cliente,
        r.Operacion, r.FechaApertura, r.Cancelada, r.Primer_Reconocimiento,
        a.descripcion AS descripcion_aduana, a.aduana AS codigo_aduana,
        aa.patente AS patente_agente,
        (SELECT TOP 1 pe.Pedimento FROM PedimentosEncabezado pe WHERE pe.id_referencia = r.id_referencias) AS pedimento,
        CASE
          WHEN r.Operacion = 1 THEN (SELECT bi.FechaHoraEvento FROM BitacoraEventosImportacion bi WHERE bi.Referencia = r.id_referencias AND bi.IdEvento = 29)
          WHEN r.Operacion = 2 THEN (SELECT be.FechaHoraEvento FROM BitacoraEventosExportacion be WHERE be.Referencia = r.id_referencias AND be.IdEvento = 29)
        END AS clearance_date
      FROM referencias r
      LEFT JOIN aduana a ON a.id_Aduana = r.id_aduana
      LEFT JOIN agentesaduanales aa ON aa.id_agenteaduanal = r.id_agenteaduanal
      WHERE r.NumeroDeReferencia = @ref
    `);

    if (!rs.recordset.length) {
      console.log('  ✘ Referencia NO encontrada en SQL Server');
      return;
    }

    const r = rs.recordset[0];
    console.log(`  NumeroDeReferencia : ${r.NumeroDeReferencia}`);
    console.log(`  id_referencias     : ${r.id_referencias}`);
    console.log(`  id_cliente         : ${r.id_cliente}`);
    console.log(`  Operacion          : ${r.Operacion} (${r.Operacion === 1 ? 'IMPORTACION' : 'EXPORTACION'})`);
    console.log(`  FechaApertura      : ${r.FechaApertura?.toISOString() ?? 'null'}`);
    console.log(`  Cancelada          : ${r.Cancelada}`);
    console.log(`  Primer_Reconocim.  : ${r.Primer_Reconocimiento ?? 'null'}`);
    console.log(`  descripcion_aduana : ${r.descripcion_aduana ?? 'null'}`);
    console.log(`  codigo_aduana      : ${r.codigo_aduana ?? 'null'}`);
    console.log(`  patente_agente     : ${r.patente_agente ?? 'null'}`);
    console.log(`  pedimento          : ${r.pedimento ?? 'null'}`);
    console.log(`  clearance_date     : ${r.clearance_date ? new Date(r.clearance_date).toISOString() : 'null'}`);

    // ── 2. Filtros del cron ───────────────────────────────────────────────
    console.log(`\n${SEP}`);
    console.log('2. FILTROS DEL CRON');
    console.log(SEP);

    const clienteConf = CLIENTES_MAP.get(r.id_cliente);
    const pasaCliente = !!clienteConf;
    const pasaCancelada = r.Cancelada == 0;
    const pasaVentana = r.FechaApertura ? r.FechaApertura > desde : false;

    console.log(`  id_cliente en lista  : ${pasaCliente ? '✔' : '✘'} ${pasaCliente ? `(${clienteConf!.importador})` : `id ${r.id_cliente} NO está en CLIENTES_INTEGRACION`}`);
    console.log(`  Cancelada = 0        : ${pasaCancelada ? '✔' : `✘ (valor=${r.Cancelada})`}`);
    console.log(`  FechaApertura > -${usyncroConfig.dias}d : ${pasaVentana ? '✔' : `✘ (${r.FechaApertura?.toISOString().split('T')[0] ?? 'null'} no está dentro de los últimos ${usyncroConfig.dias} días)`}${ignorarFecha ? ' (ignorado por --ignorar-fecha)' : ''}`);

    const pasaTodo = pasaCliente && pasaCancelada && (pasaVentana || ignorarFecha);
    console.log(`\n  → ${pasaTodo ? '✔ PASA todos los filtros — el cron la procesaría' : '✘ NO pasa los filtros — el cron la ignoraría'}`);

    // ── 3. Regla de referencias parciales (-0A, -0B...) ────────────────────
    console.log(`\n${SEP}`);
    console.log('3. REGLA DE CREACIÓN (referencias parciales)');
    console.log(SEP);

    const esParcial = esReferenciaParcial(r.NumeroDeReferencia);
    console.log(`  ¿Es referencia parcial (-0A, -0B...)? : ${esParcial ? 'SÍ' : 'NO'}`);

    if (esParcial) {
      const customsNumber = armarCustomsNumber({
        fechaApertura: r.FechaApertura,
        codigoAduana: r.codigo_aduana,
        patenteAgente: r.patente_agente,
        pedimento: r.pedimento,
      });
      const taxesListo = !!customsNumber && !!r.clearance_date;
      const dias = diasTranscurridos(r.FechaApertura);

      console.log(`  customsNumber armado                 : ${customsNumber ?? '(incompleto)'}`);
      console.log(`  clearance_date disponible             : ${r.clearance_date ? 'SÍ' : 'NO'}`);
      console.log(`  taxes completo                        : ${taxesListo ? '✔ SÍ' : '✘ NO'}`);
      console.log(`  Días desde FechaApertura               : ${dias ?? 'N/A'}`);
      console.log(`  Gracia máxima (USYNCRO_GRACIA_TAXES_DIAS) : ${usyncroConfig.graciaTaxesDias} días`);

      const [omitidaRows] = await my.query<import('mysql2').RowDataPacket[]>(
        'SELECT motivo, creado_en FROM usyncro_creacion_omitida WHERE numero_referencia = ? LIMIT 1', [numRef]
      );
      if (omitidaRows.length) {
        console.log(`\n  ✘ MARCADA COMO NUNCA SE CREARÁ: ${omitidaRows[0]['motivo']} (desde ${omitidaRows[0]['creado_en']})`);
      } else if (taxesListo) {
        console.log(`\n  ✔ Cumple la regla: SÍ se crearía en Usyncro`);
      } else if (dias !== null && dias > usyncroConfig.graciaTaxesDias) {
        console.log(`\n  ⚠ Ya venció la gracia pero aún no está marcada como omitida (se marcará en la próxima corrida)`);
      } else {
        console.log(`\n  ⏳ Aún en gracia — NO se crea todavía, se reintentará en próximas corridas`);
      }
    }

    // ── 4. Estado en MySQL ────────────────────────────────────────────────
    console.log(`\n${SEP}`);
    console.log('4. ESTADO EN MYSQL (usyncro_registros)');
    console.log(SEP);

    const [regRows] = await my.query<import('mysql2').RowDataPacket[]>(
      'SELECT * FROM usyncro_registros WHERE numero_referencia = ? LIMIT 1', [numRef]
    );
    if (!regRows.length) {
      console.log('  Sin registro — aún no se ha creado ni reservado en Usyncro');
    } else {
      const reg = regRows[0];
      if (!reg['record_id']) {
        console.log('  ⏳ Placeholder reservado (sin record_id) — esperando taxes completo para crear en Usyncro');
      } else {
        console.log(`  record_id          : ${reg['record_id']}`);
      }
      console.log(`  fecha_completado   : ${reg['fecha_completado'] ?? 'null (pendiente)'}`);
    }

    // ── 5. Campos sincronizados ───────────────────────────────────────────
    const [syncRows] = await my.query<import('mysql2').RowDataPacket[]>(
      'SELECT campo, sincronizado, intentos, ultimo_error FROM usyncro_sync_estado WHERE numero_referencia = ? ORDER BY campo',
      [numRef]
    );
    if (syncRows.length) {
      console.log(`\n${SEP}`);
      console.log('5. CAMPOS EN usyncro_sync_estado');
      console.log(SEP);
      for (const s of syncRows) {
        const estado = s['sincronizado'] ? '✔' : `✘ (intentos=${s['intentos']})`;
        const err = s['ultimo_error'] ? ` → ${String(s['ultimo_error']).slice(0, 80)}` : '';
        console.log(`  ${estado} ${s['campo']}${err}`);
      }
    }

    console.log(`\n${'═'.repeat(60)}\n`);

  } catch (err) {
    console.error('[ERROR FATAL]', (err as Error).message);
    process.exit(1);
  } finally {
    if (my) await my.end();
    if (mssqlPool) await mssqlPool.close();
  }
})();
