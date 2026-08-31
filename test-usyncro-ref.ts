import 'dotenv/config';
import * as sql from 'mssql';
import mysql from 'mysql2/promise';
import { mssqlConfig, mysqlConfig } from './src/config/database';
import { CLIENTES_MAP, CUSTOMS_BROKER_NAME } from './src/config/integracion';
import { UsyncroClient } from './src/services/usyncroClient';
import { getRegistro, insertRegistro, isCampoSincronizado, getCamposSincronizadosSet, marcarCampoOk, marcarCampoError } from './src/services/usyncroStore';

const SEP = '═'.repeat(60);
const numRef = process.argv[2];

if (!numRef) {
  console.error('Uso: npx tsx test-usyncro-ref.ts <NumeroDeReferencia>');
  console.error('Ejemplo: npx tsx test-usyncro-ref.ts VE260219-0A');
  process.exit(1);
}

(async () => {
  let mssqlPool: sql.ConnectionPool | undefined;
  let my: mysql.Connection | undefined;

  try {
    console.log(SEP);
    console.log(`TEST USYNCRO — referencia: ${numRef}`);
    console.log(SEP);

    console.log('\n[1] Conectando a bases de datos...');
    mssqlPool = await sql.connect(mssqlConfig);
    console.log('    SQL Server OK');
    my = await mysql.createConnection(mysqlConfig);
    await my.query("SET time_zone = '-06:00'");
    console.log('    MySQL OK');

    const idList = [...CLIENTES_MAP.keys()].join(',');
    console.log(`\n[2] Buscando referencia "${numRef}"...`);

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
        r.NumeroDeReferencia,
        r.id_referencias,
        r.id_cliente,
        r.Operacion,
        r.FechaApertura,
        r.Cancelada,
        r.Primer_Reconocimiento,
        a.descripcion   AS descripcion_aduana,
        a.aduana        AS codigo_aduana,
        aa.patente      AS patente_agente,
        (SELECT TOP 1 pe.Pedimento FROM PedimentosEncabezado pe WHERE pe.id_referencia = r.id_referencias) AS pedimento,
        CASE
          WHEN r.Operacion = 1 THEN (SELECT bi.FechaHoraEvento FROM BitacoraEventosImportacion bi WHERE bi.Referencia = r.id_referencias AND bi.IdEvento = 29)
          WHEN r.Operacion = 2 THEN (SELECT be.FechaHoraEvento FROM BitacoraEventosExportacion be WHERE be.Referencia = r.id_referencias AND be.IdEvento = 29)
        END AS clearance_date
      FROM referencias r
      LEFT JOIN aduana a ON a.id_Aduana = r.id_aduana
      LEFT JOIN agentesaduanales aa ON aa.id_agenteaduanal = r.id_agenteaduanal
      WHERE r.NumeroDeReferencia = @ref
        AND r.id_cliente IN (${idList})
    `);

    if (!rs.recordset.length) {
      console.log(`    ⚠ No se encontró "${numRef}" o no pertenece a los clientes configurados`);
      return;
    }

    const ref = rs.recordset[0];
    const clienteConf = CLIENTES_MAP.get(ref.id_cliente);

    if (!clienteConf) {
      console.log(`    ⚠ id_cliente=${ref.id_cliente} no está en CLIENTES_INTEGRACION`);
      return;
    }

    console.log('\n    ── Datos de la referencia ──────────────────────');
    console.log(`    NumeroDeReferencia : ${ref.NumeroDeReferencia}`);
    console.log(`    id_referencias     : ${ref.id_referencias}`);
    console.log(`    id_cliente         : ${ref.id_cliente}`);
    console.log(`    Operacion          : ${ref.Operacion} (${ref.Operacion === 1 ? 'IMPORTACION' : 'EXPORTACION'})`);
    console.log(`    FechaApertura      : ${ref.FechaApertura?.toISOString() ?? 'null'}`);
    console.log(`    Cancelada          : ${ref.Cancelada}`);
    console.log(`    descripcion_aduana : ${ref.descripcion_aduana ?? 'null'}`);
    console.log(`    codigo_aduana      : ${ref.codigo_aduana ?? 'null'}`);
    console.log(`    patente_agente     : ${ref.patente_agente ?? 'null'}`);
    console.log(`    pedimento          : ${ref.pedimento ?? 'null'}`);
    console.log(`    Primer_Reconocim.  : ${ref.Primer_Reconocimiento ?? 'null'}`);
    console.log(`    clearance_date     : ${ref.clearance_date} (raw DB)`);
    console.log('\n    ── Cliente (integracion.ts) ─────────────────────');
    console.log(`    importador : ${clienteConf.importador}`);
    console.log(`    nombre     : ${clienteConf.nombre}`);
    console.log(`    email      : ${clienteConf.email}`);

    console.log('\n[3] Login en Usyncro...');
    const client = new UsyncroClient();
    await client.login();

    let registro = await getRegistro(my, numRef);

    if (registro) {
      console.log(`\n[4] Record ya existe en MySQL → record_id: ${registro.record_id}`);
    } else {
      console.log('\n[4] Creando record en Usyncro...');
      const recordId = await client.createRecord(numRef);
      console.log(`    ✔ Record creado → ID: ${recordId}`);
      console.log('\n[5] Obteniendo actores...');
      const actores = await client.getActores(recordId);
      console.log('    Actores usados:', { creator: actores['creator'], buyer: actores['buyer'], supplier: actores['supplier'], customsBroker: actores['customsBroker'], taxes: actores['taxes'] });
      console.log('\n[6] Obteniendo places...');
      const places = await client.getPlaces(recordId);
      console.log('    Places usados:', { destination: places['destination'] });
      await insertRegistro(my, numRef, ref.id_cliente, recordId, actores, places);
      registro = (await getRegistro(my, numRef))!;
      console.log('\n[7] Guardado en MySQL ✔');
    }

    // ── [8] creator.recordReference ───────────────────────────────
    console.log(`\n[8] creator.recordReference...`);
    if (registro.actor_creator_id) {
      if (await isCampoSincronizado(my, numRef, 'creator.recordReference')) {
        console.log('    ✔ Ya sincronizado');
      } else {
        try {
          await client.updateActor(registro.record_id, registro.actor_creator_id, { recordReference: numRef });
          await marcarCampoOk(my, numRef, 'creator.recordReference');
          console.log('    ✔ Actualizado');
        } catch (e) {
          await marcarCampoError(my, numRef, 'creator.recordReference', (e as Error).message);
          console.error('    ✘', (e as Error).message);
        }
      }
    }

    // ── [9] buyer / supplier ──────────────────────────────────────
    const actorKey = ref.Operacion === 1 ? 'buyer' : 'supplier';
    const actorId  = actorKey === 'buyer' ? registro.actor_buyer_id : registro.actor_supplier_id;
    console.log(`\n[9] ${actorKey} (${actorId})...`);
    if (actorId) {
      if (await isCampoSincronizado(my, numRef, `${actorKey}.datos_cliente`)) {
        console.log('    ✔ Ya sincronizado');
      } else {
        try {
          await client.updateActor(registro.record_id, actorId, {
            name: clienteConf.importador,
            collaborators: [{ email: clienteConf.email, name: clienteConf.nombre, locale: 'es-MX' }],
          });
          await marcarCampoOk(my, numRef, `${actorKey}.datos_cliente`);
          console.log('    ✔ Actualizado');
        } catch (e) {
          await marcarCampoError(my, numRef, `${actorKey}.datos_cliente`, (e as Error).message);
          console.error('    ✘', (e as Error).message);
        }
      }
    }

    // ── [10] customsBroker ────────────────────────────────────────
    console.log(`\n[10] customsBroker (${registro.actor_customs_broker_id})...`);
    if (registro.actor_customs_broker_id) {
      if (await isCampoSincronizado(my, numRef, 'customsBroker.name')) {
        console.log('    ✔ Ya sincronizado');
      } else {
        try {
          await client.updateActor(registro.record_id, registro.actor_customs_broker_id, { name: CUSTOMS_BROKER_NAME });
          await marcarCampoOk(my, numRef, 'customsBroker.name');
          console.log(`    ✔ ${CUSTOMS_BROKER_NAME}`);
        } catch (e) {
          await marcarCampoError(my, numRef, 'customsBroker.name', (e as Error).message);
          console.error('    ✘', (e as Error).message);
        }
      }
    }

    // ── [11] destination.name ─────────────────────────────────────
    console.log(`\n[11] place destination (${registro.place_destination_id})...`);
    if (registro.place_destination_id && ref.descripcion_aduana) {
      if (await isCampoSincronizado(my, numRef, 'destination.name')) {
        console.log('    ✔ Ya sincronizado');
      } else {
        try {
          await client.updatePlace(registro.record_id, registro.place_destination_id, { name: ref.descripcion_aduana });
          await marcarCampoOk(my, numRef, 'destination.name');
          console.log(`    ✔ ${ref.descripcion_aduana}`);
        } catch (e) {
          await marcarCampoError(my, numRef, 'destination.name', (e as Error).message);
          console.error('    ✘', (e as Error).message);
        }
      }
    } else {
      console.log(`    ⚠ place_destination_id=${registro.place_destination_id} descripcion_aduana=${ref.descripcion_aduana}`);
    }

    // ── [12] taxes ────────────────────────────────────────────────
    console.log(`\n[12] taxes (${registro.actor_taxes_id})...`);
    if (registro.actor_taxes_id) {
      const yaHechos = await getCamposSincronizadosSet(my, numRef, ['taxes.name', 'taxes.taxesData']);
      const actorData: Record<string, unknown> = {};
      const camposEnviados: string[] = [];

      if (!yaHechos.has('taxes.name') && ref.descripcion_aduana) {
        actorData.name = ref.descripcion_aduana;
        camposEnviados.push('taxes.name');
      } else console.log(`    taxes.name: ${yaHechos.has('taxes.name') ? '✔ ya sincronizado' : '⚠ sin datos'}`);

      if (yaHechos.has('taxes.taxesData')) {
        console.log('    taxes.taxesData: ✔ ya sincronizado');
      } else if (!ref.clearance_date) {
        console.log('    taxes.taxesData: ⚠ clearance_date null — quedará pendiente');
      } else if (ref.Primer_Reconocimiento == null) {
        console.log('    taxes.taxesData: ⚠ Primer_Reconocimiento null — quedará pendiente');
      } else {
        const year2 = ref.FechaApertura ? new Date(ref.FechaApertura).getFullYear().toString().slice(-2) : null;
        const codigo = ref.codigo_aduana != null ? String(ref.codigo_aduana).trim() : null;
        const patente = ref.patente_agente != null ? String(ref.patente_agente).trim() : null;
        const pedimento = ref.pedimento != null ? String(ref.pedimento).trim() : null;
        console.log(`    customsNumber → year2=${year2} codigo=${codigo} patente=${patente} pedimento=${pedimento}`);
        if (year2 && codigo && patente && pedimento && pedimento !== '0') {
          actorData.taxesData = {
            customsClearance: ref.Primer_Reconocimiento === 0 ? 'green' : 'red',
            customsClearanceDate: new Date(ref.clearance_date).toISOString().split('T')[0],
            customsNumber: `${year2} ${codigo} ${patente} ${pedimento}`,
            containers: [], invoices: [], deliveryNotes: [],
            goods: [], stockKeepingUnits: [], notes: '',
          };
          camposEnviados.push('taxes.taxesData');
        } else {
          console.log('    ⚠ customsNumber incompleto — quedará pendiente');
        }
      }

      if (camposEnviados.length > 0) {
        console.log('\n    Body enviado:');
        console.log(JSON.stringify({ data: actorData, meta: {} }, null, 4));
        try {
          await client.updateActor(registro.record_id, registro.actor_taxes_id, actorData);
          for (const c of camposEnviados) { await marcarCampoOk(my, numRef, c); console.log(`    ✔ ${c}`); }
        } catch (e) {
          for (const c of camposEnviados) await marcarCampoError(my, numRef, c, (e as Error).message);
          console.error('    ✘', (e as Error).message);
        }
      } else {
        console.log('    Todos los campos ya sincronizados o sin datos disponibles');
      }
    }

    // ── [13] Facturas → POST invoices (PedimentosFacturas en SQL Server) ──────────────
    console.log(`\n[13] Sincronizando facturas (invoices)...`);
    const facReq = new sql.Request(mssqlPool!);
    facReq.input('idRef', sql.Int, ref.id_referencias);
    const facRs = await facReq.query<{
      IDFactura: number;
      NumeroDeFactura: string | null;
      Fecha: Date | null;
      Moneda: string | null;
      ImporteFacturaME: number | null;
    }>('SELECT IDFactura, NumeroDeFactura, Fecha, Moneda, ImporteFacturaME FROM PedimentosFacturas WHERE IDReferencia = @idRef');
    console.log(`    ${facRs.recordset.length} facturas encontradas`);
    for (const fac of facRs.recordset) {
      const campo = `invoice.${fac.IDFactura}`;
      const yaHecho = await isCampoSincronizado(my, numRef, campo);
      const numFac = fac.NumeroDeFactura?.trim() || null;
      const fechaStr = fac.Fecha ? new Date(fac.Fecha).toISOString().split('T')[0] : null;
      const currency = fac.Moneda?.trim() || null;
      const amount = fac.ImporteFacturaME != null ? Math.round(Number(fac.ImporteFacturaME) * 100) / 100 : null;

      if (yaHecho) { console.log(`    ✔ ${numFac} ya sincronizado`); continue; }
      console.log(`    Factura: ${numFac} | fecha=${fechaStr} | amount=${amount} | currency=${currency}`);

      if (!numFac || !fechaStr || !amount || !currency) {
        console.log(`    ⚠ Datos incompletos — quedará pendiente`); continue;
      }
      const body = {
        number: numFac, date: fechaStr,
        value: { amount: amount.toFixed(2), currency },
        notes: '', providerReference: '', paymentMethod: null,
        incoterm: null, referencePlace: null, tariffHeadings: [],
      };
      console.log('    Body:', JSON.stringify(body));
      try {
        await client.createInvoice(registro.record_id, body);
        await marcarCampoOk(my, numRef, campo);
        console.log(`    ✔ Invoice creado: ${numFac}`);
      } catch (e) {
        await marcarCampoError(my, numRef, campo, (e as Error).message);
        console.error(`    ✘ ${numFac}:`, (e as Error).message);
      }
    }

    console.log('\n' + SEP);
    console.log(`RECORD ID: ${registro.record_id}`);
    console.log(`URL: ${process.env.USYNCRO_API_URL?.replace('/api/v1', '')}/records/${registro.record_id}`);
    console.log(SEP);

  } catch (err) {
    console.error('\n[ERROR FATAL]', (err as Error).message);
    process.exit(1);
  } finally {
    if (my) await my.end();
    if (mssqlPool) await mssqlPool.close();
  }
})();
