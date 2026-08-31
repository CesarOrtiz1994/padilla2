import 'dotenv/config';
import * as sql from 'mssql';
import mysql from 'mysql2/promise';
import { mssqlConfig, mysqlConfig } from '../config/database';
import { CLIENTES_MAP, CUSTOMS_BROKER_NAME } from '../config/integracion';
import { Q_USYNCRO_REFERENCIAS } from '../queries/usyncro';
import { UsyncroClient } from '../services/usyncroClient';
import { getRegistro, insertRegistro, isCampoSincronizado, getCamposSincronizadosSet, marcarCampoOk, marcarCampoError, marcarCompletado, purgarSync } from '../services/usyncroStore';
import { ACOLCHADO_DIAS } from '../config/constants';
import { usyncroConfig } from '../config/usyncro';

interface ReferenciaRow {
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
}

export async function runEtlUsyncro(): Promise<void> {
  let mssqlPool: sql.ConnectionPool | undefined;
  let my: mysql.Connection | undefined;
  const client = new UsyncroClient();
  const startTime = Date.now();

  console.log('[ETL-Usyncro] ================================================');
  console.log('[ETL-Usyncro] INICIANDO SINCRONIZACIÓN CON USYNCRO');
  console.log('[ETL-Usyncro] ================================================');

  try {
    mssqlPool = await sql.connect(mssqlConfig);
    my = await mysql.createConnection(mysqlConfig);
    await my.query("SET time_zone = '-06:00'");

    await client.login();

    // Ventana propia de Usyncro — independiente del acolchado del ETL principal
    const desde = new Date(Date.now() - usyncroConfig.dias * 86400000);

    const req = new sql.Request(mssqlPool);
    req.input('fApertura', sql.DateTime, desde);
    const rs = await req.query<ReferenciaRow>(Q_USYNCRO_REFERENCIAS);
    const referencias = rs.recordset;

    console.log(`[ETL-Usyncro] ${referencias.length} referencias encontradas para los ${CLIENTES_MAP.size} clientes`);

    let creados = 0, yaExistian = 0, errores = 0;

    for (const ref of referencias) {
      const numRef = ref.NumeroDeReferencia;
      const clienteConf = CLIENTES_MAP.get(ref.id_cliente);
      if (!clienteConf) continue;

      try {
        // ── Paso 1: crear record si no existe ────────────────────────────
        let registro = await getRegistro(my, numRef);

        if (!registro) {
          console.log(`[ETL-Usyncro] Creando record para ${numRef}...`);
          const recordId = await client.createRecord(numRef);

          // Obtener actores y places generados automáticamente por el template
          const actores = await client.getActores(recordId);
          const places  = await client.getPlaces(recordId);

          await insertRegistro(my, numRef, ref.id_cliente, recordId, actores, places);
          registro = (await getRegistro(my, numRef))!;
          creados++;
          console.log(`[ETL-Usyncro] Record creado: ${recordId} (actores: ${Object.keys(actores).length}, places: ${Object.keys(places).length})`);
        } else {
          yaExistian++;
        }

        // ── Paso 2: creator.recordReference (solo si no está ya sincronizado) ──
        if (registro.actor_creator_id) {
          const yaHecho = await isCampoSincronizado(my, numRef, 'creator.recordReference');
          if (!yaHecho) {
            try {
              await client.updateActor(registro.record_id, registro.actor_creator_id, {
                recordReference: numRef,
              });
              await marcarCampoOk(my, numRef, 'creator.recordReference');
            } catch (e) {
              await marcarCampoError(my, numRef, 'creator.recordReference', (e as Error).message);
              console.warn(`[ETL-Usyncro] ${numRef} creator update falló: ${(e as Error).message}`);
            }
          }
        }

        // ── Paso 3: buyer (Operacion=1) o supplier (Operacion=2) ────────────
        const actorKey = ref.Operacion === 1 ? 'buyer' : 'supplier';
        const actorId  = actorKey === 'buyer' ? registro.actor_buyer_id : registro.actor_supplier_id;
        const campoSync = `${actorKey}.datos_cliente`;

        if (actorId) {
          const yaHecho = await isCampoSincronizado(my, numRef, campoSync);
          if (!yaHecho) {
            try {
              await client.updateActor(registro.record_id, actorId, {
                name: clienteConf.importador,
                collaborators: [{ email: clienteConf.email, name: clienteConf.nombre, locale: 'es-MX' }],
              });
              await marcarCampoOk(my, numRef, campoSync);
            } catch (e) {
              await marcarCampoError(my, numRef, campoSync, (e as Error).message);
              console.warn(`[ETL-Usyncro] ${numRef} ${actorKey} update falló: ${(e as Error).message}`);
            }
          }
        }

        // ── Paso 4: customsBroker.name (fijo para todos) ─────────────────────
        if (registro.actor_customs_broker_id) {
          const yaHecho = await isCampoSincronizado(my, numRef, 'customsBroker.name');
          if (!yaHecho) {
            try {
              await client.updateActor(registro.record_id, registro.actor_customs_broker_id, {
                name: CUSTOMS_BROKER_NAME,
              });
              await marcarCampoOk(my, numRef, 'customsBroker.name');
            } catch (e) {
              await marcarCampoError(my, numRef, 'customsBroker.name', (e as Error).message);
              console.warn(`[ETL-Usyncro] ${numRef} customsBroker update falló: ${(e as Error).message}`);
            }
          }
        }

        // ── Paso 6: taxes — name + taxesData (campos granulares) ────────────
        if (registro.actor_taxes_id) {
          const TAXES_CAMPOS = ['taxes.name', 'taxes.customsClearance', 'taxes.customsClearanceDate', 'taxes.customsNumber'];
          const yaHechos = await getCamposSincronizadosSet(my, numRef, TAXES_CAMPOS);

          const actorData: Record<string, unknown> = {};
          const taxesData: Record<string, unknown> = {};
          const camposEnviados: string[] = [];

          if (!yaHechos.has('taxes.name') && ref.descripcion_aduana) {
            actorData.name = ref.descripcion_aduana;
            camposEnviados.push('taxes.name');
          }

          // taxesData requiere customsClearanceDate — solo se arma si está disponible
          if (ref.clearance_date) {
            if (!yaHechos.has('taxes.customsClearance') && ref.Primer_Reconocimiento != null) {
              taxesData.customsClearance = ref.Primer_Reconocimiento === 0 ? 'green' : 'red';
              camposEnviados.push('taxes.customsClearance');
            }
            if (!yaHechos.has('taxes.customsClearanceDate')) {
              taxesData.customsClearanceDate = new Date(ref.clearance_date).toISOString().split('T')[0];
              camposEnviados.push('taxes.customsClearanceDate');
            }
            if (!yaHechos.has('taxes.customsNumber')) {
              const year2 = ref.FechaApertura ? new Date(ref.FechaApertura).getFullYear().toString().slice(-2) : null;
              const codigo = ref.codigo_aduana != null ? String(ref.codigo_aduana).trim() : null;
              const patente = ref.patente_agente != null ? String(ref.patente_agente).trim() : null;
              const pedimento = ref.pedimento != null ? String(ref.pedimento).trim() : null;
              if (year2 && codigo && patente && pedimento && pedimento !== '0') {
                taxesData.customsNumber = `${year2} ${codigo} ${patente} ${pedimento}`;
                taxesData.containers = []; taxesData.invoices = []; taxesData.deliveryNotes = [];
                taxesData.goods = []; taxesData.stockKeepingUnits = []; taxesData.notes = '';
                camposEnviados.push('taxes.customsNumber');
              }
            }
          }

          if (Object.keys(taxesData).length > 0) actorData.taxesData = taxesData;

          if (camposEnviados.length > 0) {
            try {
              await client.updateActor(registro.record_id, registro.actor_taxes_id, actorData);
              for (const c of camposEnviados) await marcarCampoOk(my, numRef, c);
            } catch (e) {
              for (const c of camposEnviados) await marcarCampoError(my, numRef, c, (e as Error).message);
              console.warn(`[ETL-Usyncro] ${numRef} taxes update falló: ${(e as Error).message}`);
            }
          }
        }

        // ── Paso 5: destination.name desde aduana ────────────────────────
        if (registro.place_destination_id && ref.descripcion_aduana) {
          const yaHecho = await isCampoSincronizado(my, numRef, 'destination.name');
          if (!yaHecho) {
            try {
              await client.updatePlace(registro.record_id, registro.place_destination_id, {
                name: ref.descripcion_aduana,
              });
              await marcarCampoOk(my, numRef, 'destination.name');
            } catch (e) {
              await marcarCampoError(my, numRef, 'destination.name', (e as Error).message);
              console.warn(`[ETL-Usyncro] ${numRef} destination update falló: ${(e as Error).message}`);
            }
          }
        }

        // ── Paso 7: facturas → POST invoices (PedimentosFacturas en SQL Server) ──
        const facReq = new sql.Request(mssqlPool!);
        facReq.input('idRef', sql.Int, ref.id_referencias);
        const facRs = await facReq.query<{
          IDFactura: number;
          NumeroDeFactura: string | null;
          Fecha: Date | null;
          Moneda: string | null;
          ImporteFacturaME: number | null;
        }>('SELECT IDFactura, NumeroDeFactura, Fecha, Moneda, ImporteFacturaME FROM PedimentosFacturas WHERE IDReferencia = @idRef');
        for (const fac of facRs.recordset) {
          const campo = `invoice.${fac.IDFactura}`;
          if (await isCampoSincronizado(my, numRef, campo)) continue;
          const numFac = fac.NumeroDeFactura?.trim() || null;
          const fechaStr = fac.Fecha ? new Date(fac.Fecha).toISOString().split('T')[0] : null;
          const currency = fac.Moneda?.trim() || null;
          const amount = fac.ImporteFacturaME != null ? Math.round(Number(fac.ImporteFacturaME) * 100) / 100 : null;
          if (!numFac || !fechaStr || !amount || !currency) continue;
          try {
            await client.createInvoice(registro.record_id, {
              number: numFac, date: fechaStr,
              value: { amount: amount.toFixed(2), currency },
              notes: '', providerReference: '', paymentMethod: null,
              incoterm: null, referencePlace: null, tariffHeadings: [],
            });
            await marcarCampoOk(my, numRef, campo);
          } catch (e) {
            await marcarCampoError(my, numRef, campo, (e as Error).message);
            console.warn(`[ETL-Usyncro] ${numRef} invoice ${numFac} falló: ${(e as Error).message}`);
          }
        }

        // ── Paso 6: taxes — name + taxesData (campos granulares) ────────────
        // ── Chequeo de completitud ────────────────────────────────────────
        if (!registro.fecha_completado) {
          const actorKey2 = ref.Operacion === 1 ? 'buyer' : 'supplier';
          const OBLIGATORIOS = [
            'creator.recordReference',
            `${actorKey2}.datos_cliente`,
            'customsBroker.name',
            'destination.name',
            'taxes.name',
            'taxes.customsClearance',
            'taxes.customsClearanceDate',
            'taxes.customsNumber',
          ];
          const completados = await getCamposSincronizadosSet(my, numRef, OBLIGATORIOS);
          if (OBLIGATORIOS.every(c => completados.has(c))) {
            await marcarCompletado(my, numRef);
            console.log(`[ETL-Usyncro] ${numRef} COMPLETADA`);
          }
        }

      } catch (e) {
        console.error(`[ETL-Usyncro] Error en ${numRef}:`, (e as Error).message);
      }
    }

    const dur = ((Date.now() - startTime) / 1000).toFixed(1);

    // Purgar sync_estado de referencias completadas hace más de retentionDias días
    const borrados = await purgarSync(my, usyncroConfig.retentionDias);
    if (borrados > 0) console.log(`[ETL-Usyncro] Purgados ${borrados} campos de sync (retención ${usyncroConfig.retentionDias} días)`);

    console.log('[ETL-Usyncro] ================================================');
    console.log(`[ETL-Usyncro] Duración: ${dur}s | Creados: ${creados} | Ya existían: ${yaExistian} | Errores: ${errores}`);
    console.log('[ETL-Usyncro] ================================================');

  } finally {
    if (my) await my.end();
    if (mssqlPool) await mssqlPool.close();
  }
}
