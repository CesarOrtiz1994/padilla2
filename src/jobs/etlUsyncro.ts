import 'dotenv/config';
import * as sql from 'mssql';
import mysql from 'mysql2/promise';
import { mssqlConfig, mysqlConfig } from '../config/database';
import { CLIENTES_MAP, CUSTOMS_BROKER_NAME } from '../config/integracion';
import { Q_USYNCRO_REFERENCIAS, qUsyncroReferenciasPorLista } from '../queries/usyncro';
import { UsyncroClient } from '../services/usyncroClient';
import { getRegistro, insertRegistro, isCampoSincronizado, getCamposSincronizadosSet, marcarCampoOk, marcarCampoError, marcarCampoOmitido, marcarCompletado, purgarSync, getReferenciasPendientes, estaCreacionOmitida, marcarCreacionOmitida, insertPendienteCreacion, eliminarRegistroPendiente } from '../services/usyncroStore';
import { esReferenciaParcial, diasTranscurridos, armarCustomsNumber } from '../utils/referencias';
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

// soloReferencias: si se especifica, procesa únicamente esas referencias ignorando
// el filtro de FechaApertura (misma consulta usada para rescatar pendientes).
export async function runEtlUsyncro(soloReferencias?: string[]): Promise<void> {
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

    let referencias: ReferenciaRow[];

    if (soloReferencias && soloReferencias.length > 0) {
      console.log(`[ETL-Usyncro] Modo forzado: ${soloReferencias.length} referencia(s) específica(s), ignorando filtro de FechaApertura`);
      const req0 = new sql.Request(mssqlPool);
      soloReferencias.forEach((r, idx) => req0.input(`ref${idx}`, sql.VarChar, r));
      const rs0 = await req0.query<ReferenciaRow>(qUsyncroReferenciasPorLista(soloReferencias.length));
      referencias = rs0.recordset;
    } else {
      // Ventana propia de Usyncro — independiente del acolchado del ETL principal
      const desde = new Date(Date.now() - usyncroConfig.dias * 86400000);

      const req = new sql.Request(mssqlPool);
      req.input('fApertura', sql.DateTime, desde);
      const rs = await req.query<ReferenciaRow>(Q_USYNCRO_REFERENCIAS);
      referencias = rs.recordset;

      // Rescate: referencias ya creadas fuera de la ventana que siguen sin completar
      const enVentana = new Set(referencias.map(r => r.NumeroDeReferencia));
      const pendientes = (await getReferenciasPendientes(my, usyncroConfig.graciaInvoicesDias)).filter(r => !enVentana.has(r));
      for (let i = 0; i < pendientes.length; i += 200) {
        const lote = pendientes.slice(i, i + 200);
        const pendReq = new sql.Request(mssqlPool);
        lote.forEach((r, idx) => pendReq.input(`ref${idx}`, sql.VarChar, r));
        const pendRs = await pendReq.query<ReferenciaRow>(qUsyncroReferenciasPorLista(lote.length));
        referencias.push(...pendRs.recordset);
      }
      if (pendientes.length > 0) console.log(`[ETL-Usyncro] ${pendientes.length} referencias pendientes fuera de ventana re-encoladas`);
    }

    console.log(`[ETL-Usyncro] ${referencias.length} referencias encontradas para los ${CLIENTES_MAP.size} clientes`);

    let creados = 0, yaExistian = 0, errores = 0;

    for (const ref of referencias) {
      const numRef = ref.NumeroDeReferencia;
      if (numRef.startsWith('QRV')) continue; // referencias QRV se omiten por completo de Usyncro
      const clienteConf = CLIENTES_MAP.get(ref.id_cliente);
      if (!clienteConf) continue;

      try {
        // ── Paso 1: crear record si no existe ────────────────────────────
        let registro = await getRegistro(my, numRef);

        if (!registro || !registro.record_id) {
          // Referencias parciales (-0A, -0B...) no se crean en Usyncro hasta tener taxes completo
          if (esReferenciaParcial(numRef)) {
            if (await estaCreacionOmitida(my, numRef)) continue;

            const customsNumber = armarCustomsNumber({
              fechaApertura: ref.FechaApertura,
              codigoAduana: ref.codigo_aduana,
              patenteAgente: ref.patente_agente,
              pedimento: ref.pedimento,
            });
            const taxesListo = !!customsNumber && !!ref.clearance_date;

            if (!taxesListo) {
              const dias = diasTranscurridos(ref.FechaApertura);
              if (dias !== null && dias > usyncroConfig.graciaTaxesDias) {
                const motivo = `referencia parcial sin taxes completo tras ${dias} días`;
                await marcarCreacionOmitida(my, numRef, motivo);
                await eliminarRegistroPendiente(my, numRef);
                console.log(`[ETL-Usyncro] ${numRef} NUNCA SE CREARÁ EN USYNCRO: ${motivo}`);
              } else if (!registro) {
                // Placeholder: la mantiene en el rescate de pendientes sin depender de la ventana
                await insertPendienteCreacion(my, numRef, ref.id_cliente);
                console.log(`[ETL-Usyncro] ${numRef} en espera de taxes completo antes de crear (día ${dias ?? '?'})`);
              } else {
                console.log(`[ETL-Usyncro] ${numRef} en espera de taxes completo antes de crear (día ${dias ?? '?'})`);
              }
              continue;
            }
          }

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

        // Nunca debería ocurrir: si sigue sin record_id, el Paso 1 ya hizo `continue`
        if (!registro.record_id) continue;
        const recordIdActivo = registro.record_id;

        // ── Paso 2: creator.recordReference (solo si no está ya sincronizado) ──
        if (registro.actor_creator_id) {
          const yaHecho = await isCampoSincronizado(my, numRef, 'creator.recordReference');
          if (!yaHecho) {
            try {
              await client.updateActor(recordIdActivo, registro.actor_creator_id, {
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
              await client.updateActor(recordIdActivo, actorId, {
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
              await client.updateActor(recordIdActivo, registro.actor_customs_broker_id, {
                name: CUSTOMS_BROKER_NAME,
              });
              await marcarCampoOk(my, numRef, 'customsBroker.name');
            } catch (e) {
              await marcarCampoError(my, numRef, 'customsBroker.name', (e as Error).message);
              console.warn(`[ETL-Usyncro] ${numRef} customsBroker update falló: ${(e as Error).message}`);
            }
          }
        }

        // ── Paso 6a: taxes.name (separado de taxesData) ──────────────────────
        if (registro.actor_taxes_id) {
          if (await isCampoSincronizado(my, numRef, 'taxes.name')) {
            console.log(`[ETL-Usyncro] ${numRef} taxes.name ya sincronizado`);
          } else if (ref.descripcion_aduana) {
            try {
              await client.updateActor(recordIdActivo, registro.actor_taxes_id, { name: ref.descripcion_aduana });
              await marcarCampoOk(my, numRef, 'taxes.name');
              console.log(`[ETL-Usyncro] ${numRef} taxes.name OK: ${ref.descripcion_aduana}`);
            } catch (e) {
              await marcarCampoError(my, numRef, 'taxes.name', (e as Error).message);
              console.warn(`[ETL-Usyncro] ${numRef} taxes.name falló: ${(e as Error).message}`);
            }
          } else {
            const dias = diasTranscurridos(ref.FechaApertura);
            if (esReferenciaParcial(numRef) && dias !== null && dias > usyncroConfig.graciaTaxesDias) {
              const motivo = `referencia parcial sin aduana tras ${dias} días`;
              await marcarCampoOmitido(my, numRef, 'taxes.name', motivo);
              console.log(`[ETL-Usyncro] ${numRef} taxes.name OMITIDO: ${motivo}`);
            }
          }
        }

        // ── Paso 6b: taxes taxesData — requiere clearance_date Y customsNumber ─
        if (registro.actor_taxes_id) {
          const TAXESDATA_CAMPOS = ['taxes.customsClearance', 'taxes.customsClearanceDate', 'taxes.customsNumber'];
          const yaHechos = await getCamposSincronizadosSet(my, numRef, TAXESDATA_CAMPOS);
          const todosHechos = TAXESDATA_CAMPOS.every(c => yaHechos.has(c));
          if (!todosHechos) {
            const customsNumber = armarCustomsNumber({
              fechaApertura: ref.FechaApertura,
              codigoAduana: ref.codigo_aduana,
              patenteAgente: ref.patente_agente,
              pedimento: ref.pedimento,
            });

            const dias = diasTranscurridos(ref.FechaApertura);
            const venceGracia = esReferenciaParcial(numRef) && dias !== null && dias > usyncroConfig.graciaTaxesDias;

            if (!ref.clearance_date || !customsNumber) {
              if (venceGracia) {
                const motivo = `referencia parcial sin ${!customsNumber ? 'pedimento/customsNumber' : 'clearance_date'} tras ${dias} días`;
                for (const c of TAXESDATA_CAMPOS) await marcarCampoOmitido(my, numRef, c, motivo);
                console.log(`[ETL-Usyncro] ${numRef} taxes taxesData OMITIDO: ${motivo}`);
              } else {
                console.log(`[ETL-Usyncro] ${numRef} taxes taxesData pendiente: ${!ref.clearance_date ? 'sin clearance_date' : 'customsNumber sin datos completos'}`);
              }
            } else {
              const taxesData: Record<string, unknown> = {
                customsClearance: ref.Primer_Reconocimiento === 1 ? 'red' : 'green',
                customsClearanceDate: new Date(ref.clearance_date).toISOString().split('T')[0],
                customsNumber,
                containers: [], invoices: [], deliveryNotes: [],
                goods: [], stockKeepingUnits: [], notes: '',
              };
              try {
                await client.updateActor(recordIdActivo, registro.actor_taxes_id, { taxesData });
                for (const c of TAXESDATA_CAMPOS) await marcarCampoOk(my, numRef, c);
                console.log(`[ETL-Usyncro] ${numRef} taxes taxesData OK`);
              } catch (e) {
                for (const c of TAXESDATA_CAMPOS) await marcarCampoError(my, numRef, c, (e as Error).message);
                console.warn(`[ETL-Usyncro] ${numRef} taxes taxesData falló: ${(e as Error).message}`);
              }
            }
          }
        }

        // ── Paso 5: destination.name desde aduana ────────────────────────
        if (registro.place_destination_id) {
          const yaHecho = await isCampoSincronizado(my, numRef, 'destination.name');
          if (!yaHecho && ref.descripcion_aduana) {
            try {
              await client.updatePlace(recordIdActivo, registro.place_destination_id, {
                name: ref.descripcion_aduana,
              });
              await marcarCampoOk(my, numRef, 'destination.name');
            } catch (e) {
              await marcarCampoError(my, numRef, 'destination.name', (e as Error).message);
              console.warn(`[ETL-Usyncro] ${numRef} destination update falló: ${(e as Error).message}`);
            }
          } else if (!yaHecho) {
            const dias = diasTranscurridos(ref.FechaApertura);
            if (esReferenciaParcial(numRef) && dias !== null && dias > usyncroConfig.graciaTaxesDias) {
              await marcarCampoOmitido(my, numRef, 'destination.name', `referencia parcial sin aduana tras ${dias} días`);
              console.log(`[ETL-Usyncro] ${numRef} destination.name OMITIDO`);
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
            await client.createInvoice(recordIdActivo, {
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
