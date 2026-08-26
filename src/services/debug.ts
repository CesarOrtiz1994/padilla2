import * as sql from 'mssql';
import type { Connection } from 'mysql2/promise';
import { DEBUG_REF_ID } from '../config/constants';
import { sumar6Horas } from '../utils/dates';

async function debugReferencia(
  mssqlPool: sql.ConnectionPool,
  my: Connection,
  desde: Date
): Promise<void> {
  if (!DEBUG_REF_ID) return;
  try {
    console.log(`DEBUG_REF_ID=${DEBUG_REF_ID}`);
    console.log(`DEBUG ventana: last_dt - ACOLCHADO_DIAS => desde=${desde.toISOString()}`);

    const req = new sql.Request(mssqlPool);
    req.input('id', sql.Int, DEBUG_REF_ID);
    const rsApertura = await req.query<{
      id_referencias: number;
      NumeroDeReferencia: string;
      FechaApertura: Date | null;
      Operacion: number;
      Cancelada: number | null;
    }>(`
      SELECT r.id_referencias, r.NumeroDeReferencia, r.FechaApertura, r.Operacion, r.Cancelada
      FROM referencias r
      WHERE r.id_referencias = @id
    `);
    const ref = rsApertura.recordset[0];

    if (!ref) {
      console.log('DEBUG: La referencia no existe en SQL Server (referencias).');
    } else {
      const fa = ref.FechaApertura ? new Date(ref.FechaApertura) : null;
      console.log(`DEBUG: referencias -> FechaApertura=${fa?.toISOString() ?? null} Operacion=${ref.Operacion} Cancelada=${ref.Cancelada}`);
      console.log(`DEBUG: cae en ventana por apertura? ${fa ? fa > desde : 'sin FechaApertura'}`);
    }

    const reqPed = new sql.Request(mssqlPool);
    reqPed.input('id', sql.Int, DEBUG_REF_ID);
    const rsPed = await reqPed.query<{ hasPedimento: number }>(`
      SELECT TOP 1 1 AS hasPedimento FROM PedimentosEncabezado WHERE id_referencia = @id
    `);
    console.log(`DEBUG: PedimentosEncabezado existe? ${rsPed.recordset.length ? 'SI' : 'NO (y Q_GENERAL usa INNER JOIN)'}`);

    const reqEv = new sql.Request(mssqlPool);
    reqEv.input('id', sql.Int, DEBUG_REF_ID);
    const rsEv = await reqEv.query<{
      FECHA_FAC: Date | null;
      ENTREGA_FAC: Date | null;
      ENTREGA_FAC_CLI: Date | null;
    }>(`
      SELECT
        MAX(CASE WHEN r.Operacion = 1 AND b.IdEvento = 48 THEN b.FechaHoraCapturada
                 WHEN r.Operacion = 2 AND be.IdEvento = 48 THEN be.FechaHoraCapturada END) AS FECHA_FAC,
        MAX(CASE WHEN r.Operacion = 1 AND b.IdEvento = 47 THEN b.FechaHoraCapturada
                 WHEN r.Operacion = 2 AND be.IdEvento = 47 THEN be.FechaHoraCapturada END) AS ENTREGA_FAC,
        MAX(CASE WHEN r.Operacion = 1 AND b.IdEvento = 49 THEN b.FechaHoraCapturada
                 WHEN r.Operacion = 2 AND be.IdEvento = 49 THEN be.FechaHoraCapturada END) AS ENTREGA_FAC_CLI
      FROM referencias r
      LEFT JOIN BitacoraEventosImportacion b ON b.Referencia = r.id_referencias
      LEFT JOIN BitacoraEventosExportacion  be ON be.Referencia = r.id_referencias
      WHERE r.id_referencias = @id
      GROUP BY r.id_referencias
    `);
    const ev = rsEv.recordset[0];
    console.log(`DEBUG: Bitacora IdEvento=48 (FECHA_FAC) -> ${ev?.FECHA_FAC ? new Date(ev.FECHA_FAC).toISOString() : null}`);

    const [myRow] = await my.query<import('mysql2').RowDataPacket[]>(
      'SELECT id_referencias, APERTURA, FECHA_FAC FROM general WHERE id_referencias = ? LIMIT 1',
      [DEBUG_REF_ID]
    );
    const g = myRow[0];
    console.log(`DEBUG: MySQL general -> existe? ${g ? 'SI' : 'NO'}`);
    if (g) {
      console.log(`DEBUG: MySQL general -> APERTURA=${g['APERTURA'] ? new Date(g['APERTURA'] as string).toISOString() : null} FECHA_FAC=${g['FECHA_FAC'] ? new Date(g['FECHA_FAC'] as string).toISOString() : null}`);
    }
  } catch (e) {
    console.error('DEBUG: error debugReferencia:', (e as Error)?.message ?? e);
  }
}

export { debugReferencia };
