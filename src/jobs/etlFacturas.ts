import * as sql from 'mssql';
import type { Connection } from 'mysql2/promise';
import { Q_FACTURAS, UP_FACTURAS } from '../queries/facturas';
import { sumar6Horas } from '../utils/dates';
import { safeMoneyValue } from '../utils/money';
import { upsertChunks } from '../services/mysqlHelpers';
import type { FacturaRow, EtlFacturasResult } from '../types';

async function runEtlFacturas(
  mssqlPool: sql.ConnectionPool,
  mysqlConn: Connection,
  desde: Date
): Promise<EtlFacturasResult> {
  const req = new sql.Request(mssqlPool);
  req.input('fApertura', sql.DateTime, desde);
  const rs = await req.query<FacturaRow>(Q_FACTURAS);
  const rows = rs.recordset;
  const selectedFacturas = rows.length;

  const cleanRows = rows.filter(r => r.id_referencias != null && r.IDFactura != null);
  const droppedFacturas = selectedFacturas - cleanRows.length;

  const vals = cleanRows.map(r => [
    r.id_referencias,
    r.IDFactura,
    r.NumFac,
    sumar6Horas(r.Fecha_c),
    r.Incoterm ?? r.INCOTER ?? null,
    r.Moneda,
    safeMoneyValue(r.Valor_ME, 'Valor_ME'),
    safeMoneyValue(r.Valor_USD, 'Valor_USD'),
  ]);

  const prepared = vals.length;

  const res = prepared
    ? await upsertChunks(mysqlConn, UP_FACTURAS, vals, 1000, { label: 'facturas', idIndex: 0 })
    : { totals: { records: 0, duplicates: 0, warnings: 0, changedRows: 0, affectedRows: 0 }, warningsSummary: {} };

  return {
    stats: res.totals,
    selected: selectedFacturas,
    dropped: droppedFacturas,
    prepared,
    warningsSummary: res.warningsSummary ?? {},
  };
}

export { runEtlFacturas };
