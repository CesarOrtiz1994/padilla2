// src/jobs/etlFacturas.js - Job ETL para tabla facturas
const sql = require('mssql');
const { Q_FACTURAS, UP_FACTURAS } = require('../queries/facturas');
const { sumar6Horas } = require('../utils/dates');
const { safeMoneyValue } = require('../utils/money');
const { upsertChunks } = require('../services/mysqlHelpers');

/**
 * Ejecuta el ETL para la tabla facturas
 * @param {Object} mssqlPool - Pool de conexión MSSQL
 * @param {Object} mysqlConn - Conexión MySQL
 * @param {Date} desde - Fecha desde para filtrar
 * @returns {Object} Estadísticas de la operación
 */
async function runEtlFacturas(mssqlPool, mysqlConn, desde) {
  const req = new sql.Request(mssqlPool);
  req.input('fApertura', sql.DateTime, desde);
  const rs = await req.query(Q_FACTURAS);
  const rows = rs.recordset;
  const selectedFacturas = rows.length;

  const cleanRows = rows.filter(r => r.id_referencias != null && r.IDFactura != null);
  const droppedFacturas = selectedFacturas - cleanRows.length;

  // Mapear VALUES con transformaciones
  const vals = cleanRows.map(r => ([
    r.id_referencias,
    r.IDFactura,
    r.NumFac,
    sumar6Horas(r.Fecha_c),
    (r.Incoterm ?? r.INCOTER ?? null),
    r.Moneda,
    safeMoneyValue(r.Valor_ME, 'Valor_ME'),
    safeMoneyValue(r.Valor_USD, 'Valor_USD')
  ]));

  const prepared = vals.length;

  // Ejecutar upserts
  const res = prepared
    ? await upsertChunks(mysqlConn, UP_FACTURAS, vals, 1000, { label: 'facturas', idIndex: 0 })
    : { totals: { records: 0, duplicates: 0, warnings: 0, changedRows: 0, affectedRows: 0 }, warningsSummary: {} };

  return {
    stats: res.totals,
    selected: selectedFacturas,
    dropped: droppedFacturas,
    prepared,
    warningsSummary: res.warningsSummary || {}
  };
}

module.exports = { runEtlFacturas };
