// src/jobs/etlGeneral.js - Job ETL para tabla general
const sql = require('mssql');
const { Q_GENERAL, UP_GENERAL } = require('../queries/general');
const { sumar6Horas } = require('../utils/dates');
const { safeMoneyValue } = require('../utils/money');
const { upsertChunks } = require('../services/mysqlHelpers');
const { DEBUG_REF_ID } = require('../config/constants');

/**
 * Ejecuta el ETL para la tabla general
 * @param {Object} mssqlPool - Pool de conexión MSSQL
 * @param {Object} mysqlConn - Conexión MySQL
 * @param {Date} desde - Fecha desde para filtrar
 * @returns {Object} Estadísticas de la operación
 */
async function runEtlGeneral(mssqlPool, mysqlConn, desde) {
  const req = new sql.Request(mssqlPool);
  req.input('fApertura', sql.DateTime, desde);
  const rs = await req.query(Q_GENERAL);
  const rows = rs.recordset;
  const selectedGeneral = rows.length;

  if (DEBUG_REF_ID) {
    const dbg = rows.find(r => Number(r.id_referencias) === DEBUG_REF_ID);
    if (!dbg) {
      console.log(`DEBUG: Q_GENERAL NO incluyó id_referencias=${DEBUG_REF_ID}`);
    } else {
      console.log(`DEBUG: Q_GENERAL incluyó id_referencias=${DEBUG_REF_ID}`);
      console.log(`DEBUG: Q_GENERAL -> APERTURA=${dbg.APERTURA ? new Date(dbg.APERTURA).toISOString() : null}`);
    }
  }

  const cleanRows = rows.filter(r => r.id_referencias != null);
  const droppedGeneral = selectedGeneral - cleanRows.length;

  // Mapear VALUES con transformaciones
  const vals = cleanRows.map(r => ([
    r.NumeroDeReferencia,
    r.id_referencias,
    r.Pedimento,
    r.Operacion,
    r.Clave_pedimento,
    r.a_despacho,
    r.a_llegada,
    r.C_Imp_Exp,
    r.facturada,
    r.Facturar_a,
    r.Agente_Aduanal,
    r.Ejecutivo,
    r.medio_trasporte,
    sumar6Horas(r.APERTURA),
    sumar6Horas(r.LLEGADA_MERCAN),
    sumar6Horas(r.ENTREGA_CLASIFICA),
    sumar6Horas(r.INICIO_CLASIFICA),
    sumar6Horas(r.TERMINO_CLASIFICA),
    sumar6Horas(r.INICIO_GLOSA),
    sumar6Horas(r.TERMINO_GLOSA),
    sumar6Horas(r.ENTREGA_GLOSA),
    sumar6Horas(r.PAGO_PEDIMENTO),
    sumar6Horas(r.DESPACHO_MERCAN),
    sumar6Horas(r.ENTREGA_FAC),
    sumar6Horas(r.FECHA_FAC),
    sumar6Horas(r.ENTREGA_FAC_CLI),
    sumar6Horas(r.ENTREGA_CAPTURA),
    sumar6Horas(r.INICIO_CAPTURA),
    sumar6Horas(r.TERMINO_CAPTURA),
    sumar6Horas(r.PRIMER_RECONOCIMIENTO),
    safeMoneyValue(r.Total_Adv, 'Total_Adv'),
    safeMoneyValue(r.Total_DTA, 'Total_DTA'),
    safeMoneyValue(r.Total_IVA, 'Total_IVA'),
    safeMoneyValue(r.Total_Imp, 'Total_Imp'),
    r.Cancelada
  ]));

  const prepared = vals.length;

  // Ejecutar upserts
  const res = prepared
    ? await upsertChunks(mysqlConn, UP_GENERAL, vals, 1000, { label: 'general', idIndex: 1 })
    : { totals: { records: 0, duplicates: 0, warnings: 0, changedRows: 0, affectedRows: 0 } };

  // Calcular máxima fecha de apertura para checkpoint
  let maxApertura = null;
  for (const r of cleanRows) {
    if (r.APERTURA && (!maxApertura || r.APERTURA > maxApertura)) maxApertura = r.APERTURA;
  }

  return {
    stats: res.totals,
    selected: selectedGeneral,
    dropped: droppedGeneral,
    prepared,
    maxApertura,
    warningsSummary: res.warningsSummary || {}
  };
}

module.exports = { runEtlGeneral };
