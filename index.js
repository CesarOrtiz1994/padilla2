// index.js
require('dotenv').config();
const sql = require('mssql');
const mysql = require('mysql2/promise');

// ---------- Config de conexiones ----------
const mssqlConfig = {
  server: process.env.MSSQL_SERVER,
  database: process.env.MSSQL_DB,
  user: process.env.MSSQL_USER,
  password: process.env.MSSQL_PASS,
  port: Number(process.env.MSSQL_PORT || 1433),

  connectionTimeout: Number(process.env.MSSQL_CONN_TIMEOUT_MS || 30000),
  requestTimeout:    Number(process.env.MSSQL_REQUEST_TIMEOUT_MS || 300000),

  pool: { max: 5, min: 0, idleTimeoutMillis: 30000 },

  options: {
    encrypt: true,               // si usas Azure SQL => true
    trustServerCertificate: true // útil on-prem sin CA
  }
};

// ----- MySQL conn -----
const mysqlConfig = {
  host: process.env.MYSQL_HOST1,
  user: process.env.MYSQL_USER1,
  password: process.env.MYSQL_PASS1,
  database: process.env.MYSQL_DB1,
  port: Number(process.env.MYSQL_PORT1 || 3306)
};

const ACOLCHADO_DIAS = Number(process.env.ACOLCHADO_DIAS || 70);

// ---------- Checkpoint: SOLO leer/actualizar (ya existe) ----------
async function getCheckpoint(conn) {
  const [rows] = await conn.query(
    "SELECT last_dt FROM sync_checkpoint WHERE name='apertura_activos' LIMIT 1"
  );
  if (!rows.length || !rows[0].last_dt) {
    return new Date('2024-01-01T00:00:00Z');
  }
  return rows[0].last_dt;
}

async function setCheckpoint(conn, dt) {
  await conn.query(
    "UPDATE sync_checkpoint SET last_dt=? WHERE name='apertura_activos'",
    [dt]
  );
}

// ---------- Utilidades ----------
function chunk(arr, size) {
  const out = [];
  for (let i = 0; i < arr.length; i += size) out.push(arr.slice(i, i + size));
  return out;
}

// Función para convertir valores money a números para MySQL
// Esta versión preserva los valores originales sin limitarlos
function safeMoneyValue(value, fieldName = 'desconocido') {
  try {
    // Caso especial: si es null o undefined
    if (value === null || value === undefined) {
      return null; // Mantener null para preservar la semántica original
    }
    
    // Si es un objeto (como en algunos resultados de SQL Server)
    if (typeof value === 'object') {
      console.log(`Campo ${fieldName}: Valor es un objeto, intentando convertir:`, value);
      // Intentar extraer un valor numérico si existe
      if (value.value !== undefined) {
        value = value.value;
      } else {
        console.log(`Campo ${fieldName}: No se pudo extraer valor del objeto, usando null`);
        return null;
      }
    }
    
    // Convertir a número si es string
    let numValue;
    if (typeof value === 'string') {
      // Eliminar caracteres no numéricos excepto punto decimal y signo negativo
      const cleanValue = value.replace(/[^\d.-]/g, '');
      numValue = parseFloat(cleanValue);
    } else {
      numValue = Number(value);
    }
    
    // Verificar si es un número válido
    if (isNaN(numValue)) {
      console.log(`Campo ${fieldName}: Valor no numérico: "${value}", usando null`);
      return null;
    }
    
    // Registrar valores extremadamente grandes pero no modificarlos
    if (Math.abs(numValue) > 1000000) { // Si es mayor a un millón
      console.log(`Campo ${fieldName}: Valor monetario extremo detectado: ${numValue}, manteniendo valor original`);
    }
    
    // Devolver el valor original sin modificar
    return numValue;
  } catch (err) {
    console.error(`Error procesando valor monetario para ${fieldName}:`, err);
    return null; // Valor por defecto en caso de error
  }
}

// Parsear OkPacket para obtener Records / Duplicates / Warnings
function parseOkPacket(ok, fallbackRecords = 0) {
  // mysql2 usa 'info' (no 'message') para: "Records: N  Duplicates: M  Warnings: W"
  const info = (ok?.info || ok?.message || '').replace(/,/g, '');
  let records = fallbackRecords, duplicates = 0, warnings = Number(ok?.warningStatus || 0);

  const m = /Records:\s*(\d+)\s*Duplicates:\s*(\d+)\s*Warnings:\s*(\d+)/i.exec(info);
  if (m) {
    records    = Number(m[1] || 0);
    duplicates = Number(m[2] || 0);
    warnings   = Number(m[3] || 0);
  }

  const changedRows  = Number(ok?.changedRows  || 0); // updates que realmente cambiaron algo
  const affectedRows = Number(ok?.affectedRows || 0); // no lo usamos para métricas finales
  return { records, duplicates, warnings, changedRows, affectedRows, rawInfo: info };
}

// UPSERT por lotes + captura warnings por batch
async function upsertChunks(conn, query, data, size = 1000) {
  const totals = { records: 0, duplicates: 0, warnings: 0, changedRows: 0, affectedRows: 0 };
  const warningBatches = []; // [{batchIndex, warnings: [...] }]

  for (let i = 0; i < data.length; i += size) {
    const part = data.slice(i, i + size);
    const [ok] = await conn.query(query, [part]);
    const s = parseOkPacket(ok, part.length);

    totals.records      += s.records;
    totals.duplicates   += s.duplicates;
    totals.warnings     += s.warnings;
    totals.changedRows  += s.changedRows;
    totals.affectedRows += s.affectedRows;

    if (s.warnings > 0) {
      const [warnRows] = await conn.query('SHOW WARNINGS LIMIT 50');
      warningBatches.push({ batchIndex: Math.floor(i / size), warnings: warnRows });
    }
  }
  return { totals, warningBatches };
}

// ---------- Queries (como los tenías) ----------
const Q_GENERAL = `
SELECT
  r.NumeroDeReferencia,
  r.id_referencias,
  p.Pedimento,
  r.Operacion,
  re.regimen            AS Clave_pedimento,
  a_origen.descripcion  AS a_despacho,
  a_llegada.descripcion AS a_llegada,
  c_i.nombre            AS C_Imp_Exp,
  c_f.nombre            AS Facturar_a,
  aa.nombre             AS Agente_Aduanal,
  u.nombre              AS Ejecutivo,
  mt.descripcion        AS medio_trasporte,
  r.FechaApertura       AS APERTURA,
  MAX(CASE 
      WHEN r.Operacion = 1 AND b.IdEvento = 6 THEN b.FechaHoraCapturada 
      WHEN r.Operacion = 2 AND be.IdEvento = 6 THEN be.FechaHoraCapturada 
  END) AS LLEGADA_MERCAN,
  MAX(CASE 
      WHEN r.Operacion = 1 AND b.IdEvento = 18 THEN b.FechaHoraCapturada 
      WHEN r.Operacion = 2 AND be.IdEvento = 18 THEN be.FechaHoraCapturada 
  END) AS ENTREGA_CLASIFICA,
  MAX(CASE 
      WHEN r.Operacion = 1 AND b.IdEvento = 19 THEN b.FechaHoraCapturada 
      WHEN r.Operacion = 2 AND be.IdEvento = 19 THEN be.FechaHoraCapturada 
  END) AS INICIO_CLASIFICA,
  MAX(CASE 
      WHEN r.Operacion = 1 AND b.IdEvento = 20 THEN b.FechaHoraCapturada 
      WHEN r.Operacion = 2 AND be.IdEvento = 20 THEN be.FechaHoraCapturada 
  END) AS TERMINO_CLASIFICA,
  MAX(CASE 
      WHEN r.Operacion = 1 AND b.IdEvento = 69 THEN b.FechaHoraCapturada 
      WHEN r.Operacion = 2 AND be.IdEvento = 69 THEN be.FechaHoraCapturada 
  END) AS INICIO_GLOSA,
  MAX(CASE 
      WHEN r.Operacion = 1 AND b.IdEvento = 70 THEN b.FechaHoraCapturada 
      WHEN r.Operacion = 2 AND be.IdEvento = 70 THEN be.FechaHoraCapturada 
  END) AS TERMINO_GLOSA,
  MAX(CASE 
      WHEN r.Operacion = 1 AND b.IdEvento = 22 THEN b.FechaHoraCapturada 
      WHEN r.Operacion = 2 AND be.IdEvento = 22 THEN be.FechaHoraCapturada 
  END) AS ENTREGA_GLOSA,
  MAX(CASE 
      WHEN r.Operacion = 1 AND b.IdEvento = 29 THEN b.FechaHoraCapturada 
      WHEN r.Operacion = 2 AND be.IdEvento = 29 THEN be.FechaHoraCapturada 
  END) AS PAGO_PEDIMENTO,
  MAX(CASE 
      WHEN r.Operacion = 1 AND b.IdEvento = 32 THEN b.FechaHoraCapturada 
      WHEN r.Operacion = 2 AND be.IdEvento = 32 THEN be.FechaHoraCapturada 
  END) AS DESPACHO_MERCAN,
  MAX(CASE 
      WHEN r.Operacion = 1 AND b.IdEvento = 47 THEN b.FechaHoraCapturada 
      WHEN r.Operacion = 2 AND be.IdEvento = 47 THEN be.FechaHoraCapturada 
  END) AS ENTREGA_FAC,
  MAX(CASE 
      WHEN r.Operacion = 1 AND b.IdEvento = 48 THEN b.FechaHoraCapturada 
      WHEN r.Operacion = 2 AND be.IdEvento = 48 THEN be.FechaHoraCapturada 
  END) AS FECHA_FAC,
  MAX(CASE 
      WHEN r.Operacion = 1 AND b.IdEvento = 49 THEN b.FechaHoraCapturada 
      WHEN r.Operacion = 2 AND be.IdEvento = 49 THEN be.FechaHoraCapturada 
  END) AS ENTREGA_FAC_CLI,
  MAX(CASE 
      WHEN r.Operacion = 1 AND b.IdEvento = 26 THEN b.FechaHoraCapturada 
      WHEN r.Operacion = 2 AND be.IdEvento = 26 THEN be.FechaHoraCapturada 
  END) AS ENTREGA_CAPTURA,
  MAX(CASE 
      WHEN r.Operacion = 1 AND b.IdEvento = 33 THEN b.FechaHoraCapturada 
      WHEN r.Operacion = 2 AND be.IdEvento = 33 THEN be.FechaHoraCapturada 
  END) AS INICIO_CAPTURA,
  MAX(CASE 
      WHEN r.Operacion = 1 AND b.IdEvento = 42 THEN b.FechaHoraCapturada 
      WHEN r.Operacion = 2 AND be.IdEvento = 42 THEN be.FechaHoraCapturada 
  END) AS TERMINO_CAPTURA,
  MAX(CASE 
      WHEN r.Operacion = 1 AND b.IdEvento = 36 THEN b.FechaHoraCapturada 
      WHEN r.Operacion = 2 AND be.IdEvento = 36 THEN be.FechaHoraCapturada 
  END) AS PRIMER_RECONOCIMIENTO,
  MAX(p.ADV1)           AS Total_Adv,
  MAX(p.DTA1)           AS Total_DTA,
  MAX(p.IVA1)           AS Total_IVA,
  MAX(p.TOTALIMPUESTOS) AS Total_Imp,
  r.Cancelada           AS Cancelada
FROM referencias r
INNER JOIN PedimentosEncabezado p ON p.id_referencia = r.id_referencias
LEFT JOIN regimen re ON re.id_regimen = r.id_regimen
LEFT JOIN aduana a_origen ON a_origen.id_Aduana = r.id_aduana
LEFT JOIN aduana a_llegada ON a_llegada.id_Aduana = r.Id_AduanaLlegada
LEFT JOIN clientes c_i ON c_i.id_cliente = r.id_cliente
LEFT JOIN clientes c_f ON c_f.id_cliente = r.concargo
LEFT JOIN agentesaduanales aa ON aa.id_agenteaduanal = r.id_agenteaduanal
LEFT JOIN usuarios u ON u.id_usuario = r.IdEjecutivo
LEFT JOIN MediosDeTransporte mt ON mt.IDMedioDeTransporte = p.IDTransporteEnt_Sal
LEFT JOIN BitacoraEventosImportacion b ON b.Referencia = r.id_referencias
LEFT JOIN BitacoraEventosExportacion be ON be.Referencia = r.id_referencias
WHERE r.FechaApertura > @fApertura
GROUP BY
  r.NumeroDeReferencia, r.id_referencias, p.Pedimento, r.Operacion, re.regimen,
  a_origen.descripcion, a_llegada.descripcion, c_i.nombre, c_f.nombre,
  aa.nombre, u.nombre, mt.descripcion, r.FechaApertura, r.Cancelada
`;

const Q_FACTURAS = `
SELECT
  r.id_referencias,
  pf.IDFactura,
  pf.NumeroDeFactura AS NumFac,
  pf.Fecha           AS Fecha_c,
  pf.IDIncoter       AS Incoterm,
  pf.Moneda          AS Moneda,
  pf.ImporteFacturaME AS Valor_ME,
  pf.ImporteFacturaUS AS Valor_USD
FROM referencias r
INNER JOIN PedimentosFacturas pf ON r.id_referencias = pf.IDReferencia
WHERE r.FechaApertura > @fApertura
`;

// ---------- UPSERTS MySQL ----------
const UP_GENERAL = `
INSERT INTO general (
  NumeroDeReferencia, id_referencias, Pedimento, Operacion, Clave_pedimento, a_despacho, a_llegada,
  C_Imp_Exp, Facturar_a, Agente_Aduanal, Ejecutivo, medio_trasporte, APERTURA,
  LLEGADA_MERCAN, ENTREGA_CLASIFICA, INICIO_CLASIFICA, TERMINO_CLASIFICA,
  INICIO_GLOSA, TERMINO_GLOSA, ENTREGA_GLOSA, PAGO_PEDIMENTO, DESPACHO_MERCAN,
  ENTREGA_FAC, FECHA_FAC, ENTREGA_FAC_CLI, ENTREGA_CAPTURA, INICIO_CAPTURA, 
  TERMINO_CAPTURA, PRIMER_RECONOCIMIENTO, Total_Adv, Total_DTA, Total_IVA, Total_Imp, Cancelada
) VALUES ?
ON DUPLICATE KEY UPDATE
  NumeroDeReferencia=VALUES(NumeroDeReferencia),
  Pedimento=VALUES(Pedimento),
  Operacion=VALUES(Operacion),
  Clave_pedimento=VALUES(Clave_pedimento),
  a_despacho=VALUES(a_despacho),
  a_llegada=VALUES(a_llegada),
  C_Imp_Exp=VALUES(C_Imp_Exp),
  Facturar_a=VALUES(Facturar_a),
  Agente_Aduanal=VALUES(Agente_Aduanal),
  Ejecutivo=VALUES(Ejecutivo),
  medio_trasporte=VALUES(medio_trasporte),
  APERTURA=VALUES(APERTURA),
  LLEGADA_MERCAN=VALUES(LLEGADA_MERCAN),
  ENTREGA_CLASIFICA=VALUES(ENTREGA_CLASIFICA),
  INICIO_CLASIFICA=VALUES(INICIO_CLASIFICA),
  TERMINO_CLASIFICA=VALUES(TERMINO_CLASIFICA),
  INICIO_GLOSA=VALUES(INICIO_GLOSA),
  TERMINO_GLOSA=VALUES(TERMINO_GLOSA),
  ENTREGA_GLOSA=VALUES(ENTREGA_GLOSA),
  PAGO_PEDIMENTO=VALUES(PAGO_PEDIMENTO),
  DESPACHO_MERCAN=VALUES(DESPACHO_MERCAN),
  ENTREGA_FAC=VALUES(ENTREGA_FAC),
  FECHA_FAC=VALUES(FECHA_FAC),
  ENTREGA_FAC_CLI=VALUES(ENTREGA_FAC_CLI),
  ENTREGA_CAPTURA=VALUES(ENTREGA_CAPTURA),
  INICIO_CAPTURA=VALUES(INICIO_CAPTURA),
  TERMINO_CAPTURA=VALUES(TERMINO_CAPTURA),
  PRIMER_RECONOCIMIENTO=VALUES(PRIMER_RECONOCIMIENTO),
  Total_Adv=VALUES(Total_Adv),
  Total_DTA=VALUES(Total_DTA),
  Total_IVA=VALUES(Total_IVA),
  Total_Imp=VALUES(Total_Imp),
  Cancelada=VALUES(Cancelada);
`;

const UP_FACTURAS = `
INSERT INTO facturas (
  id_referencias, IDFactura, NumFac, Fecha_c, Incoterm, Moneda, Valor_ME, Valor_USD
) VALUES ?
ON DUPLICATE KEY UPDATE
  NumFac=VALUES(NumFac),
  Fecha_c=VALUES(Fecha_c),
  Incoterm=VALUES(Incoterm),
  Moneda=VALUES(Moneda),
  Valor_ME=VALUES(Valor_ME),
  Valor_USD=VALUES(Valor_USD);
`;

// ---------- Runner con transacción + warnings ----------
(async () => {
  let mssqlPool, my;
  try {
    console.log('Conectando...');
    mssqlPool = await sql.connect(mssqlConfig);
    my = await mysql.createConnection(mysqlConfig);
    await my.query("SET time_zone = '-06:00'"); // America/Mexico_City (UTC-6)

    // 1) Lee checkpoint y calcula ventana
    const lastDt = await getCheckpoint(my);
    const desde = new Date(lastDt.getTime() - ACOLCHADO_DIAS * 86400000);

    // 2) GENERAL
    const req1 = new sql.Request(mssqlPool);
    req1.input('fApertura', sql.DateTime, desde);
    const rs1 = await req1.query(Q_GENERAL);
    const rows1 = rs1.recordset;
    const selectedGeneral = rows1.length;

    const cleanGen = rows1.filter(r => r.id_referencias != null);
    const droppedGeneral = selectedGeneral - cleanGen.length;

    // 3) FACTURAS
    const req2 = new sql.Request(mssqlPool);
    req2.input('fApertura', sql.DateTime, desde);
    const rs2 = await req2.query(Q_FACTURAS);
    const rows2 = rs2.recordset;
    const selectedFacturas = rows2.length;

    const cleanFac = rows2.filter(r => r.id_referencias != null && r.IDFactura != null);
    const droppedFacturas = selectedFacturas - cleanFac.length;

    // 4) Mapear VALUES (prepared)
    const valsGeneral = cleanGen.map(r => ([
      r.NumeroDeReferencia,
      r.id_referencias,
      r.Pedimento,
      r.Operacion,
      r.Clave_pedimento,
      r.a_despacho,
      r.a_llegada,
      r.C_Imp_Exp,
      r.Facturar_a,
      r.Agente_Aduanal,
      r.Ejecutivo,
      r.medio_trasporte,
      r.APERTURA,
      r.LLEGADA_MERCAN,
      r.ENTREGA_CLASIFICA,
      r.INICIO_CLASIFICA,
      r.TERMINO_CLASIFICA,
      r.INICIO_GLOSA,
      r.TERMINO_GLOSA,
      r.ENTREGA_GLOSA,
      r.PAGO_PEDIMENTO,
      r.DESPACHO_MERCAN,
      r.ENTREGA_FAC,
      r.FECHA_FAC,
      r.ENTREGA_FAC_CLI,
      r.ENTREGA_CAPTURA,
      r.INICIO_CAPTURA,
      r.TERMINO_CAPTURA,
      r.PRIMER_RECONOCIMIENTO,
      safeMoneyValue(r.Total_Adv, 'Total_Adv'),
      safeMoneyValue(r.Total_DTA, 'Total_DTA'),
      safeMoneyValue(r.Total_IVA, 'Total_IVA'),
      safeMoneyValue(r.Total_Imp, 'Total_Imp'),
      r.Cancelada
    ]));
    const preparedGeneral = valsGeneral.length;

    const valsFacturas = cleanFac.map(r => ([
      r.id_referencias,
      r.IDFactura,
      r.NumFac,
      r.Fecha_c,
      (r.Incoterm ?? r.INCOTER ?? null),
      r.Moneda,
      safeMoneyValue(r.Valor_ME, 'Valor_ME'),
      safeMoneyValue(r.Valor_USD, 'Valor_USD')
    ]));
    const preparedFacturas = valsFacturas.length;

    // 5) TRANSACCIÓN: upserts + checkpoint
    await my.beginTransaction();

    const resGen = preparedGeneral ? await upsertChunks(my, UP_GENERAL, valsGeneral, 1000) : { totals: {records:0,duplicates:0,warnings:0,changedRows:0,affectedRows:0}, warningBatches: [] };
    const resFac = preparedFacturas ? await upsertChunks(my, UP_FACTURAS, valsFacturas, 1000) : { totals: {records:0,duplicates:0,warnings:0,changedRows:0,affectedRows:0}, warningBatches: [] };

    // checkpoint al máximo APERTURA recibido (de GENERAL)
    let maxApertura = null;
    for (const r of cleanGen) {
      if (r.APERTURA && (!maxApertura || r.APERTURA > maxApertura)) maxApertura = r.APERTURA;
    }
    if (maxApertura) await setCheckpoint(my, maxApertura);

    await my.commit();

    // 6) Logs de métricas
    const statsGen = resGen.totals;
    const statsFac = resFac.totals;
    const insertedGen = statsGen.records - statsGen.duplicates;
    const insertedFac = statsFac.records - statsFac.duplicates;

    console.log([
      '✅ OK',
      `general: selected=${selectedGeneral} dropped=${droppedGeneral} prepared=${preparedGeneral} ` +
      `upsert_total=${statsGen.records} inserted=${insertedGen} ` +
      `updated_attempted=${statsGen.duplicates} updated_changed=${statsGen.changedRows} warnings=${statsGen.warnings}`,
      `facturas: selected=${selectedFacturas} dropped=${droppedFacturas} prepared=${preparedFacturas} ` +
      `upsert_total=${statsFac.records} inserted=${insertedFac} ` +
      `updated_attempted=${statsFac.duplicates} updated_changed=${statsFac.changedRows} warnings=${statsFac.warnings}`,
      `watermark->${maxApertura ? maxApertura.toISOString() : 'N/A'}`,
      `desde:${desde.toISOString()}`
    ].join(' | '));

    // 7) Loguea WARNINGS por batch (si hubo)
    if (resGen.warningBatches.length) {
      console.log('⚠️  WARNINGS general:');
      for (const wb of resGen.warningBatches) {
        console.log(`  - batch ${wb.batchIndex}`);
        console.table(wb.warnings);
      }
    }
    if (resFac.warningBatches.length) {
      console.log('⚠️  WARNINGS facturas:');
      for (const wb of resFac.warningBatches) {
        console.log(`  - batch ${wb.batchIndex}`);
        console.table(wb.warnings);
      }
    }

    // 8) Cierres
    await my.end();
    await mssqlPool.close();
  } catch (err) {
    // si ya teníamos conexión MySQL abierta, intenta rollback
    try { if (my) await my.rollback(); } catch (e) { /* noop */ }
    console.error('❌ ETL ERROR:', err);
    try { if (my) await my.end(); } catch (e) { /* noop */ }
    try { if (mssqlPool) await mssqlPool.close(); } catch (e) { /* noop */ }
    process.exit(1);
  }
})();
