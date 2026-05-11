// src/services/mysqlHelpers.js - Helpers para operaciones MySQL

/**
 * Parsear OkPacket para obtener Records / Duplicates / Warnings
 */
function parseOkPacket(ok, fallbackRecords = 0) {
  const info = (ok?.info || ok?.message || '').replace(/,/g, '');
  let records = fallbackRecords, duplicates = 0, warnings = Number(ok?.warningStatus || 0);

  const m = /Records:\s*(\d+)\s*Duplicates:\s*(\d+)\s*Warnings:\s*(\d+)/i.exec(info);
  if (m) {
    records = Number(m[1] || 0);
    duplicates = Number(m[2] || 0);
    warnings = Number(m[3] || 0);
  }

  const changedRows = Number(ok?.changedRows || 0);
  const affectedRows = Number(ok?.affectedRows || 0);
  return { records, duplicates, warnings, changedRows, affectedRows, rawInfo: info };
}

/**
 * Obtiene y analiza los warnings específicos de MySQL
 */
async function getWarningsDetails(conn) {
  try {
    const [warnings] = await conn.query('SHOW WARNINGS');
    const summary = {};
    const details = [];

    for (const w of warnings) {
      const key = `${w.Code}: ${w.Message}`;
      summary[key] = (summary[key] || 0) + 1;
      details.push({ code: w.Code, level: w.Level, message: w.Message });
    }

    return { summary, details, count: warnings.length };
  } catch (e) {
    return { summary: {}, details: [], count: 0, error: e.message };
  }
}

/**
 * Analiza warnings comunes y sugiere correcciones
 */
function analyzeWarnings(summary) {
  const issues = [];

  for (const [key, count] of Object.entries(summary)) {
    if (key.includes('1265') || key.includes('Data truncated')) {
      issues.push({
        type: 'DATA_TRUNCATED',
        count,
        message: 'Datos truncados: valor más largo que la columna',
        solution: 'Aumentar tamaño de columna o truncar datos antes de insertar'
      });
    } else if (key.includes('1264') || key.includes('Out of range')) {
      issues.push({
        type: 'OUT_OF_RANGE',
        count,
        message: 'Valor fuera de rango numérico',
        solution: 'Revisar safeMoneyValue o cambiar tipo de columna a DECIMAL(19,4)'
      });
    } else if (key.includes('1292') || key.includes('Incorrect datetime')) {
      issues.push({
        type: 'INVALID_DATE',
        count,
        message: 'Fecha inválida',
        solution: 'Revisar función sumar6Horas, fechas 1899 deberían ser null'
      });
    } else if (key.includes('1048') || key.includes('cannot be null')) {
      issues.push({
        type: 'NULL_VIOLATION',
        count,
        message: 'Columna NOT NULL recibió NULL',
        solution: 'Revisar mapeo de valores o cambiar columna a NULLable'
      });
    }
  }

  return issues;
}

/**
 * UPSERT por lotes + captura warnings por batch
 */
async function upsertChunks(conn, query, data, size = 1000, opts = {}) {
  const totals = { records: 0, duplicates: 0, warnings: 0, changedRows: 0, affectedRows: 0 };
  const allWarningsSummary = {};
  const warningFilas = []; // acumula detalles de filas con warning para el resumen final

  const label = opts.label || 'upsert';
  const idIndex = Number.isInteger(opts.idIndex) ? opts.idIndex : null;

  for (let i = 0; i < data.length; i += size) {
    const part = data.slice(i, i + size);

    let rangeStr = '';
    if (idIndex != null) {
      let minId = null;
      let maxId = null;
      for (const row of part) {
        const v = row?.[idIndex];
        const n = v == null ? null : Number(v);
        if (n == null || Number.isNaN(n)) continue;
        if (minId == null || n < minId) minId = n;
        if (maxId == null || n > maxId) maxId = n;
      }
      if (minId != null || maxId != null) rangeStr = ` ids=[${minId ?? 'N/A'}..${maxId ?? 'N/A'}]`;
    }

    const batchIndex = Math.floor(i / size);
    const t0 = Date.now();
    console.log(`BATCH ${label} #${batchIndex} size=${part.length}${rangeStr}`);

    const [ok] = await conn.query(query, [part]);
    const s = parseOkPacket(ok, part.length);

    // Capturar warnings específicos si hay
    if (s.warnings > 0) {
      const warnDetails = await getWarningsDetails(conn);
      for (const [key, count] of Object.entries(warnDetails.summary)) {
        allWarningsSummary[key] = (allWarningsSummary[key] || 0) + count;
      }
      // Acumular detalles de fila para mostrar en resumen final
      for (const w of warnDetails.details) {
        const rowMatch = /at row (\d+)/i.exec(w.message);
        const rowNum = rowMatch ? parseInt(rowMatch[1], 10) : null;
        const fila = rowNum != null ? part[rowNum - 1] : null;
        warningFilas.push({
          batch: batchIndex,
          rowNum,
          mensaje: w.message,
          ref: fila ? (fila[0] ?? 'N/A') : 'N/A',
          archivo: fila ? (fila[1] ?? 'N/A') : 'N/A',
          importe: fila ? (fila[2] ?? 'N/A') : 'N/A'
        });
      }
    }

    const t1 = Date.now();
    console.log(
      `BATCH ${label} #${batchIndex} done in ${Math.round((t1 - t0) / 1000)}s ` +
      `(records=${s.records} dup=${s.duplicates} changed=${s.changedRows} warnings=${s.warnings})`
    );

    totals.records += s.records;
    totals.duplicates += s.duplicates;
    totals.warnings += s.warnings;
    totals.changedRows += s.changedRows;
    totals.affectedRows += s.affectedRows;
  }

  // Análisis final de warnings
  if (Object.keys(allWarningsSummary).length > 0) {
    console.log(`\nRESUMEN DE WARNINGS (${label}):`);
    const issues = analyzeWarnings(allWarningsSummary);
    for (const issue of issues) {
      console.log(`  [${issue.type}] ${issue.count} ocurrencias: ${issue.message}`);
      console.log(`  -> Solucion: ${issue.solution}`);
    }
    if (warningFilas.length > 0) {
      console.log(`\n  Filas con warnings:`);
      for (const wf of warningFilas) {
        console.log(`  [batch #${wf.batch} row ${wf.rowNum}] ref=${wf.ref} archivo=${wf.archivo} importe=${wf.importe}`);
        console.log(`    -> ${wf.mensaje}`);
      }
    }
  }

  return { totals, warningsSummary: allWarningsSummary };
}

module.exports = { parseOkPacket, upsertChunks, getWarningsDetails, analyzeWarnings };
