// src/services/checkpoint.js - Gestión del checkpoint de sincronización

/**
 * Lee el checkpoint de sincronización desde MySQL
 */
async function getCheckpoint(conn) {
  const [rows] = await conn.query(
    "SELECT last_dt FROM sync_checkpoint WHERE name='apertura_activos' LIMIT 1"
  );
  if (!rows.length || !rows[0].last_dt) {
    return new Date('2024-01-01T00:00:00Z');
  }
  return rows[0].last_dt;
}

/**
 * Actualiza el checkpoint de sincronización en MySQL
 */
async function setCheckpoint(conn, dt) {
  await conn.query(
    "UPDATE sync_checkpoint SET last_dt=? WHERE name='apertura_activos'",
    [dt]
  );
}

module.exports = { getCheckpoint, setCheckpoint };
