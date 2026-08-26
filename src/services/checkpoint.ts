import type { Connection } from 'mysql2/promise';

async function getCheckpoint(conn: Connection): Promise<Date> {
  const [rows] = await conn.query<import('mysql2').RowDataPacket[]>(
    "SELECT last_dt FROM sync_checkpoint WHERE name='apertura_activos' LIMIT 1"
  );
  if (!rows.length || !rows[0]['last_dt']) {
    return new Date('2024-01-01T00:00:00Z');
  }
  return rows[0]['last_dt'] as Date;
}

async function setCheckpoint(conn: Connection, dt: Date): Promise<void> {
  await conn.query(
    "UPDATE sync_checkpoint SET last_dt=? WHERE name='apertura_activos'",
    [dt]
  );
}

export { getCheckpoint, setCheckpoint };
