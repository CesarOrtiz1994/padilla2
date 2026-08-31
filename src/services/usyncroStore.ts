import type { Connection, RowDataPacket } from 'mysql2/promise';
import type { UsyncroRegistro, SyncEstado, ActoresMap, PlacesMap } from '../types/usyncro';

export async function getRegistro(conn: Connection, ref: string): Promise<UsyncroRegistro | null> {
  const [rows] = await conn.query<RowDataPacket[]>(
    'SELECT * FROM usyncro_registros WHERE numero_referencia = ? LIMIT 1', [ref]
  );
  return (rows[0] as UsyncroRegistro) ?? null;
}

export async function insertRegistro(
  conn: Connection,
  ref: string,
  idCliente: number,
  recordId: string,
  actores: ActoresMap,
  places: PlacesMap
): Promise<void> {
  await conn.query(
    `INSERT INTO usyncro_registros
      (numero_referencia, id_cliente, record_id,
       actor_creator_id, actor_buyer_id, actor_supplier_id,
       actor_customs_broker_id, actor_taxes_id,
       place_destination_id)
     VALUES (?,?,?,?,?,?,?,?,?)
     ON DUPLICATE KEY UPDATE
       record_id=VALUES(record_id),
       actor_creator_id=VALUES(actor_creator_id),
       actor_buyer_id=VALUES(actor_buyer_id),
       actor_supplier_id=VALUES(actor_supplier_id),
       actor_customs_broker_id=VALUES(actor_customs_broker_id),
       actor_taxes_id=VALUES(actor_taxes_id),
       place_destination_id=VALUES(place_destination_id),
       actualizado_en=NOW()`,
    [
      ref, idCliente, recordId,
      actores['creator'] ?? null,
      actores['buyer'] ?? null,
      actores['supplier'] ?? null,
      actores['customsBroker'] ?? null,
      actores['taxes'] ?? null,
      places['destination'] ?? null,
    ]
  );
}

export async function getCamposSincronizadosSet(conn: Connection, ref: string, campos: string[]): Promise<Set<string>> {
  if (campos.length === 0) return new Set();
  const placeholders = campos.map(() => '?').join(',');
  const [rows] = await conn.query<RowDataPacket[]>(
    `SELECT campo FROM usyncro_sync_estado WHERE numero_referencia = ? AND campo IN (${placeholders}) AND sincronizado = 1`,
    [ref, ...campos]
  );
  return new Set((rows as Array<{ campo: string }>).map(r => r.campo));
}

export async function isCampoSincronizado(conn: Connection, ref: string, campo: string): Promise<boolean> {
  const [rows] = await conn.query<RowDataPacket[]>(
    'SELECT sincronizado FROM usyncro_sync_estado WHERE numero_referencia = ? AND campo = ? LIMIT 1',
    [ref, campo]
  );
  return rows.length > 0 && rows[0]['sincronizado'] === 1;
}

export async function getCamposPendientes(conn: Connection, ref: string): Promise<SyncEstado[]> {
  const [rows] = await conn.query<RowDataPacket[]>(
    'SELECT * FROM usyncro_sync_estado WHERE numero_referencia = ? AND sincronizado = 0',
    [ref]
  );
  return rows as SyncEstado[];
}

export async function marcarCampoOk(conn: Connection, ref: string, campo: string): Promise<void> {
  await conn.query(
    `INSERT INTO usyncro_sync_estado (numero_referencia, campo, sincronizado, sincronizado_en)
     VALUES (?, ?, 1, NOW())
     ON DUPLICATE KEY UPDATE sincronizado=1, sincronizado_en=NOW(), ultimo_error=NULL`,
    [ref, campo]
  );
}

export async function marcarCampoError(conn: Connection, ref: string, campo: string, error: string): Promise<void> {
  await conn.query(
    `INSERT INTO usyncro_sync_estado (numero_referencia, campo, sincronizado, intentos, ultimo_error)
     VALUES (?, ?, 0, 1, ?)
     ON DUPLICATE KEY UPDATE intentos=intentos+1, ultimo_error=?, actualizado_en=NOW()`,
    [ref, campo, error, error]
  );
}

export async function marcarCompletado(conn: Connection, ref: string): Promise<void> {
  await conn.query(
    'UPDATE usyncro_registros SET fecha_completado = NOW() WHERE numero_referencia = ? AND fecha_completado IS NULL',
    [ref]
  );
}

// Borra sync_estado de referencias completadas hace más de retentionDias días
export async function purgarSync(conn: Connection, retentionDias: number): Promise<number> {
  const [result] = await conn.query(
    `DELETE s FROM usyncro_sync_estado s
     INNER JOIN usyncro_registros r ON r.numero_referencia = s.numero_referencia
     WHERE r.fecha_completado IS NOT NULL
       AND r.fecha_completado < DATE_SUB(NOW(), INTERVAL ? DAY)`,
    [retentionDias]
  );
  return (result as import('mysql2').ResultSetHeader).affectedRows;
}
