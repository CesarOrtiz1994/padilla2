import type { Connection, RowDataPacket } from 'mysql2/promise';
import type { UsyncroActor, UsyncroPlace, UsyncroRegistro } from '../types';

// ── Registro principal ────────────────────────────────────────────────────────

export async function getRegistro(
  conn: Connection,
  numeroReferencia: string
): Promise<UsyncroRegistro | null> {
  const [rows] = await conn.query<RowDataPacket[]>(
    'SELECT * FROM usyncro_registros WHERE numero_referencia = ? LIMIT 1',
    [numeroReferencia]
  );
  return (rows[0] as UsyncroRegistro) ?? null;
}

export async function saveRegistro(
  conn: Connection,
  reg: UsyncroRegistro
): Promise<void> {
  await conn.query(
    `INSERT INTO usyncro_registros (
      numero_referencia, id_cliente, record_id,
      actor_creator_id, actor_manager_id, actor_buyer_id, actor_supplier_id,
      actor_customs_broker_id, actor_transporter_id, actor_notify_id, actor_taxes_id,
      place_origin_id, place_destination_id, place_delivery_id, place_pickup_id
    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    ON DUPLICATE KEY UPDATE
      record_id=VALUES(record_id),
      actor_creator_id=VALUES(actor_creator_id), actor_manager_id=VALUES(actor_manager_id),
      actor_buyer_id=VALUES(actor_buyer_id), actor_supplier_id=VALUES(actor_supplier_id),
      actor_customs_broker_id=VALUES(actor_customs_broker_id),
      actor_transporter_id=VALUES(actor_transporter_id), actor_notify_id=VALUES(actor_notify_id),
      actor_taxes_id=VALUES(actor_taxes_id),
      place_origin_id=VALUES(place_origin_id), place_destination_id=VALUES(place_destination_id),
      place_delivery_id=VALUES(place_delivery_id), place_pickup_id=VALUES(place_pickup_id)`,
    [
      reg.numero_referencia, reg.id_cliente, reg.record_id,
      reg.actor_creator_id, reg.actor_manager_id, reg.actor_buyer_id, reg.actor_supplier_id,
      reg.actor_customs_broker_id, reg.actor_transporter_id, reg.actor_notify_id, reg.actor_taxes_id,
      reg.place_origin_id, reg.place_destination_id, reg.place_delivery_id, reg.place_pickup_id,
    ]
  );
}

// ── Estado de sincronización por campo (reintentos) ───────────────────────────

export async function marcarSincronizado(
  conn: Connection,
  numeroReferencia: string,
  campo: string
): Promise<void> {
  await conn.query(
    `INSERT INTO usyncro_sync_estado (numero_referencia, campo, sincronizado, sincronizado_en)
     VALUES (?, ?, 1, NOW())
     ON DUPLICATE KEY UPDATE sincronizado=1, sincronizado_en=NOW(), ultimo_error=NULL`,
    [numeroReferencia, campo]
  );
}

export async function marcarError(
  conn: Connection,
  numeroReferencia: string,
  campo: string,
  error: string
): Promise<void> {
  await conn.query(
    `INSERT INTO usyncro_sync_estado (numero_referencia, campo, sincronizado, intentos, ultimo_error)
     VALUES (?, ?, 0, 1, ?)
     ON DUPLICATE KEY UPDATE sincronizado=0, intentos=intentos+1, ultimo_error=?`,
    [numeroReferencia, campo, error, error]
  );
}

export async function getPendientes(
  conn: Connection,
  numeroReferencia: string
): Promise<string[]> {
  const [rows] = await conn.query<RowDataPacket[]>(
    `SELECT campo FROM usyncro_sync_estado
     WHERE numero_referencia = ? AND sincronizado = 0`,
    [numeroReferencia]
  );
  return rows.map(r => r['campo'] as string);
}

// ── Helpers para extraer IDs de actores y places ──────────────────────────────

export function extraerActores(actores: UsyncroActor[]): Partial<UsyncroRegistro> {
  const result: Partial<UsyncroRegistro> = {};
  for (const a of actores) {
    switch (a.attributes.subtype) {
      case 'creator':      result.actor_creator_id = a.id; break;
      case 'manager':      result.actor_manager_id = a.id; break;
      case 'buyer':        result.actor_buyer_id = a.id; break;
      case 'supplier':     result.actor_supplier_id = a.id; break;
      case 'customsBroker': result.actor_customs_broker_id = a.id; break;
      case 'transporter':  if (!result.actor_transporter_id) result.actor_transporter_id = a.id; break;
      case 'notify':       if (!result.actor_notify_id) result.actor_notify_id = a.id; break;
      case 'taxes':        result.actor_taxes_id = a.id; break;
    }
  }
  return result;
}

export function extraerPlaces(places: UsyncroPlace[]): Partial<UsyncroRegistro> {
  const result: Partial<UsyncroRegistro> = {};
  for (const p of places) {
    switch (p.attributes.subtype) {
      case 'origin':      result.place_origin_id = p.id; break;
      case 'destination': result.place_destination_id = p.id; break;
      case 'delivery':    result.place_delivery_id = p.id; break;
      case 'pickup':      result.place_pickup_id = p.id; break;
    }
  }
  return result;
}
