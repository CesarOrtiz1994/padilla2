-- Eliminar columnas no utilizadas de usyncro_registros
ALTER TABLE usyncro_registros
  DROP COLUMN actor_manager_id,
  DROP COLUMN actor_transporter_id,
  DROP COLUMN actor_notify_id,
  DROP COLUMN place_origin_id,
  DROP COLUMN place_delivery_id,
  DROP COLUMN place_pickup_id;
