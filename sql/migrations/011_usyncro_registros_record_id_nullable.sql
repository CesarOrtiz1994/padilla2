-- Permite guardar un placeholder (sin record_id) para referencias parciales
-- que aún no cumplen la regla de taxes completo. Se completa cuando llega el pedimento.
ALTER TABLE usyncro_registros
  MODIFY COLUMN record_id VARCHAR(30) NULL;
