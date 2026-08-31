ALTER TABLE usyncro_registros
  ADD COLUMN fecha_completado DATETIME NULL,
  ADD KEY idx_fecha_completado (fecha_completado);
