-- Referencias parciales (-0A, -0B...) que nunca llegaron a completar taxes
-- tras el período de gracia: se marcan para NUNCA crearse en Usyncro.
CREATE TABLE IF NOT EXISTS usyncro_creacion_omitida (
  numero_referencia VARCHAR(50) NOT NULL PRIMARY KEY,
  motivo            TEXT,
  creado_en         DATETIME DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
