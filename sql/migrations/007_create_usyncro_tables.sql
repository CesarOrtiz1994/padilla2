-- Registros creados en Usyncro, uno por NumeroDeReferencia
CREATE TABLE IF NOT EXISTS usyncro_registros (
  numero_referencia  VARCHAR(50)  NOT NULL PRIMARY KEY,
  id_cliente         INT          NOT NULL,
  record_id          VARCHAR(30)  NOT NULL UNIQUE,
  -- actores
  actor_creator_id       VARCHAR(30),
  actor_manager_id       VARCHAR(30),
  actor_buyer_id         VARCHAR(30),
  actor_supplier_id      VARCHAR(30),
  actor_customs_broker_id VARCHAR(30),
  actor_transporter_id   VARCHAR(30),
  actor_notify_id        VARCHAR(30),
  actor_taxes_id         VARCHAR(30),
  -- places
  place_origin_id      VARCHAR(30),
  place_destination_id VARCHAR(30),
  place_delivery_id    VARCHAR(30),
  place_pickup_id      VARCHAR(30),
  creado_en      DATETIME DEFAULT CURRENT_TIMESTAMP,
  actualizado_en DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Estado de sincronización por campo — permite reintentos granulares
CREATE TABLE IF NOT EXISTS usyncro_sync_estado (
  numero_referencia VARCHAR(50)  NOT NULL,
  campo             VARCHAR(100) NOT NULL,   -- ej: 'buyer.name', 'origin.address'
  sincronizado      TINYINT(1)   NOT NULL DEFAULT 0,
  intentos          SMALLINT     NOT NULL DEFAULT 0,
  ultimo_error      TEXT,
  sincronizado_en   DATETIME,
  actualizado_en    DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (numero_referencia, campo),
  KEY idx_pendientes (sincronizado, intentos)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
