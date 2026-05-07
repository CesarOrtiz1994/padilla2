-- Crear tabla para almacenar gastos de Almacenaje y Demora extraídos de FTP
CREATE TABLE IF NOT EXISTS ftp_adicional (
  id INT AUTO_INCREMENT PRIMARY KEY,
  referencia VARCHAR(50) NOT NULL COMMENT 'NumeroDeReferencia del sistema origen',
  archivo_xml VARCHAR(255) NOT NULL COMMENT 'Nombre del archivo XML de origen',
  importe DECIMAL(15,2) NOT NULL COMMENT 'Importe del concepto en el XML',
  concepto ENUM('ALMACENAJE','DEMORA') NOT NULL COMMENT 'Tipo de gasto identificado',
  descripcion TEXT COMMENT 'Descripción del concepto tal como aparece en el XML',
  observaciones TEXT COMMENT 'Observaciones adicionales del sistema origen',
  fecha_procesamiento DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  
  -- Índice único: un registro por referencia+archivo+concepto
  UNIQUE KEY uk_ref_archivo_concepto (referencia, archivo_xml, concepto),
  
  -- Índice para búsquedas por referencia
  KEY idx_referencia (referencia),
  
  -- Índice para búsquedas por concepto
  KEY idx_concepto (concepto)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
COMMENT='Gastos comprobados de Almacenaje y Demora extraídos de archivos XML vía FTP';

-- Si la tabla ya existe, aplicar los cambios de estructura:
-- ALTER TABLE ftp_adicional ADD COLUMN IF NOT EXISTS archivo_xml VARCHAR(255) NOT NULL DEFAULT '' COMMENT 'Nombre del archivo XML de origen' AFTER referencia;
-- ALTER TABLE ftp_adicional ADD COLUMN IF NOT EXISTS descripcion TEXT COMMENT 'Descripción del concepto tal como aparece en el XML' AFTER concepto;
-- ALTER TABLE ftp_adicional DROP INDEX IF EXISTS uk_ref_concepto;
-- ALTER TABLE ftp_adicional ADD UNIQUE KEY uk_ref_archivo_concepto (referencia, archivo_xml, concepto);
