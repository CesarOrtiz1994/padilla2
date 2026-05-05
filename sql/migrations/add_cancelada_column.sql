-- Script para agregar la columna Cancelada a la tabla general
ALTER TABLE general ADD COLUMN Cancelada TINYINT(1) DEFAULT 0 COMMENT 'Indica si la referencia está cancelada (1) o activa (0)';

-- Índice para mejorar el rendimiento de consultas que filtran por Cancelada
CREATE INDEX idx_general_cancelada ON general(Cancelada);
