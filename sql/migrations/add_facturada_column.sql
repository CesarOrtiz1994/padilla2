-- Script para agregar la columna facturada a la tabla general
ALTER TABLE general
  ADD COLUMN facturada TINYINT(1) DEFAULT 0
  AFTER C_Imp_Exp;
