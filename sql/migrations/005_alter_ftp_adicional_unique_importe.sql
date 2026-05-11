-- Problema: dos conceptos del mismo tipo (ej: 2x DEMORA) en el mismo archivo
-- se pisaban porque el UNIQUE KEY era (referencia, archivo_xml, concepto).
-- Solución: agregar importe al UNIQUE KEY para distinguir cada concepto individual.

ALTER TABLE ftp_adicional DROP INDEX uk_ref_archivo_concepto;

ALTER TABLE ftp_adicional
  ADD UNIQUE KEY uk_ref_archivo_concepto_importe (referencia, archivo_xml, concepto, importe);
