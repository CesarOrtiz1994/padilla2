// src/queries/gastosComprobados.js - Queries para gastos comprobados

const Q_GASTOS_COMPROBADOS = `
SELECT 
    d.nombreSistema, 
    d.nombreOriginal, 
    g.id_referencia, 
    g.concepto, 
    g.Adicional,
    r.facturada,
    r.FechaDeModificacion,
    r.NumeroDeReferencia
FROM gastoscomprobados g
INNER JOIN Documentos d ON g.id_gastoComprobado = d.id_propio
INNER JOIN referencias r ON g.id_referencia = r.id_referencias
WHERE r.FechaDeModificacion >= DATEADD(WEEK, -1, GETDATE())
  AND r.facturada = 1
  AND d.id_tipoDocumento = 8888
  AND g.concepto IN ('MANIOBRAS', 'MANIOBRAS Y ALMACENAJES', 'ALMACENAJES', 'DEMORAS')
  AND d.nombreOriginal LIKE '%.xml'
`;

const UPSERT_FTP_ADICIONAL = `
INSERT INTO ftp_adicional (
  referencia, archivo_xml, importe, concepto, descripcion, observaciones
) VALUES ? AS new
ON DUPLICATE KEY UPDATE
  descripcion = new.descripcion,
  observaciones = new.observaciones;
`;

module.exports = { Q_GASTOS_COMPROBADOS, UPSERT_FTP_ADICIONAL };
