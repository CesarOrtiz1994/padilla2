// src/queries/facturas.js - Queries para tabla facturas

const Q_FACTURAS = `
SELECT
  r.id_referencias,
  pf.IDFactura,
  pf.NumeroDeFactura AS NumFac,
  pf.Fecha           AS Fecha_c,
  pf.IDIncoter       AS Incoterm,
  pf.Moneda          AS Moneda,
  pf.ImporteFacturaME AS Valor_ME,
  pf.ImporteFacturaUS AS Valor_USD
FROM referencias r
INNER JOIN PedimentosFacturas pf ON r.id_referencias = pf.IDReferencia
WHERE r.FechaApertura > @fApertura
`;

const UP_FACTURAS = `
INSERT INTO facturas (
  id_referencias, IDFactura, NumFac, Fecha_c, Incoterm, Moneda, Valor_ME, Valor_USD
) VALUES ? AS new
ON DUPLICATE KEY UPDATE
  NumFac=new.NumFac,
  Fecha_c=new.Fecha_c,
  Incoterm=new.Incoterm,
  Moneda=new.Moneda,
  Valor_ME=new.Valor_ME,
  Valor_USD=new.Valor_USD;
`;

module.exports = { Q_FACTURAS, UP_FACTURAS };
