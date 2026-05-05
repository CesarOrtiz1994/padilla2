// src/queries/general.js - Queries para tabla general

const Q_GENERAL = `
SELECT
  r.NumeroDeReferencia,
  r.id_referencias,
  p.Pedimento,
  r.Operacion,
  re.regimen            AS Clave_pedimento,
  a_origen.descripcion  AS a_despacho,
  a_llegada.descripcion AS a_llegada,
  c_i.nombre            AS C_Imp_Exp,
  r.facturada           AS facturada,
  c_f.nombre            AS Facturar_a,
  aa.nombre             AS Agente_Aduanal,
  u.nombre              AS Ejecutivo,
  mt.descripcion        AS medio_trasporte,
  r.FechaApertura       AS APERTURA,
  MAX(CASE 
      WHEN r.Operacion = 1 AND b.IdEvento = 6 THEN b.FechaHoraCapturada 
      WHEN r.Operacion = 2 AND be.IdEvento = 6 THEN be.FechaHoraCapturada 
  END) AS LLEGADA_MERCAN,
  MAX(CASE 
      WHEN r.Operacion = 1 AND b.IdEvento = 18 THEN b.FechaHoraCapturada 
      WHEN r.Operacion = 2 AND be.IdEvento = 18 THEN be.FechaHoraCapturada 
  END) AS ENTREGA_CLASIFICA,
  MAX(CASE 
      WHEN r.Operacion = 1 AND b.IdEvento = 19 THEN b.FechaHoraCapturada 
      WHEN r.Operacion = 2 AND be.IdEvento = 19 THEN be.FechaHoraCapturada 
  END) AS INICIO_CLASIFICA,
  MAX(CASE 
      WHEN r.Operacion = 1 AND b.IdEvento = 20 THEN b.FechaHoraCapturada 
      WHEN r.Operacion = 2 AND be.IdEvento = 20 THEN be.FechaHoraCapturada 
  END) AS TERMINO_CLASIFICA,
  MAX(CASE 
      WHEN r.Operacion = 1 AND b.IdEvento = 69 THEN b.FechaHoraCapturada 
      WHEN r.Operacion = 2 AND be.IdEvento = 69 THEN be.FechaHoraCapturada 
  END) AS INICIO_GLOSA,
  MAX(CASE 
      WHEN r.Operacion = 1 AND b.IdEvento = 70 THEN b.FechaHoraCapturada 
      WHEN r.Operacion = 2 AND be.IdEvento = 70 THEN be.FechaHoraCapturada 
  END) AS TERMINO_GLOSA,
  MAX(CASE 
      WHEN r.Operacion = 1 AND b.IdEvento = 22 THEN b.FechaHoraCapturada 
      WHEN r.Operacion = 2 AND be.IdEvento = 22 THEN be.FechaHoraCapturada 
  END) AS ENTREGA_GLOSA,
  MAX(CASE 
      WHEN r.Operacion = 1 AND b.IdEvento = 29 THEN b.FechaHoraCapturada 
      WHEN r.Operacion = 2 AND be.IdEvento = 29 THEN be.FechaHoraCapturada 
  END) AS PAGO_PEDIMENTO,
  MAX(CASE 
      WHEN r.Operacion = 1 AND b.IdEvento = 32 THEN b.FechaHoraCapturada 
      WHEN r.Operacion = 2 AND be.IdEvento = 32 THEN be.FechaHoraCapturada 
  END) AS DESPACHO_MERCAN,
  MAX(CASE 
      WHEN r.Operacion = 1 AND b.IdEvento = 47 THEN b.FechaHoraCapturada 
      WHEN r.Operacion = 2 AND be.IdEvento = 47 THEN be.FechaHoraCapturada 
  END) AS ENTREGA_FAC,
  MAX(CASE 
      WHEN r.Operacion = 1 AND b.IdEvento = 48 THEN b.FechaHoraCapturada 
      WHEN r.Operacion = 2 AND be.IdEvento = 48 THEN be.FechaHoraCapturada 
  END) AS FECHA_FAC,
  MAX(CASE 
      WHEN r.Operacion = 1 AND b.IdEvento = 49 THEN b.FechaHoraCapturada 
      WHEN r.Operacion = 2 AND be.IdEvento = 49 THEN be.FechaHoraCapturada 
  END) AS ENTREGA_FAC_CLI,
  MAX(CASE 
      WHEN r.Operacion = 1 AND b.IdEvento = 26 THEN b.FechaHoraCapturada 
      WHEN r.Operacion = 2 AND be.IdEvento = 26 THEN be.FechaHoraCapturada 
  END) AS ENTREGA_CAPTURA,
  MAX(CASE 
      WHEN r.Operacion = 1 AND b.IdEvento = 33 THEN b.FechaHoraCapturada 
      WHEN r.Operacion = 2 AND be.IdEvento = 33 THEN be.FechaHoraCapturada 
  END) AS INICIO_CAPTURA,
  MAX(CASE 
      WHEN r.Operacion = 1 AND b.IdEvento = 42 THEN b.FechaHoraCapturada 
      WHEN r.Operacion = 2 AND be.IdEvento = 42 THEN be.FechaHoraCapturada 
  END) AS TERMINO_CAPTURA,
  MAX(CASE 
      WHEN r.Operacion = 1 AND b.IdEvento = 36 THEN b.FechaHoraCapturada 
      WHEN r.Operacion = 2 AND be.IdEvento = 36 THEN be.FechaHoraCapturada 
  END) AS PRIMER_RECONOCIMIENTO,
  MAX(p.ADV1)           AS Total_Adv,
  MAX(p.DTA1)           AS Total_DTA,
  MAX(p.IVA1)           AS Total_IVA,
  MAX(p.TOTALIMPUESTOS) AS Total_Imp,
  r.Cancelada           AS Cancelada
FROM referencias r
INNER JOIN PedimentosEncabezado p ON p.id_referencia = r.id_referencias
LEFT JOIN regimen re ON re.id_regimen = r.id_regimen
LEFT JOIN aduana a_origen ON a_origen.id_Aduana = r.id_aduana
LEFT JOIN aduana a_llegada ON a_llegada.id_Aduana = r.Id_AduanaLlegada
LEFT JOIN clientes c_i ON c_i.id_cliente = r.id_cliente
LEFT JOIN clientes c_f ON c_f.id_cliente = r.concargo
LEFT JOIN agentesaduanales aa ON aa.id_agenteaduanal = r.id_agenteaduanal
LEFT JOIN usuarios u ON u.id_usuario = r.IdEjecutivo
LEFT JOIN MediosDeTransporte mt ON mt.IDMedioDeTransporte = p.IDTransporteEnt_Sal
LEFT JOIN BitacoraEventosImportacion b ON b.Referencia = r.id_referencias
LEFT JOIN BitacoraEventosExportacion be ON be.Referencia = r.id_referencias
WHERE r.FechaApertura > @fApertura
GROUP BY
  r.NumeroDeReferencia, r.id_referencias, p.Pedimento, r.Operacion, re.regimen,
  a_origen.descripcion, a_llegada.descripcion, c_i.nombre, r.facturada, c_f.nombre,
  aa.nombre, u.nombre, mt.descripcion, r.FechaApertura, r.Cancelada
`;

const UP_GENERAL = `
INSERT INTO general (
  NumeroDeReferencia, id_referencias, Pedimento, Operacion, Clave_pedimento, a_despacho, a_llegada,
  C_Imp_Exp, facturada, Facturar_a, Agente_Aduanal, Ejecutivo, medio_trasporte, APERTURA,
  LLEGADA_MERCAN, ENTREGA_CLASIFICA, INICIO_CLASIFICA, TERMINO_CLASIFICA,
  INICIO_GLOSA, TERMINO_GLOSA, ENTREGA_GLOSA, PAGO_PEDIMENTO, DESPACHO_MERCAN,
  ENTREGA_FAC, FECHA_FAC, ENTREGA_FAC_CLI, ENTREGA_CAPTURA, INICIO_CAPTURA, 
  TERMINO_CAPTURA, PRIMER_RECONOCIMIENTO, Total_Adv, Total_DTA, Total_IVA, Total_Imp, Cancelada
) VALUES ? AS new
ON DUPLICATE KEY UPDATE
  NumeroDeReferencia=new.NumeroDeReferencia,
  Pedimento=new.Pedimento,
  Operacion=new.Operacion,
  Clave_pedimento=new.Clave_pedimento,
  a_despacho=new.a_despacho,
  a_llegada=new.a_llegada,
  C_Imp_Exp=new.C_Imp_Exp,
  facturada=new.facturada,
  Facturar_a=new.Facturar_a,
  Agente_Aduanal=new.Agente_Aduanal,
  Ejecutivo=new.Ejecutivo,
  medio_trasporte=new.medio_trasporte,
  APERTURA=new.APERTURA,
  LLEGADA_MERCAN=new.LLEGADA_MERCAN,
  ENTREGA_CLASIFICA=new.ENTREGA_CLASIFICA,
  INICIO_CLASIFICA=new.INICIO_CLASIFICA,
  TERMINO_CLASIFICA=new.TERMINO_CLASIFICA,
  INICIO_GLOSA=new.INICIO_GLOSA,
  TERMINO_GLOSA=new.TERMINO_GLOSA,
  ENTREGA_GLOSA=new.ENTREGA_GLOSA,
  PAGO_PEDIMENTO=new.PAGO_PEDIMENTO,
  DESPACHO_MERCAN=new.DESPACHO_MERCAN,
  ENTREGA_FAC=new.ENTREGA_FAC,
  FECHA_FAC=new.FECHA_FAC,
  ENTREGA_FAC_CLI=new.ENTREGA_FAC_CLI,
  ENTREGA_CAPTURA=new.ENTREGA_CAPTURA,
  INICIO_CAPTURA=new.INICIO_CAPTURA,
  TERMINO_CAPTURA=new.TERMINO_CAPTURA,
  PRIMER_RECONOCIMIENTO=new.PRIMER_RECONOCIMIENTO,
  Total_Adv=new.Total_Adv,
  Total_DTA=new.Total_DTA,
  Total_IVA=new.Total_IVA,
  Total_Imp=new.Total_Imp,
  Cancelada=new.Cancelada;
`;

module.exports = { Q_GENERAL, UP_GENERAL };
