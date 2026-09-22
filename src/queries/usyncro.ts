import { CLIENTES_MAP } from '../config/integracion';

const idList = [...CLIENTES_MAP.keys()].join(',');

const SELECT_REFERENCIA = `
SELECT
  r.NumeroDeReferencia,
  r.id_referencias,
  r.id_cliente,
  r.Operacion,
  r.FechaApertura,
  r.Cancelada,
  r.Primer_Reconocimiento,
  a.descripcion   AS descripcion_aduana,
  a.aduana        AS codigo_aduana,
  aa.patente      AS patente_agente,
  (SELECT TOP 1 pe.Pedimento FROM PedimentosEncabezado pe WHERE pe.id_referencia = r.id_referencias ORDER BY pe.Pedimento DESC) AS pedimento,
  CASE
    WHEN r.Operacion = 1 THEN (SELECT bi.FechaHoraEvento FROM BitacoraEventosImportacion bi WHERE bi.Referencia = r.id_referencias AND bi.IdEvento = 29)
    WHEN r.Operacion = 2 THEN (SELECT be.FechaHoraEvento FROM BitacoraEventosExportacion be WHERE be.Referencia = r.id_referencias AND be.IdEvento = 29)
  END AS clearance_date
FROM referencias r
LEFT JOIN aduana a ON a.id_Aduana = r.id_aduana
LEFT JOIN agentesaduanales aa ON aa.id_agenteaduanal = r.id_agenteaduanal
`;

// Selecciona referencias de los clientes habilitados con apertura reciente
export const Q_USYNCRO_REFERENCIAS = `${SELECT_REFERENCIA}
WHERE r.id_cliente IN (${idList})
  AND r.FechaApertura > @fApertura
  AND r.Cancelada = 0
`;

// Rescata referencias fuera de la ventana que siguen sin completar (params @ref0..@refN)
export function qUsyncroReferenciasPorLista(cantidad: number): string {
  const params = Array.from({ length: cantidad }, (_, i) => `@ref${i}`).join(',');
  return `${SELECT_REFERENCIA}
WHERE r.id_cliente IN (${idList})
  AND r.Cancelada = 0
  AND r.NumeroDeReferencia IN (${params})
`;
}

