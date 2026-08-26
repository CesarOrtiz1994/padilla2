// src/types/index.ts - Tipos compartidos del proyecto ETL

import type { ResultSetHeader } from 'mysql2/promise';

// ── SQL Server rows ──────────────────────────────────────────────────────────

export interface GeneralRow {
  NumeroDeReferencia: string;
  id_referencias: number;
  Pedimento: string | null;
  Operacion: number;
  Clave_pedimento: string | null;
  a_despacho: string | null;
  a_llegada: string | null;
  C_Imp_Exp: string | null;
  facturada: number | null;
  Facturar_a: string | null;
  Agente_Aduanal: string | null;
  Ejecutivo: string | null;
  medio_trasporte: string | null;
  APERTURA: Date | null;
  LLEGADA_MERCAN: Date | null;
  ENTREGA_CLASIFICA: Date | null;
  INICIO_CLASIFICA: Date | null;
  TERMINO_CLASIFICA: Date | null;
  INICIO_GLOSA: Date | null;
  TERMINO_GLOSA: Date | null;
  ENTREGA_GLOSA: Date | null;
  PAGO_PEDIMENTO: Date | null;
  DESPACHO_MERCAN: Date | null;
  ENTREGA_FAC: Date | null;
  FECHA_FAC: Date | null;
  ENTREGA_FAC_CLI: Date | null;
  ENTREGA_CAPTURA: Date | null;
  INICIO_CAPTURA: Date | null;
  TERMINO_CAPTURA: Date | null;
  PRIMER_RECONOCIMIENTO: Date | null;
  Total_Adv: number | null;
  Total_DTA: number | null;
  Total_IVA: number | null;
  Total_Imp: number | null;
  Cancelada: number | null;
}

export interface FacturaRow {
  id_referencias: number;
  IDFactura: number;
  NumFac: string | null;
  Fecha_c: Date | null;
  Incoterm: string | null;
  INCOTER: string | null;
  Moneda: string | null;
  Valor_ME: number | null;
  Valor_USD: number | null;
}

export interface GastoComprobadoRow {
  nombreSistema: string;
  nombreOriginal: string;
  id_referencia: number;
  concepto: string;
  Adicional: string | null;
  facturada: number;
  FechaDeModificacion: Date;
  NumeroDeReferencia: string;
}

// ── MySQL helpers ────────────────────────────────────────────────────────────

export type OkPacketLike = ResultSetHeader & { message?: string };

export interface UpsertStats {
  records: number;
  duplicates: number;
  warnings: number;
  changedRows: number;
  affectedRows: number;
}

export interface UpsertChunksOptions {
  label?: string;
  idIndex?: number;
}

export interface UpsertChunksResult {
  totals: UpsertStats;
  warningsSummary: Record<string, number>;
}

export interface WarningDetail {
  code: number;
  level: string;
  message: string;
}

export interface WarningsResult {
  summary: Record<string, number>;
  details: WarningDetail[];
  count: number;
  error?: string;
}

export interface WarningIssue {
  type: string;
  count: number;
  message: string;
  solution: string;
}

// ── ETL results ──────────────────────────────────────────────────────────────

export interface EtlGeneralResult {
  stats: UpsertStats;
  selected: number;
  dropped: number;
  prepared: number;
  maxApertura: Date | null;
  warningsSummary: Record<string, number>;
}

export interface EtlFacturasResult {
  stats: UpsertStats;
  selected: number;
  dropped: number;
  prepared: number;
  warningsSummary: Record<string, number>;
}

export interface EtlGasFTPError {
  referencia: string;
  archivo: string;
  error: string;
}

export interface EtlGasFTPResult {
  processed: number;
  procesados: number;
  conConceptos: number;
  sinConceptos: number;
  erroresFTP: number;
  inserted: number;
  errors: number;
  duration: number;
  errorDetails: EtlGasFTPError[];
}

// ── XML parser ───────────────────────────────────────────────────────────────

export interface ConceptoGasto {
  concepto: 'ALMACENAJE' | 'DEMORA';
  importe: number;
  descripcion: string;
}
