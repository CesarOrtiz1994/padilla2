import { XMLParser } from 'fast-xml-parser';
import type { ConceptoGasto } from '../types';

function parseConceptosGastos(xmlBuffer: Buffer): ConceptoGasto[] {
  if (!xmlBuffer || xmlBuffer.length === 0) return [];

  try {
    const parser = new XMLParser({
      ignoreAttributes: false,
      attributeNamePrefix: '',
      parseAttributeValue: false,
      trimValues: true,
      removeNSPrefix: true,
      parseTagValue: false,
    });

    const parsed = parser.parse(xmlBuffer.toString('utf-8')) as Record<string, unknown>;
    const resultados: ConceptoGasto[] = [];
    const conceptos = extractConceptosFromXML(parsed);

    for (const concepto of conceptos) {
      const c = concepto as Record<string, unknown>;
      const desc = String(c['Descripcion'] ?? c['descripcion'] ?? '').toUpperCase();
      const importe = Math.round(parseFloat(String(c['Importe'] ?? c['importe'] ?? '0')) * 10000) / 10000;

      if (!desc || importe <= 0) continue;

      let tipoConcepto: 'ALMACENAJE' | 'DEMORA' | null = null;
      if (desc.includes('ALMACENAJE')) tipoConcepto = 'ALMACENAJE';
      else if (desc.includes('DEMORA')) tipoConcepto = 'DEMORA';

      if (tipoConcepto) {
        resultados.push({ concepto: tipoConcepto, importe, descripcion: desc });
      }
    }

    return resultados;
  } catch (err) {
    console.error('XML parse error:', (err as Error).message);
    return [];
  }
}

function extractConceptosFromXML(parsed: Record<string, unknown>): unknown[] {
  const conceptos: unknown[] = [];

  type NestedRecord = Record<string, unknown>;
  const comp = parsed?.['Comprobante'] as NestedRecord | undefined;
  const cfdiComp = parsed?.['cfdi:Comprobante'] as NestedRecord | undefined;

  const posiblesRutas: unknown[] = [
    comp?.['Conceptos'] && (comp['Conceptos'] as NestedRecord)['Concepto'],
    (parsed?.['Conceptos'] as NestedRecord | undefined)?.['Concepto'],
    (parsed?.['conceptos'] as NestedRecord | undefined)?.['concepto'],
    parsed?.['Concepto'],
    parsed?.['concepto'],
    cfdiComp?.['cfdi:Conceptos'] && (cfdiComp['cfdi:Conceptos'] as NestedRecord)['cfdi:Concepto'],
    (parsed?.['cfdi:Conceptos'] as NestedRecord | undefined)?.['cfdi:Concepto'],
  ];

  for (const ruta of posiblesRutas) {
    if (ruta) {
      if (Array.isArray(ruta)) conceptos.push(...ruta);
      else conceptos.push(ruta);
      break;
    }
  }

  return conceptos;
}

function agruparConceptosPorTipo(conceptos: ConceptoGasto[]): ConceptoGasto[] {
  const agrupados: Record<string, ConceptoGasto> = {};

  for (const c of conceptos) {
    if (!agrupados[c.concepto]) {
      agrupados[c.concepto] = { concepto: c.concepto, importe: 0, descripcion: c.descripcion };
    }
    agrupados[c.concepto].importe += c.importe;
  }

  return Object.values(agrupados);
}

const IMPORTE_MAX = 9_999_999_999_999.99;

function validarConcepto(concepto: ConceptoGasto, referencia: string, archivo: string): string | null {
  const { importe, descripcion } = concepto;

  if (importe > IMPORTE_MAX)
    return `ALERTA importe fuera de rango: ref=${referencia} archivo=${archivo} concepto=${concepto.concepto} importe=${importe} (max permitido=${IMPORTE_MAX})`;
  if (importe < 0)
    return `ALERTA importe negativo: ref=${referencia} archivo=${archivo} concepto=${concepto.concepto} importe=${importe}`;
  if (!descripcion || descripcion.length === 0)
    return `ALERTA descripcion vacia: ref=${referencia} archivo=${archivo} concepto=${concepto.concepto}`;

  return null;
}

export { parseConceptosGastos, agruparConceptosPorTipo, validarConcepto };
