// src/services/xmlParser.js - Parser de XML para extraer conceptos de gastos

const { XMLParser } = require('fast-xml-parser');

/**
 * Parsea un buffer XML y extrae conceptos de Almacenaje y Demora
 * @param {Buffer} xmlBuffer - Buffer con contenido XML
 * @returns {Array} Array de objetos { concepto, importe, descripcion }
 */
function parseConceptosGastos(xmlBuffer) {
  if (!xmlBuffer || xmlBuffer.length === 0) {
    return [];
  }

  try {
    const parser = new XMLParser({
      ignoreAttributes: false,
      attributeNamePrefix: '',
      parseAttributeValue: false,
      trimValues: true,
      removeNSPrefix: true, // Quitar prefijos de namespace (cfdi:)
      parseTagValue: false
    });

    const xmlString = xmlBuffer.toString('utf-8');
    const parsed = parser.parse(xmlString);

    const resultados = [];

    // Buscar conceptos en diferentes posibles ubicaciones del XML
    const conceptos = extractConceptosFromXML(parsed);

    for (const concepto of conceptos) {
      const desc = (concepto.Descripcion || concepto.descripcion || '').toUpperCase();
      const importe = parseFloat(concepto.Importe || concepto.importe || 0);

      if (!desc || importe <= 0) continue;

      // Determinar si es Almacenaje o Demora
      let tipoConcepto = null;
      
      if (desc.includes('ALMACENAJE')) {
        tipoConcepto = 'ALMACENAJE';
      } else if (desc.includes('DEMORA')) {
        tipoConcepto = 'DEMORA';
      }

      if (tipoConcepto) {
        resultados.push({
          concepto: tipoConcepto,
          importe: importe,
          descripcion: desc
        });
      }
    }

    return resultados;

  } catch (err) {
    console.error('XML parse error:', err.message);
    return [];
  }
}

/**
 * Extrae conceptos de diferentes estructuras XML posibles
 */
function extractConceptosFromXML(parsed) {
  const conceptos = [];

  // Intentar diferentes rutas comunes en XML de facturas/gastos
  // Con removeNSPrefix: true, los nombres no tendrán prefijo cfdi:
  const posiblesRutas = [
    // CFDI 4.0/3.3 - Conceptos (sin namespace)
    parsed?.Comprobante?.Conceptos?.Concepto,
    // Otra estructura plana
    parsed?.Conceptos?.Concepto,
    parsed?.conceptos?.concepto,
    // Lista directa
    parsed?.Concepto,
    parsed?.concepto,
    // Aún con prefijo (por si removeNSPrefix no funciona)
    parsed?.['cfdi:Comprobante']?.['cfdi:Conceptos']?.['cfdi:Concepto'],
    parsed?.['cfdi:Conceptos']?.['cfdi:Concepto']
  ];

  for (const ruta of posiblesRutas) {
    if (ruta) {
      // Si es array, agregar todos
      if (Array.isArray(ruta)) {
        conceptos.push(...ruta);
      } else {
        // Si es objeto único, agregarlo
        conceptos.push(ruta);
      }
      break; // Encontramos una ruta válida, salimos
    }
  }

  return conceptos;
}

/**
 * Agrupa conceptos por tipo y suma importes
 * @param {Array} conceptos - Array de { concepto, importe, descripcion }
 * @returns {Array} Array agrupado de { concepto, importe_total, descripcion }
 */
function agruparConceptosPorTipo(conceptos) {
  const agrupados = {};

  for (const c of conceptos) {
    if (!agrupados[c.concepto]) {
      agrupados[c.concepto] = {
        concepto: c.concepto,
        importe: 0,
        descripcion: c.descripcion
      };
    }
    agrupados[c.concepto].importe += c.importe;
  }

  return Object.values(agrupados);
}

module.exports = {
  parseConceptosGastos,
  agruparConceptosPorTipo
};
