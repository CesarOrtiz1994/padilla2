// test/test-xml-parser.js - Script para probar el parser con XML local

const fs = require('fs');
const path = require('path');
const { parseConceptosGastos, agruparConceptosPorTipo } = require('../src/services/xmlParser');

const xmlFile = process.argv[2] || './SATO_GTOSCOM_655188.xml';

console.log('===============================================');
console.log('TEST DE PARSER XML');
console.log('===============================================');
console.log(`Archivo: ${xmlFile}`);

if (!fs.existsSync(xmlFile)) {
  console.error(`Archivo no encontrado: ${xmlFile}`);
  process.exit(1);
}

const xmlBuffer = fs.readFileSync(xmlFile);
console.log(`Tamaño: ${xmlBuffer.length} bytes`);

console.log('\n→ Parseando XML...');
const conceptos = parseConceptosGastos(xmlBuffer);

console.log(`\nConceptos encontrados: ${conceptos.length}`);

if (conceptos.length > 0) {
  console.log('\nDetalle de conceptos:');
  conceptos.forEach((c, i) => {
    console.log(`  [${i + 1}] ${c.concepto}: $${c.importe.toFixed(2)} - "${c.descripcion.substring(0, 60)}..."`);
  });

  console.log('\nAgrupando por tipo...');
  const agrupados = agruparConceptosPorTipo(conceptos);
  
  console.log('\nResultados agrupados:');
  agrupados.forEach((a, i) => {
    console.log(`  [${i + 1}] ${a.concepto}: $${a.importe.toFixed(2)}`);
  });
} else {
  console.log('No se encontraron conceptos Almacenaje o Demora');
}

console.log('\n===============================================');
console.log('Test completado');
console.log('===============================================');
