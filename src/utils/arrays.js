// src/utils/arrays.js - Utilidades para manipulación de arrays

/**
 * Divide un array en chunks de tamaño especificado
 */
function chunk(arr, size) {
  const out = [];
  for (let i = 0; i < arr.length; i += size) out.push(arr.slice(i, i + size));
  return out;
}

module.exports = { chunk };
