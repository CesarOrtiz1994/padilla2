// src/utils/money.js - Utilidades para conversión de valores monetarios

/**
 * Convierte valores money de SQL Server a números para MySQL
 * Preserva los valores originales sin limitarlos
 */
function safeMoneyValue(value, fieldName = 'desconocido') {
  try {
    // Caso especial: si es null o undefined
    if (value === null || value === undefined) {
      return null;
    }

    // Si es un objeto (como en algunos resultados de SQL Server)
    if (typeof value === 'object') {
      if (value.value !== undefined) {
        value = value.value;
      } else {
        return null;
      }
    }

    // Convertir a número si es string
    let numValue;
    if (typeof value === 'string') {
      // Eliminar caracteres no numéricos excepto punto decimal y signo negativo
      const cleanValue = value.replace(/[^\d.-]/g, '');
      numValue = parseFloat(cleanValue);
    } else {
      numValue = Number(value);
    }

    // Verificar si es un número válido
    if (isNaN(numValue)) return null;

    return numValue;
  } catch (err) {
    console.error(`Error procesando valor monetario para ${fieldName}:`, err);
    return null;
  }
}

module.exports = { safeMoneyValue };
