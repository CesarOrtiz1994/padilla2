// src/utils/dates.js - Utilidades de manejo de fechas

/**
 * Suma 6 horas a una fecha para ajuste de zona horaria (UTC → Mexico City)
 * Filtra fechas inválidas tipo 1899-12-31
 */
function sumar6Horas(fecha) {
  if (!fecha) return null;

  const date = new Date(fecha);
  // Descartar fechas inválidas tipo 1899-12-31
  if (date.getFullYear() <= 1900) return null;
  // Sumar 6 horas (6 * 60 * 60 * 1000 milisegundos)
  date.setTime(date.getTime() + (6 * 60 * 60 * 1000));

  return date;
}

module.exports = { sumar6Horas };
