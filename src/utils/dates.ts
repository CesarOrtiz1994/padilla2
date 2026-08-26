// Suma 6 horas fijas: México abolió el horario de verano, offset permanente UTC-6
function sumar6Horas(fecha: Date | string | null | undefined): Date | null {
  if (!fecha) return null;
  const date = new Date(fecha);
  if (date.getFullYear() <= 1900) return null; // descartar artifacts 1899-12-31 de SQL Server
  date.setTime(date.getTime() + 6 * 60 * 60 * 1000);
  return date;
}

export { sumar6Horas };
