// Referencias "parciales" (sufijo con letra: -0A, -0B, -1C...) pueden no tener pedimento nunca.
const RE_REFERENCIA_PARCIAL = /-\d*[A-Za-z]+$/;

export function esReferenciaParcial(numeroReferencia: string): boolean {
  return RE_REFERENCIA_PARCIAL.test(numeroReferencia.trim());
}

export function diasTranscurridos(desde: Date | string | null): number | null {
  if (!desde) return null;
  const d = new Date(desde);
  if (isNaN(d.getTime())) return null;
  return Math.floor((Date.now() - d.getTime()) / 86400000);
}

// Arma el customsNumber requerido por Usyncro; null si falta cualquier parte
export function armarCustomsNumber(params: {
  fechaApertura: Date | string | null;
  codigoAduana: string | number | null;
  patenteAgente: string | number | null;
  pedimento: string | number | null;
}): string | null {
  const year2 = params.fechaApertura ? new Date(params.fechaApertura).getFullYear().toString().slice(-2) : null;
  const codigo = params.codigoAduana != null ? String(params.codigoAduana).trim() : null;
  const patente = params.patenteAgente != null ? String(params.patenteAgente).trim() : null;
  const pedimento = params.pedimento != null ? String(params.pedimento).trim() : null;
  if (year2 && codigo && patente && pedimento && pedimento !== '0') {
    return `${year2} ${codigo} ${patente} ${pedimento}`;
  }
  return null;
}
