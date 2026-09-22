const usyncroConfig = {
  baseUrl: process.env.USYNCRO_API_URL ?? 'https://sandbox.usyncro.com/api/v1',
  // Cuenta 2 (integraciones@grupopadilla.com.mx) es la que usa el cron real; se prioriza aquí
  // para que scripts manuales (forzado, diagnóstico, tests) usen la misma cuenta.
  email: process.env.USYNCRO_EMAIL2 ?? process.env.USYNCRO_EMAIL ?? '',
  apiKey: process.env.USYNCRO_API_KEY2 ?? process.env.USYNCRO_API_KEY ?? '',
  templateId: process.env.USYNCRO_TEMPLATE_ID ?? '',
  dias: Number(process.env.USYNCRO_DIAS ?? 2),
  retentionDias: Number(process.env.USYNCRO_RETENTION_DIAS ?? 7),
  // Vigencia del cliente es 1 mes; se deja colchón antes de dar taxes por omitido
  graciaTaxesDias: Number(process.env.USYNCRO_GRACIA_TAXES_DIAS ?? 60),
  // Días adicionales para procesar facturas tardías después de marcar completado (debe ser < retentionDias)
  graciaInvoicesDias: Number(process.env.USYNCRO_GRACIA_INVOICES_DIAS ?? 5),
};

export { usyncroConfig };
