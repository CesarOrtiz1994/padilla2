const usyncroConfig = {
  baseUrl: process.env.USYNCRO_API_URL ?? 'https://sandbox.usyncro.com/api/v1',
  email: process.env.USYNCRO_EMAIL ?? '',
  apiKey: process.env.USYNCRO_API_KEY ?? '',
  templateId: process.env.USYNCRO_TEMPLATE_ID ?? '',
  dias: Number(process.env.USYNCRO_DIAS ?? 2),
  retentionDias: Number(process.env.USYNCRO_RETENTION_DIAS ?? 7),
};

export { usyncroConfig };
