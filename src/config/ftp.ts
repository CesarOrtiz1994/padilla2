const ftpConfig = {
  host: process.env.FTP_HOST ?? 'localhost',
  port: parseInt(process.env.FTP_PORT ?? '22', 10),
  user: process.env.FTP_USER ?? '',
  password: process.env.FTP_PASS ?? '',
  basePath: process.env.FTP_BASE_PATH ?? '/Referencias',
};

export { ftpConfig };
