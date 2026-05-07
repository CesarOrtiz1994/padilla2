// src/config/ftp.js - Configuración de conexión FTP

const ftpConfig = {
  host: process.env.FTP_HOST || 'localhost',
  port: parseInt(process.env.FTP_PORT || '21', 10),
  user: process.env.FTP_USER || '',
  password: process.env.FTP_PASS || '',
  basePath: process.env.FTP_BASE_PATH || '/Referencias'
};

module.exports = { ftpConfig };
