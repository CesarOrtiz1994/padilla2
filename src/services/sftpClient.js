// src/services/sftpClient.js - Cliente SFTP para descarga de archivos

const SFTPClient = require('ssh2-sftp-client');
const { ftpConfig } = require('../config/ftp');

class SFTPService {
  constructor() {
    this.client = new SFTPClient();
    this.connected = false;
  }

  async connect() {
    try {
      console.log(`[SFTP-CLIENT] Intentando conectar a ${ftpConfig.host}:${ftpConfig.port}...`);
      console.log(`[SFTP-CLIENT] Usuario: ${ftpConfig.user}`);
      console.log(`[SFTP-CLIENT] Base path: ${ftpConfig.basePath}`);
      
      await this.client.connect({
        host: ftpConfig.host,
        port: ftpConfig.port,
        username: ftpConfig.user,
        password: ftpConfig.password,
        readyTimeout: 30000,
        retries: 2
      });
      
      this.connected = true;
      console.log(`[SFTP-CLIENT] Conexion exitosa a ${ftpConfig.host}:${ftpConfig.port}`);
      
      // Verificar directorio base
      try {
        const currentDir = await this.client.cwd();
        console.log(`[SFTP-CLIENT] Directorio actual: ${currentDir}`);
      } catch (pwdErr) {
        console.log(`[SFTP-CLIENT] No se pudo obtener directorio actual (no critico)`);
      }
      
      return true;
    } catch (err) {
      console.error('[SFTP-CLIENT] Error de conexion SFTP:', err.message);
      console.error('[SFTP-CLIENT] Host:', ftpConfig.host);
      console.error('[SFTP-CLIENT] Port:', ftpConfig.port);
      console.error('[SFTP-CLIENT] User:', ftpConfig.user);
      
      if (err.code === 'ECONNREFUSED') {
        console.error('[SFTP-CLIENT] → El servidor rechazó la conexión. Verificar host y puerto.');
      } else if (err.code === 'ETIMEDOUT' || err.message.includes('Timeout')) {
        console.error('[SFTP-CLIENT] → Timeout de conexión. Posibles causas:');
        console.error('[SFTP-CLIENT]   - Servidor SFTP no responde');
        console.error('[SFTP-CLIENT]   - Firewall bloqueando el puerto 22');
        console.error('[SFTP-CLIENT]   - Host incorrecto o no accesible');
      } else if (err.code === 'ENOTFOUND') {
        console.error('[SFTP-CLIENT] → Host no encontrado. Verificar FTP_HOST.');
      } else if (err.level === 'client-authentication') {
        console.error('[SFTP-CLIENT] → Error de autenticación. Verificar usuario y contraseña.');
      }
      
      throw err;
    }
  }

  async downloadFile(remotePath) {
    const startTime = Date.now();
    try {
      console.log(`[SFTP-CLIENT] Descargando: ${remotePath}`);
      
      // Descargar a buffer
      console.log(`[SFTP-CLIENT] → Iniciando descarga...`);
      const buffer = await this.client.get(remotePath);
      
      const duration = Date.now() - startTime;
      console.log(`[SFTP-CLIENT] Descarga completa: ${remotePath} (${buffer.length} bytes en ${duration}ms)`);
      
      return buffer;
    } catch (err) {
      const duration = Date.now() - startTime;
      console.error(`[SFTP-CLIENT] Error descargando ${remotePath} (${duration}ms):`, err.message);
      
      if (err.code === 2 || err.message.includes('No such file')) {
        console.error(`[SFTP-CLIENT] → Archivo no encontrado en el servidor`);
      } else if (err.code === 3 || err.message.includes('Permission denied')) {
        console.error(`[SFTP-CLIENT] → Permisos insuficientes`);
      }
      
      return null;
    }
  }

  async disconnect() {
    try {
      console.log('[SFTP-CLIENT] Cerrando conexión...');
      await this.client.end();
      this.connected = false;
      console.log('[SFTP-CLIENT] Desconectado');
    } catch (err) {
      console.error('[SFTP-CLIENT] Error al cerrar conexion:', err.message);
    }
  }
  
  isConnected() {
    return this.connected;
  }
}

module.exports = { SFTPService };
