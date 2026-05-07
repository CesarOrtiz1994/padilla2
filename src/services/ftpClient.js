// src/services/ftpClient.js - Cliente FTP para descarga de archivos

const ftp = require('basic-ftp');
const { ftpConfig } = require('../config/ftp');

class FTPClient {
  constructor() {
    this.client = new ftp.Client();
    // Timeout de 30 segundos para conexión
    this.client.ftp.timeout = 30000;
    this.client.ftp.verbose = false; // Cambiar a true para debug detallado
  }

  async connect() {
    try {
      console.log(`[FTP-CLIENT] Intentando conectar a ${ftpConfig.host}:${ftpConfig.port}...`);
      console.log(`[FTP-CLIENT] Usuario: ${ftpConfig.user}`);
      console.log(`[FTP-CLIENT] Base path: ${ftpConfig.basePath}`);
      
      await this.client.access({
        host: ftpConfig.host,
        port: ftpConfig.port,
        user: ftpConfig.user,
        password: ftpConfig.password,
        secure: false // FTP explícito, cambiar a 'explict' si es FTPS
      });
      
      console.log(`[FTP-CLIENT] ✅ Conexión exitosa a ${ftpConfig.host}:${ftpConfig.port}`);
      
      // Verificar directorio base
      try {
        const currentDir = await this.client.pwd();
        console.log(`[FTP-CLIENT] Directorio actual: ${currentDir}`);
      } catch (pwdErr) {
        console.log(`[FTP-CLIENT] ⚠️ No se pudo obtener directorio actual (no crítico)`);
      }
      
      return true;
    } catch (err) {
      console.error('[FTP-CLIENT] ❌ Error de conexión FTP:', err.message);
      console.error('[FTP-CLIENT] Host:', ftpConfig.host);
      console.error('[FTP-CLIENT] Port:', ftpConfig.port);
      console.error('[FTP-CLIENT] User:', ftpConfig.user);
      
      if (err.code === 'ECONNREFUSED') {
        console.error('[FTP-CLIENT] → El servidor rechazó la conexión. Verificar host y puerto.');
      } else if (err.code === 'ETIMEDOUT' || err.message.includes('Timeout')) {
        console.error('[FTP-CLIENT] → Timeout de conexión. Posibles causas:');
        console.error('[FTP-CLIENT]   - Servidor FTP no responde');
        console.error('[FTP-CLIENT]   - Firewall bloqueando el puerto');
        console.error('[FTP-CLIENT]   - Host incorrecto o no accesible');
      } else if (err.code === 'ENOTFOUND') {
        console.error('[FTP-CLIENT] → Host no encontrado. Verificar FTP_HOST.');
      }
      
      throw err;
    }
  }

  async downloadFile(remotePath) {
    const startTime = Date.now();
    try {
      console.log(`[FTP-CLIENT] 📥 Descargando: ${remotePath}`);
      
      // Separar ruta y nombre de archivo
      const pathParts = remotePath.split('/');
      const fileName = pathParts.pop();
      const dirPath = pathParts.join('/');
      
      console.log(`[FTP-CLIENT] → Cambiando a directorio: ${dirPath}`);
      
      // Cambiar al directorio del archivo
      await this.client.cwd(dirPath);
      
      const currentDir = await this.client.pwd();
      console.log(`[FTP-CLIENT] → Directorio actual: ${currentDir}`);
      
      // Descargar a buffer
      console.log(`[FTP-CLIENT] → Iniciando descarga de archivo: ${fileName}`);
      const chunks = [];
      
      await this.client.downloadTo(
        { 
          write: (chunk) => { 
            chunks.push(chunk); 
            return true; 
          }
        },
        fileName
      );
      
      const buffer = Buffer.concat(chunks);
      const duration = Date.now() - startTime;
      console.log(`[FTP-CLIENT] ✅ Descarga completa: ${remotePath} (${buffer.length} bytes en ${duration}ms)`);
      
      return buffer;
    } catch (err) {
      const duration = Date.now() - startTime;
      console.error(`[FTP-CLIENT] ❌ Error descargando ${remotePath} (${duration}ms):`, err.message);
      
      if (err.code === 550 || err.message.includes('No such file')) {
        console.error(`[FTP-CLIENT] → Archivo no encontrado en el servidor`);
      } else if (err.code === 530 || err.message.includes('denied')) {
        console.error(`[FTP-CLIENT] → Permisos insuficientes`);
      }
      
      return null;
    }
  }

  async disconnect() {
    try {
      console.log('[FTP-CLIENT] Cerrando conexión...');
      this.client.close();
      console.log('[FTP-CLIENT] ✅ Desconectado');
    } catch (err) {
      console.error('[FTP-CLIENT] ⚠️ Error al cerrar conexión:', err.message);
    }
  }
  
  /**
   * Verificar si el cliente está conectado
   */
  isConnected() {
    return this.client && this.client.closed === false;
  }
}

module.exports = { FTPClient };
