import SFTPClient from 'ssh2-sftp-client';
import { ftpConfig } from '../config/ftp';

class SFTPService {
  private client: SFTPClient;
  private connected = false;

  constructor() {
    this.client = new SFTPClient();
  }

  async connect(): Promise<boolean> {
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
        retries: 2,
      });

      this.connected = true;
      console.log(`[SFTP-CLIENT] Conexion exitosa a ${ftpConfig.host}:${ftpConfig.port}`);

      try {
        const currentDir = await this.client.cwd();
        console.log(`[SFTP-CLIENT] Directorio actual: ${currentDir}`);
      } catch {
        console.log(`[SFTP-CLIENT] No se pudo obtener directorio actual (no critico)`);
      }

      return true;
    } catch (err) {
      const e = err as NodeJS.ErrnoException & { level?: string };
      console.error('[SFTP-CLIENT] Error de conexion SFTP:', e.message);
      console.error('[SFTP-CLIENT] Host:', ftpConfig.host);
      console.error('[SFTP-CLIENT] Port:', ftpConfig.port);
      console.error('[SFTP-CLIENT] User:', ftpConfig.user);

      if (e.code === 'ECONNREFUSED') {
        console.error('[SFTP-CLIENT] → El servidor rechazó la conexión. Verificar host y puerto.');
      } else if (e.code === 'ETIMEDOUT' || e.message.includes('Timeout')) {
        console.error('[SFTP-CLIENT] → Timeout de conexión. Posibles causas:');
        console.error('[SFTP-CLIENT]   - Servidor SFTP no responde');
        console.error('[SFTP-CLIENT]   - Firewall bloqueando el puerto');
        console.error('[SFTP-CLIENT]   - Host incorrecto o no accesible');
      } else if (e.code === 'ENOTFOUND') {
        console.error('[SFTP-CLIENT] → Host no encontrado. Verificar FTP_HOST.');
      } else if (e.level === 'client-authentication') {
        console.error('[SFTP-CLIENT] → Error de autenticación. Verificar usuario y contraseña.');
      }

      throw err;
    }
  }

  async downloadFile(remotePath: string): Promise<Buffer | null> {
    const startTime = Date.now();
    try {
      console.log(`[SFTP-CLIENT] Descargando: ${remotePath}`);
      console.log(`[SFTP-CLIENT] → Iniciando descarga...`);
      const buffer = await this.client.get(remotePath) as Buffer;
      const duration = Date.now() - startTime;
      console.log(`[SFTP-CLIENT] Descarga completa: ${remotePath} (${buffer.length} bytes en ${duration}ms)`);
      return buffer;
    } catch (err) {
      const e = err as NodeJS.ErrnoException;
      const duration = Date.now() - startTime;
      console.error(`[SFTP-CLIENT] Error descargando ${remotePath} (${duration}ms):`, e.message);

      if (e.code === '2' || e.message.includes('No such file')) {
        console.error(`[SFTP-CLIENT] → Archivo no encontrado en el servidor`);
      } else if (e.code === '3' || e.message.includes('Permission denied')) {
        console.error(`[SFTP-CLIENT] → Permisos insuficientes`);
      }

      return null;
    }
  }

  async disconnect(): Promise<void> {
    try {
      console.log('[SFTP-CLIENT] Cerrando conexión...');
      await this.client.end();
      this.connected = false;
      console.log('[SFTP-CLIENT] Desconectado');
    } catch (err) {
      console.error('[SFTP-CLIENT] Error al cerrar conexion:', (err as Error).message);
    }
  }

  isConnected(): boolean {
    return this.connected;
  }
}

export { SFTPService };
