declare module 'ssh2-sftp-client' {
  import { ConnectConfig } from 'ssh2';

  interface SFTPClientOptions {
    host?: string;
    port?: number;
    username?: string;
    password?: string;
    readyTimeout?: number;
    retries?: number;
    [key: string]: unknown;
  }

  class SFTPClient {
    connect(config: SFTPClientOptions): Promise<void>;
    get(remotePath: string): Promise<Buffer | string>;
    cwd(): Promise<string>;
    end(): Promise<void>;
  }

  export = SFTPClient;
}
