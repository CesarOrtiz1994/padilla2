import 'dotenv/config';
import type { config as MssqlConfig } from 'mssql';
import type { ConnectionOptions } from 'mysql2/promise';

const mssqlConfig: MssqlConfig = {
  server: process.env.MSSQL_SERVER!,
  database: process.env.MSSQL_DB,
  user: process.env.MSSQL_USER,
  password: process.env.MSSQL_PASS,
  port: Number(process.env.MSSQL_PORT ?? 1433),
  connectionTimeout: Number(process.env.MSSQL_CONN_TIMEOUT_MS ?? 30000),
  requestTimeout: Number(process.env.MSSQL_REQUEST_TIMEOUT_MS ?? 300000),
  pool: { max: 5, min: 0, idleTimeoutMillis: 30000 },
  options: {
    encrypt: true,
    trustServerCertificate: true,
  },
};

const mysqlConfig: ConnectionOptions = {
  host: process.env.MYSQL_HOST1,
  user: process.env.MYSQL_USER1,
  password: process.env.MYSQL_PASS1,
  database: process.env.MYSQL_DB1,
  port: Number(process.env.MYSQL_PORT1 ?? 3306),
};

export { mssqlConfig, mysqlConfig };
