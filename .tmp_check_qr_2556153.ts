import 'dotenv/config';
import mysql from 'mysql2/promise';
import { mysqlConfig } from './src/config/database';

(async () => {
  const my = await mysql.createConnection(mysqlConfig);
  const [reg] = await my.query('SELECT * FROM usyncro_registros WHERE numero_referencia = ?', ['QR260393-00']);
  console.log('REGISTRO:', JSON.stringify(reg, null, 2));
  const [sync] = await my.query('SELECT campo, sincronizado, intentos, ultimo_error FROM usyncro_sync_estado WHERE numero_referencia = ? ORDER BY campo', ['QR260393-00']);
  console.log('SYNC_ESTADO:', JSON.stringify(sync, null, 2));
  await my.end();
})().catch((err) => { console.error(err); process.exit(1); });
