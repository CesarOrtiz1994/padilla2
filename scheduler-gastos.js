// scheduler-gastos.js - Scheduler para ETL de gastos comprobados vía FTP
// Corre diariamente a la 1:30 AM (evita empalme con ETL principal)

require('dotenv').config();
const cron = require('node-cron');
const { execFile } = require('child_process');

const TZ = process.env.TZ || 'America/Mexico_City';
let isRunning = false;

function runJob() {
  if (isRunning) {
    console.log('[Scheduler-Gastos] Job anterior sigue corriendo, se omite esta ejecución.');
    return;
  }
  isRunning = true;

  const started = new Date();
  console.log(`[Scheduler-Gastos] ETL-FTP start ${started.toISOString()}`);

  const child = execFile(process.execPath, ['etl-gastos-runner.js'], { env: process.env });

  child.stdout.on('data', d => process.stdout.write(d));
  child.stderr.on('data', d => process.stderr.write(d));

  child.on('close', code => {
    const ended = new Date();
    const ms = ended - started;
    console.log(`[Scheduler-Gastos] ETL-FTP end (code=${code}) ${ended.toISOString()} (${Math.round(ms / 1000)}s)`);
    isRunning = false;
  });
}

// Ejecutar una vez al iniciar (opcional, descomentar si se desea)
// runJob();

// Programa: 1:30 AM todos los días
// Evita empalme con ETL principal que corre cada 3 horas
cron.schedule('30 1 * * *', runJob, { timezone: TZ });

console.log('[Scheduler-Gastos] Iniciado. Próxima ejecución: 1:30 AM');
