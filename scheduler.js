// scheduler.js
require('dotenv').config();
const cron = require('node-cron');
const { execFile } = require('child_process');

const TZ = process.env.TZ || 'America/Mexico_City'; // ajusta si quieres
let isRunning = false;

function runJob() {
  if (isRunning) {
    console.log('⏭️  Job anterior sigue corriendo, se omite esta ejecución.');
    return;
  }
  isRunning = true;
  const started = new Date();
  console.log(`▶️  ETL start ${started.toISOString()}`);

  const child = execFile(process.execPath, ['index.js'], { env: process.env });

  child.stdout.on('data', d => process.stdout.write(d));
  child.stderr.on('data', d => process.stderr.write(d));

  child.on('close', code => {
    const ended = new Date();
    const ms = ended - started;
    console.log(`⏹️  ETL end (code=${code}) ${ended.toISOString()} (${Math.round(ms/1000)}s)`);
    isRunning = false;
  });
}

// Ejecuta una vez al arrancar el scheduler
runJob();

// Programa: minuto 5 de cada hora (evita top-of-hour spikes)
cron.schedule('5 * * * *', runJob, { timezone: TZ });
