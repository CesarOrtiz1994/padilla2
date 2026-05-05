// scheduler.js
require('dotenv').config();
const cron = require('node-cron');
const { execFile } = require('child_process');

const TZ = process.env.TZ || 'America/Mexico_City'; // ajusta si quieres
let isRunning = false;

function runJob() {
  if (isRunning) {
    console.log('Job anterior sigue corriendo, se omite esta ejecución.');
    return;
  }
  isRunning = true;
  const started = new Date();
  console.log(`ETL start ${started.toISOString()}`);

  const child = execFile(process.execPath, ['index.js'], { env: process.env });

  child.stdout.on('data', d => process.stdout.write(d));
  child.stderr.on('data', d => process.stderr.write(d));

  child.on('close', code => {
    const ended = new Date();
    const ms = ended - started;
    console.log(`ETL end (code=${code}) ${ended.toISOString()} (${Math.round(ms/1000)}s)`);
    isRunning = false;
  });
}

// Ejecuta una vez al arrancar el scheduler
runJob();

// Programa: cada 3 horas (3am, 6am, 9am, 12pm, 3pm, 6pm, 9pm, 12am)
//cron.schedule('0 0,3,6,9,12,15,18,21 * * *', runJob, { timezone: TZ });
// Para prueba descomentar: '30 13 * * *' (1:30 PM)
cron.schedule('45 13 * * *', runJob, { timezone: TZ });  // ← solo esta línea
