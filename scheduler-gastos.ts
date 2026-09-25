import 'dotenv/config';
import cron from 'node-cron';
import { execFile } from 'child_process';
import path from 'path';

const TZ = process.env.TZ ?? 'America/Mexico_City';
// Detecta si corre como .ts (tsx) o como .js compilado (dist/)
const runningAsTS = __filename.endsWith('.ts');
const [interpreter, script] = runningAsTS
  ? [path.join(__dirname, 'node_modules', '.bin', 'tsx'), 'etl-gastos-runner.ts']
  : [process.execPath, path.join(__dirname, 'etl-gastos-runner.js')];
let isRunning = false;

function runJob(): void {
  if (isRunning) {
    console.log('[Scheduler-Gastos] Job anterior sigue corriendo, se omite esta ejecución.');
    return;
  }
  isRunning = true;

  const started = new Date();
  console.log(`[Scheduler-Gastos] ETL-FTP start ${started.toISOString()}`);

  const child = execFile(interpreter, [script], { env: process.env });

  child.stdout?.on('data', (d: Buffer) => process.stdout.write(d));
  child.stderr?.on('data', (d: Buffer) => process.stderr.write(d));

  child.on('close', (code: number | null) => {
    const ended = new Date();
    const ms = ended.getTime() - started.getTime();
    console.log(`[Scheduler-Gastos] ETL-FTP end (code=${code}) ${ended.toISOString()} (${Math.round(ms / 1000)}s)`);
    isRunning = false;
  });
}

// Programa: 00:10 y 12:10 todos los días
cron.schedule('10 0,12 * * *', runJob, { timezone: TZ });

console.log('[Scheduler-Gastos] Iniciado. Ejecuciones diarias: 00:10 y 12:10');
