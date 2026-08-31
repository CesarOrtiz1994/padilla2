import 'dotenv/config';
import cron from 'node-cron';
import { execFile } from 'child_process';
import path from 'path';

const TZ = process.env.TZ ?? 'America/Mexico_City';
const runningAsTS = __filename.endsWith('.ts');
const [interpreter, script] = runningAsTS
  ? [path.join(__dirname, 'node_modules', '.bin', 'tsx'), 'etl-usyncro-runner.ts']
  : [process.execPath, path.join(__dirname, 'etl-usyncro-runner.js')];
let isRunning = false;

function runJob(): void {
  if (isRunning) {
    console.log('[Scheduler-Usyncro] Job anterior sigue corriendo, se omite esta ejecución.');
    return;
  }
  isRunning = true;

  const started = new Date();
  console.log(`[Scheduler-Usyncro] ETL-Usyncro start ${started.toISOString()}`);

  const child = execFile(interpreter, [script], { env: process.env });

  child.stdout?.on('data', (d: Buffer) => process.stdout.write(d));
  child.stderr?.on('data', (d: Buffer) => process.stderr.write(d));

  child.on('close', (code: number | null) => {
    const ended = new Date();
    const ms = ended.getTime() - started.getTime();
    console.log(`[Scheduler-Usyncro] ETL-Usyncro end (code=${code}) ${ended.toISOString()} (${Math.round(ms / 1000)}s)`);
    isRunning = false;
  });
}

// 10 PM hora México
cron.schedule('0 22 * * *', runJob, { timezone: TZ });

console.log('[Scheduler-Usyncro] Iniciado. Próxima ejecución: 10:00 PM México');
