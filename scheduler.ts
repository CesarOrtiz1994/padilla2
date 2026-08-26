import 'dotenv/config';
import cron from 'node-cron';
import { execFile } from 'child_process';
import path from 'path';

const TZ = process.env.TZ ?? 'America/Mexico_City';
// Detecta si corre como .ts (tsx) o como .js compilado (dist/)
const runningAsTS = __filename.endsWith('.ts');
const [interpreter, script] = runningAsTS
  ? [path.join(__dirname, 'node_modules', '.bin', 'tsx'), 'index.ts']
  : [process.execPath, path.join(__dirname, 'index.js')];
let isRunning = false;

function runJob(): void {
  if (isRunning) {
    console.log('Job anterior sigue corriendo, se omite esta ejecución.');
    return;
  }
  isRunning = true;
  const started = new Date();
  console.log(`ETL start ${started.toISOString()}`);

  const child = execFile(interpreter, [script], { env: process.env });

  child.stdout?.on('data', (d: Buffer) => process.stdout.write(d));
  child.stderr?.on('data', (d: Buffer) => process.stderr.write(d));

  child.on('close', (code: number | null) => {
    const ended = new Date();
    const ms = ended.getTime() - started.getTime();
    console.log(`ETL end (code=${code}) ${ended.toISOString()} (${Math.round(ms / 1000)}s)`);
    isRunning = false;
  });
}

runJob();

// Cron cada 3 horas: descomentar cuando se requiera en producción
// cron.schedule('0 0,3,6,9,12,15,18,21 * * *', runJob, { timezone: TZ });
cron.schedule('45 13 * * *', runJob, { timezone: TZ });
