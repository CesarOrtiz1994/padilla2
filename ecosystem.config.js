const cwd = '/home/admon/proyect/padilla2';
const tsx = `${cwd}/node_modules/.bin/tsx`;

// Opción A — tsx directo (sin build, recomendado para este proyecto)
const appsTsx = [
  { name: 'scheduler',        script: 'scheduler.ts',        interpreter: tsx, cwd, watch: false, autorestart: true },
  { name: 'scheduler-gastos', script: 'scheduler-gastos.ts', interpreter: tsx, cwd, watch: false, autorestart: true },
];

// Opción B — compilado (requiere `npm run build` antes de iniciar)
const appsDist = [
  { name: 'scheduler',        script: 'dist/scheduler.js',        interpreter: 'node', cwd, watch: false, autorestart: true },
  { name: 'scheduler-gastos', script: 'dist/scheduler-gastos.js', interpreter: 'node', cwd, watch: false, autorestart: true },
  { name: 'scheduler-usyncro', script: 'dist/scheduler-usyncro.js', interpreter: 'node', cwd, watch: false, autorestart: true },
];

// Cambia a appsTsx si prefieres correr sin build
module.exports = { apps: appsDist };

