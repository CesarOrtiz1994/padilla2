// Test rápido de conexión a Usyncro: login -> logout (local)
import 'dotenv/config';
import { usyncroConfig } from './src/config/usyncro';
import { UsyncroClient } from './src/services/usyncroClient';

const SEP = '═'.repeat(60);

(async () => {
  const client = new UsyncroClient();
  let exitCode = 0;

  try {
    console.log(SEP);
    console.log('TEST CONEXIÓN USYNCRO');
    console.log(SEP);
    console.log(`baseUrl: ${usyncroConfig.baseUrl}`);
    console.log(`email  : ${usyncroConfig.email}`);

    console.log('\n[1] Login...');
    await client.login();
    console.log('    ✔ Token obtenido');
  } catch (err) {
    exitCode = 1;
    console.error('\n✖ Error durante la prueba:', (err as Error).message);
  } finally {
    console.log('\n[2] Logout...');
    client.logout();
  }

  console.log('\n' + SEP);
  console.log(exitCode === 0 ? 'RESULTADO: OK' : 'RESULTADO: FALLÓ');
  console.log(SEP);
  process.exit(exitCode);
})();

