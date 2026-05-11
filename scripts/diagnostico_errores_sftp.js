// scripts/diagnostico_errores_sftp.js
// Diagnostica referencias con error "No se pudo obtener XML"
// Uso: node scripts/diagnostico_errores_sftp.js [referencia1] [referencia2] ...
// Ejemplo: node scripts/diagnostico_errores_sftp.js AP260018-00 MI260040-00

require('dotenv').config();
const sql = require('mssql');
const path = require('path');

const { mssqlConfig } = require('../src/config/database');
const { SFTPService } = require('../src/services/sftpClient');

const Q_ARCHIVOS_REF = `
SELECT
    d.nombreSistema,
    d.nombreOriginal,
    g.id_referencia,
    g.concepto,
    g.Adicional,
    r.NumeroDeReferencia,
    r.facturada,
    r.FechaDeModificacion
FROM gastoscomprobados g
INNER JOIN Documentos d ON g.id_gastoComprobado = d.id_propio
INNER JOIN referencias r ON g.id_referencia = r.id_referencias
WHERE r.NumeroDeReferencia IN (REFERENCIAS)
  AND d.id_tipoDocumento = 8888
  AND g.concepto IN ('MANIOBRAS', 'MANIOBRAS Y ALMACENAJES', 'ALMACENAJES', 'DEMORAS')
  AND d.nombreOriginal LIKE '%.xml'
`;

async function diagnosticar() {
  const refs = process.argv.slice(2);

  if (refs.length === 0) {
    console.log('Uso: node scripts/diagnostico_errores_sftp.js REF1 REF2 ...');
    console.log('Ejemplo: node scripts/diagnostico_errores_sftp.js AP260018-00 MI260040-00');
    process.exit(1);
  }

  console.log('============================================');
  console.log('DIAGNOSTICO DE ERRORES SFTP');
  console.log('============================================');
  console.log('Referencias a verificar:', refs.join(', '));

  let mssqlPool, sftpClient;

  try {
    // Conectar BD
    console.log('\n[1] Conectando a SQL Server...');
    mssqlPool = await sql.connect(mssqlConfig);
    console.log('    SQL Server conectado');

    // Conectar SFTP
    console.log('\n[2] Conectando a SFTP...');
    sftpClient = new SFTPService();
    await sftpClient.connect();
    console.log('    SFTP conectado');

    // Buscar archivos en SQL Server
    console.log('\n[3] Buscando archivos en SQL Server...');
    const placeholders = refs.map(r => `'${r}'`).join(', ');
    const query = Q_ARCHIVOS_REF.replace('REFERENCIAS', placeholders);
    const req = new sql.Request(mssqlPool);
    const rs = await req.query(query);
    const registros = rs.recordset;

    console.log(`    ${registros.length} registro(s) encontrados en SQL Server`);

    if (registros.length === 0) {
      console.log('\n    ADVERTENCIA: No se encontraron registros en SQL Server para esas referencias.');
      console.log('    Verifica que las referencias existan y tengan documentos tipo 8888.');
      return;
    }

    // Verificar cada archivo en SFTP
    console.log('\n[4] Verificando archivos en SFTP...\n');

    const resultados = [];

    for (const reg of registros) {
      const numRef = reg.NumeroDeReferencia;
      const fileName = reg.nombreSistema;
      const remotePath = path.posix.join('/Referencias', numRef, 'GASTOS COMPROBADOS', fileName);

      process.stdout.write(`    [${numRef}] ${fileName} -> `);

      try {
        // Intentar obtener info del archivo (sin descargarlo completo)
        const stat = await sftpClient.client.stat(remotePath);
        console.log(`EXISTE (${stat.size} bytes, modificado: ${new Date(stat.modifyTime * 1000).toISOString().substring(0, 10)})`);
        resultados.push({ ref: numRef, archivo: fileName, estado: 'EXISTE', size: stat.size, path: remotePath });
      } catch (statErr) {
        // Si stat falla, intentar listar la carpeta para ver qué hay
        console.log(`NO ENCONTRADO`);
        resultados.push({ ref: numRef, archivo: fileName, estado: 'NO_ENCONTRADO', path: remotePath });

        // Listar la carpeta para ver qué archivos hay realmente
        const carpeta = path.posix.join('/Referencias', numRef, 'GASTOS COMPROBADOS');
        try {
          const lista = await sftpClient.client.list(carpeta);
          if (lista.length > 0) {
            console.log(`      Carpeta existe. Archivos disponibles:`);
            lista.forEach(f => console.log(`        - ${f.name} (${f.size} bytes)`));
          } else {
            console.log(`      Carpeta existe pero esta vacia`);
          }
        } catch (listErr) {
          // Intentar listar la referencia para ver si la carpeta existe
          const carpetaRef = path.posix.join('/Referencias', numRef);
          try {
            const listaRef = await sftpClient.client.list(carpetaRef);
            console.log(`      Carpeta GASTOS COMPROBADOS no existe. Subcarpetas de ${numRef}:`);
            listaRef.forEach(f => console.log(`        - ${f.name}`));
          } catch (e) {
            console.log(`      La referencia ${numRef} tampoco existe en SFTP`);
          }
        }
      }
    }

    // Resumen final
    console.log('\n============================================');
    console.log('RESUMEN');
    console.log('============================================');
    const existentes = resultados.filter(r => r.estado === 'EXISTE');
    const faltantes = resultados.filter(r => r.estado === 'NO_ENCONTRADO');
    console.log(`Archivos encontrados en SFTP : ${existentes.length}`);
    console.log(`Archivos NO encontrados      : ${faltantes.length}`);

    if (faltantes.length > 0) {
      console.log('\nArchivos faltantes:');
      faltantes.forEach(f => console.log(`  - ${f.ref}: ${f.path}`));
    }

  } catch (err) {
    console.error('\nError fatal:', err.message);
  } finally {
    if (sftpClient) await sftpClient.disconnect();
    if (mssqlPool) await mssqlPool.close();
  }
}

diagnosticar();
