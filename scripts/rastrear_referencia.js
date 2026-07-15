// scripts/rastrear_referencia.js
// Rastrea el proceso ETL-FTP de gastos comprobados para una referencia especifica.
// Muestra, etapa por etapa, que ocurre con la referencia y donde (si acaso) se descarta.
//
// Uso:   node scripts/rastrear_referencia.js <REFERENCIA>
// Ejemplo: node scripts/rastrear_referencia.js MI250033-00

require('dotenv').config();
const sql = require('mssql');
const path = require('path');

const { mssqlConfig } = require('../src/config/database');
const { ftpConfig } = require('../src/config/ftp');
const { SFTPService } = require('../src/services/sftpClient');
const { parseConceptosGastos, validarConcepto } = require('../src/services/xmlParser');

// Query SIN filtro de fecha, para poder distinguir si el problema es la fecha o algo mas.
// Trae ademas FechaDeModificacion y facturada para poder diagnosticar cada filtro.
const Q_REF = `
SELECT
    d.nombreSistema,
    d.nombreOriginal,
    d.id_tipoDocumento,
    g.id_referencia,
    g.concepto,
    g.Adicional,
    r.NumeroDeReferencia,
    r.facturada,
    r.FechaDeModificacion
FROM gastoscomprobados g
INNER JOIN Documentos d ON g.id_gastoComprobado = d.id_propio
INNER JOIN referencias r ON g.id_referencia = r.id_referencias
WHERE r.NumeroDeReferencia = @ref
`;

// Los mismos filtros que aplica el ETL real (Q_GASTOS_COMPROBADOS)
const CONCEPTOS_VALIDOS = ['MANIOBRAS', 'MANIOBRAS Y ALMACENAJES', 'ALMACENAJES', 'DEMORAS'];
const TIPO_DOCUMENTO = 8888;

function linea() {
  console.log('------------------------------------------------------------');
}

function titulo(t) {
  console.log('\n============================================================');
  console.log(t);
  console.log('============================================================');
}

async function rastrear() {
  const ref = process.argv[2];

  if (!ref) {
    console.log('Uso: node scripts/rastrear_referencia.js <REFERENCIA>');
    console.log('Ejemplo: node scripts/rastrear_referencia.js MI250033-00');
    process.exit(1);
  }

  titulo(`RASTREO DE REFERENCIA: ${ref}`);

  let mssqlPool, sftpClient;

  try {
    // ---------------------------------------------------------------
    // CONEXIONES
    // ---------------------------------------------------------------
    console.log('\n[CONEXION] Conectando a SQL Server...');
    mssqlPool = await sql.connect(mssqlConfig);
    console.log('[CONEXION] SQL Server OK');

    // ---------------------------------------------------------------
    // ETAPA 1: Existencia y filtros de la query
    // ---------------------------------------------------------------
    titulo('ETAPA 1: SQL Server (existencia y filtros de la query)');

    const req = new sql.Request(mssqlPool);
    req.input('ref', sql.VarChar, ref);
    const rs = await req.query(Q_REF);
    const registros = rs.recordset;

    if (registros.length === 0) {
      console.log(`\n[X] La referencia ${ref} NO tiene registros en gastoscomprobados`);
      console.log('    con JOIN a Documentos + referencias.');
      console.log('    Posibles causas:');
      console.log('      - La referencia no existe.');
      console.log('      - No tiene gastos comprobados registrados.');
      console.log('      - El documento no esta ligado (id_gastoComprobado != id_propio).');
      console.log('\n>> CONCLUSION: la referencia nunca llega al ETL. Se descarta en ETAPA 1.');
      return;
    }

    console.log(`\n[OK] ${registros.length} registro(s) encontrados para ${ref}\n`);

    const ahora = new Date();
    const haceUnaSemana = new Date(ahora.getTime() - 7 * 86400000);

    // Evaluar cada filtro del ETL real sobre cada registro
    const candidatos = [];

    for (let i = 0; i < registros.length; i++) {
      const r = registros[i];
      linea();
      console.log(`Registro [${i + 1}/${registros.length}]`);
      console.log(`  nombreOriginal    : ${r.nombreOriginal}`);
      console.log(`  nombreSistema     : ${r.nombreSistema}`);
      console.log(`  concepto          : ${r.concepto}`);
      console.log(`  id_tipoDocumento  : ${r.id_tipoDocumento}`);
      console.log(`  facturada         : ${r.facturada}`);
      console.log(`  FechaDeModificacion: ${r.FechaDeModificacion ? new Date(r.FechaDeModificacion).toISOString() : 'null'}`);

      // Filtro A: fecha de modificacion (ultima semana)
      const fMod = r.FechaDeModificacion ? new Date(r.FechaDeModificacion) : null;
      const pasaFecha = fMod && fMod >= haceUnaSemana;
      // Filtro B: facturada = 1
      const pasaFacturada = Number(r.facturada) === 1;
      // Filtro C: tipo documento 8888
      const pasaTipo = Number(r.id_tipoDocumento) === TIPO_DOCUMENTO;
      // Filtro D: concepto valido
      const pasaConcepto = CONCEPTOS_VALIDOS.includes((r.concepto || '').toUpperCase());
      // Filtro E: nombre termina en .xml
      const pasaXml = /\.xml$/i.test(r.nombreOriginal || '');

      console.log('  --- Evaluacion de filtros del ETL ---');
      console.log(`   [${pasaFecha ? 'PASA' : 'FALLA'}] FechaDeModificacion >= ${haceUnaSemana.toISOString()} (ultima semana)`);
      console.log(`   [${pasaFacturada ? 'PASA' : 'FALLA'}] facturada = 1`);
      console.log(`   [${pasaTipo ? 'PASA' : 'FALLA'}] id_tipoDocumento = ${TIPO_DOCUMENTO}`);
      console.log(`   [${pasaConcepto ? 'PASA' : 'FALLA'}] concepto IN (${CONCEPTOS_VALIDOS.join(', ')})`);
      console.log(`   [${pasaXml ? 'PASA' : 'FALLA'}] nombreOriginal LIKE '%.xml'`);

      const pasaTodo = pasaFecha && pasaFacturada && pasaTipo && pasaConcepto && pasaXml;

      if (pasaTodo) {
        console.log('  >> Este registro SI seria seleccionado por el ETL real.');
        candidatos.push(r);
      } else {
        console.log('  >> Este registro NO seria seleccionado por el ETL real (falla algun filtro arriba).');
      }
    }
    linea();

    if (candidatos.length === 0) {
      console.log('\n>> CONCLUSION: Ningun registro pasa todos los filtros de la query.');
      console.log('   La referencia se descarta en ETAPA 1. Revisa cual filtro dice FALLA arriba.');
      console.log('   (El filtro de fecha es el mas comun: el ETL solo procesa la ultima semana.)');
      return;
    }

    console.log(`\n[OK] ${candidatos.length} registro(s) pasan la ETAPA 1. Continuando a SFTP...`);

    // ---------------------------------------------------------------
    // ETAPA 2: Descarga desde SFTP
    // ---------------------------------------------------------------
    titulo('ETAPA 2: SFTP (descarga del archivo XML)');

    console.log('\n[CONEXION] Conectando a SFTP...');
    sftpClient = new SFTPService();
    await sftpClient.connect();
    console.log('[CONEXION] SFTP OK');

    for (let i = 0; i < candidatos.length; i++) {
      const r = candidatos[i];
      const numRef = r.NumeroDeReferencia;
      const fileName = r.nombreSistema;
      const remotePath = path.posix.join(ftpConfig.basePath, numRef, 'GASTOS COMPROBADOS', fileName);

      linea();
      console.log(`Candidato [${i + 1}/${candidatos.length}]`);
      console.log(`  Ruta FTP: ${remotePath}`);

      let xmlBuffer = null;
      try {
        xmlBuffer = await sftpClient.downloadFile(remotePath);
      } catch (e) {
        console.log(`  [X] Excepcion al descargar: ${e.message}`);
      }

      if (!xmlBuffer) {
        console.log('  [X] No se pudo descargar el archivo.');
        // Listar la carpeta para ver que hay realmente
        const carpeta = path.posix.join(ftpConfig.basePath, numRef, 'GASTOS COMPROBADOS');
        try {
          const lista = await sftpClient.client.list(carpeta);
          if (lista.length > 0) {
            console.log('  La carpeta existe. Archivos disponibles:');
            lista.forEach(f => console.log(`    - ${f.name} (${f.size} bytes)`));
            console.log('  >> El archivo esperado no coincide con lo que hay en el servidor.');
          } else {
            console.log('  La carpeta existe pero esta vacia.');
          }
        } catch (listErr) {
          console.log(`  La carpeta "${carpeta}" no existe o no es accesible.`);
        }
        console.log('  >> Este candidato se descarta en ETAPA 2 (FTP).');
        continue;
      }

      console.log(`  [OK] Descargado (${xmlBuffer.length} bytes)`);

      // ---------------------------------------------------------------
      // ETAPA 3: Parseo del XML
      // ---------------------------------------------------------------
      titulo('ETAPA 3: Parseo del XML (extraccion de conceptos)');

      const conceptos = parseConceptosGastos(xmlBuffer);
      console.log(`\n  Conceptos ALMACENAJE/DEMORA detectados: ${conceptos.length}`);

      if (conceptos.length === 0) {
        console.log('  [X] El parser no encontro conceptos de ALMACENAJE ni DEMORA.');
        console.log('  NOTA: el parser solo reconoce descripciones que contengan');
        console.log('        "ALMACENAJE" o "DEMORA". Si el XML solo trae MANIOBRAS,');
        console.log('        se descarta aqui aunque haya pasado la query.');
        console.log('\n  --- Vista previa del XML (primeros 1500 caracteres) ---');
        console.log(xmlBuffer.toString('utf-8').substring(0, 1500));
        console.log('  --- fin vista previa ---');
        console.log('\n  >> Este candidato se descarta en ETAPA 3 (parseo).');
        continue;
      }

      conceptos.forEach((c, idx) => {
        console.log(`    [${idx + 1}] concepto=${c.concepto} importe=$${c.importe.toFixed(2)} desc="${c.descripcion}"`);
      });

      // ---------------------------------------------------------------
      // ETAPA 4: Validacion previa a insertar
      // ---------------------------------------------------------------
      titulo('ETAPA 4: Validacion previa a insertar en MySQL');

      let validos = 0;
      for (const c of conceptos) {
        const alerta = validarConcepto(c, numRef, fileName);
        if (alerta) {
          console.log(`  [X] ${alerta}`);
        } else {
          validos++;
          console.log(`  [OK] concepto=${c.concepto} importe=$${c.importe.toFixed(2)} -> listo para insertar`);
        }
      }

      console.log(`\n  Resultado: ${validos}/${conceptos.length} conceptos pasan la validacion.`);
      if (validos > 0) {
        console.log('  >> Este candidato SI llegaria a insertarse en MySQL (ftp_adicional).');
      } else {
        console.log('  >> Todos los conceptos fallan la validacion. Se descarta en ETAPA 4.');
      }
    }
    linea();

    titulo('RASTREO COMPLETADO');
    console.log('Revisa arriba en que ETAPA aparece la primera marca [X] o FALLA:');
    console.log('  ETAPA 1 -> problema de filtros en SQL Server (fecha, facturada, tipo, concepto, .xml)');
    console.log('  ETAPA 2 -> el archivo XML no esta en el SFTP');
    console.log('  ETAPA 3 -> el XML no contiene conceptos ALMACENAJE/DEMORA reconocibles');
    console.log('  ETAPA 4 -> el importe/descripcion no pasa la validacion');

  } catch (err) {
    console.error('\n[ERROR FATAL]', err.message);
    console.error(err.stack);
  } finally {
    try { if (sftpClient) await sftpClient.disconnect(); } catch (e) { /* noop */ }
    try { if (mssqlPool) await mssqlPool.close(); } catch (e) { /* noop */ }
  }
}

rastrear();
