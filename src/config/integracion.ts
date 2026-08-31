// Clientes habilitados para la integración y sus datos fijos (no vienen de la BD)
export interface ClienteIntegracion {
  id_cliente: number;
  importador: string;
  nombre: string;
  email: string;
}

export const CLIENTES_INTEGRACION: ClienteIntegracion[] = [
  { id_cliente: 3435, importador: 'LINDAL DE MEXICO SA DE CV',                          nombre: 'Lindal',   email: 'trafico3870@gmail.com'       },
  { id_cliente: 3785, importador: 'DATAWARE SOLUCIONES S.A. DE C.V.',                   nombre: 'Dataware', email: 'smartdataware0@gmail.com'     },
  { id_cliente: 4121, importador: 'ALTCAM MEXICO',                                      nombre: 'Altcam',   email: 'smartaltcam@gmail.com'        },
  { id_cliente: 4067, importador: 'SOUND AAROUND MX',                                   nombre: 'Pyle',     email: 'smartsounaround@gmail.com'    },
  { id_cliente: 4162, importador: 'GERDAU CORSA',                                       nombre: 'Gerdau',   email: 'smartgerdau0@gmail.com'       },
  { id_cliente: 4165, importador: 'TEJIDOS Y CONFECCIONES DEL CENTRO',                  nombre: 'Galga',    email: 'smartgalga01@gmail.com'       },
  { id_cliente: 4167, importador: 'LOGÍSTICA EN COMERCIO EXTERIOR',                     nombre: 'Galga',    email: 'smartgalga01@gmail.com'       },
  { id_cliente: 3769, importador: 'INSTALACIONES PROFESIONALES Y SERVICIOS',            nombre: 'Inprosa',  email: 'smartinstalaciones0@gmail.com' },
  { id_cliente: 4180, importador: 'Distribuidora De Materiales Eléctricos Industriales de Toluca', nombre: 'Dimeint', email: 'smartdimeint@gmail.com' },
];

// Map para lookup rápido por id_cliente
export const CLIENTES_MAP = new Map(
  CLIENTES_INTEGRACION.map(c => [c.id_cliente, c])
);

export const CUSTOMS_BROKER_NAME = 'GP COMERCIO EXTERIOR Y ADUANAS';
