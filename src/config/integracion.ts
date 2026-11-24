// Clientes habilitados para la integración y sus datos fijos (no vienen de la BD)
export interface Colaborador {
  nombre: string;
  email: string;
}

export interface ClienteIntegracion {
  id_cliente: number;
  importador: string;
  colaboradores: Colaborador[];
}

export const CLIENTES_INTEGRACION: ClienteIntegracion[] = [
  { id_cliente: 3435, importador: 'LINDAL DE MEXICO SA DE CV',                          colaboradores: [{ nombre: 'Lindal', email: 'trafico3870@gmail.com' }] },
  { id_cliente: 3785, importador: 'DATAWARE SOLUCIONES S.A. DE C.V.',                   colaboradores: [{ nombre: 'Dataware', email: 'smartdataware0@gmail.com' }] },
  { id_cliente: 4121, importador: 'ALTCAM MEXICO',                                      colaboradores: [{ nombre: 'Altcam', email: 'smartaltcam@gmail.com' }] },
  { id_cliente: 4067, importador: 'SOUND AAROUND MX',                                   colaboradores: [{ nombre: 'Pyle', email: 'smartsounaround@gmail.com' }, { nombre: 'Diana', email: 'daguilar@grupopadilla.com.mx' }] },
  { id_cliente: 4162, importador: 'GERDAU CORSA',                                       colaboradores: [{ nombre: 'Gerdau', email: 'smartgerdau0@gmail.com' }] },
  { id_cliente: 4165, importador: 'TEJIDOS Y CONFECCIONES DEL CENTRO',                  colaboradores: [{ nombre: 'Galga', email: 'smartgalga01@gmail.com' }] },
  { id_cliente: 4167, importador: 'LOGÍSTICA EN COMERCIO EXTERIOR',                     colaboradores: [{ nombre: 'Galga', email: 'smartgalga01@gmail.com' }] },
  { id_cliente: 3769, importador: 'INSTALACIONES PROFESIONALES Y SERVICIOS',            colaboradores: [{ nombre: 'Inprosa', email: 'smartinstalaciones0@gmail.com' }] },
  { id_cliente: 4180, importador: 'Distribuidora De Materiales Eléctricos Industriales de Toluca', colaboradores: [{ nombre: 'Dimeint', email: 'smartdimeint@gmail.com' }] },
  { id_cliente: 1867, importador: 'TECHNOMIX',                                          colaboradores: [{ nombre: 'Technomix', email: 'smarttechnomix@gmail.com' }, { nombre: 'Isabel', email: 'ilagunes@grupopadilla.com.mx' }] },

];

// Map para lookup rápido por id_cliente
export const CLIENTES_MAP = new Map(
  CLIENTES_INTEGRACION.map(c => [c.id_cliente, c])
);

export const CUSTOMS_BROKER_NAME = 'GP COMERCIO EXTERIOR Y ADUANAS';
