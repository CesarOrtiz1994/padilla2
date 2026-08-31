// src/types/usyncro.ts - Tipos para la API de Usyncro

export interface UsyncroRecord {
  id: string;
  attributes: {
    status: string;
    creatorReference: string | null;
    buyer: { type: string; id: string } | null;
    supplier: { type: string; id: string } | null;
  };
}

export interface CreateRecordResponse {
  data: UsyncroRecord;
}

export interface RecordActor {
  type: 'record-actors';
  id: string;
  attributes: {
    subtype: string;
    name: string | null;
    recordReference: string | null;
    legalId: string | null;
    email: string | null;
    contactName: string | null;
    contactEmail: string | null;
  };
}

export interface RecordPlace {
  type: 'record-places';
  id: string;
  attributes: {
    subtype: string;
    name: string | null;
    address: {
      country: string | null;
      subdivision: string | null;
      locality: string | null;
      postalCode: string | null;
      street: string | null;
    } | null;
  };
}

export interface ActorsResponse {
  data: RecordActor[];
}

export interface PlacesResponse {
  data: RecordPlace[];
}

// Mapa de actores indexado por subtype
export type ActoresMap = Partial<Record<string, string>>;
export type PlacesMap  = Partial<Record<string, string>>;

// Row de usyncro_registros en MySQL
export interface UsyncroRegistro {
  numero_referencia: string;
  id_cliente: number;
  record_id: string;
  actor_creator_id:        string | null;
  actor_buyer_id:          string | null;
  actor_supplier_id:       string | null;
  actor_customs_broker_id: string | null;
  actor_taxes_id:          string | null;
  place_destination_id:    string | null;
  fecha_completado:        Date | null;
}

// Row de usyncro_sync_estado en MySQL
export interface SyncEstado {
  numero_referencia: string;
  campo: string;
  sincronizado: number;
  intentos: number;
  ultimo_error: string | null;
  sincronizado_en: Date | null;
}
