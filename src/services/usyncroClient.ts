import { usyncroConfig } from '../config/usyncro';
import type { CreateRecordResponse, ActorsResponse, PlacesResponse, ActoresMap, PlacesMap } from '../types/usyncro';

interface LoginResponse {
  data: { attributes: { token: string; entity: { type: string; id: string } } };
}

export class UsyncroClient {
  private token: string | null = null;

  async login(): Promise<void> {
    const res = await fetch(`${usyncroConfig.baseUrl}/auth/login/api-key`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ data: { email: usyncroConfig.email, apiKey: usyncroConfig.apiKey }, meta: {} }),
    });
    if (!res.ok) throw new Error(`[Usyncro] Login fallido: ${res.status} ${await res.text()}`);
    const json = await res.json() as LoginResponse;
    this.token = json.data.attributes.token;
    console.log('[Usyncro] Login exitoso');
  }

  private authHeaders(): Record<string, string> {
    if (!this.token) throw new Error('[Usyncro] No hay token, llama login() primero');
    return { 'Content-Type': 'application/json', 'Authorization': `Bearer ${this.token}` };
  }

  async createRecord(creatorReference: string): Promise<string> {
    const res = await fetch(
      `${usyncroConfig.baseUrl}/record-templates/${usyncroConfig.templateId}/create-record`,
      { method: 'POST', headers: this.authHeaders(), body: JSON.stringify({ data: { creatorReference }, meta: {} }) }
    );
    if (!res.ok) throw new Error(`[Usyncro] createRecord falló: ${res.status} ${await res.text()}`);
    const json = await res.json() as CreateRecordResponse;
    return json.data.id;
  }

  async getActores(recordId: string): Promise<ActoresMap> {
    const res = await fetch(`${usyncroConfig.baseUrl}/records/${recordId}/actors`, { headers: this.authHeaders() });
    if (!res.ok) throw new Error(`[Usyncro] getActores falló: ${res.status} ${await res.text()}`);
    const json = await res.json() as ActorsResponse;
    const map: ActoresMap = {};
    for (const actor of json.data) map[actor.attributes.subtype] = actor.id;
    return map;
  }

  async getPlaces(recordId: string): Promise<PlacesMap> {
    const res = await fetch(`${usyncroConfig.baseUrl}/records/${recordId}/places`, { headers: this.authHeaders() });
    if (!res.ok) throw new Error(`[Usyncro] getPlaces falló: ${res.status} ${await res.text()}`);
    const json = await res.json() as PlacesResponse;
    const map: PlacesMap = {};
    for (const place of json.data) map[place.attributes.subtype] = place.id;
    return map;
  }

  async updateActor(recordId: string, actorId: string, data: Record<string, unknown>): Promise<void> {
    const res = await fetch(`${usyncroConfig.baseUrl}/records/${recordId}/actors/${actorId}`, {
      method: 'PATCH', headers: this.authHeaders(), body: JSON.stringify({ data, meta: {} }),
    });
    if (!res.ok) throw new Error(`[Usyncro] updateActor ${actorId} falló: ${res.status} ${await res.text()}`);
  }

  async updatePlace(recordId: string, placeId: string, data: Record<string, unknown>): Promise<void> {
    const res = await fetch(`${usyncroConfig.baseUrl}/records/${recordId}/places/${placeId}`, {
      method: 'PATCH', headers: this.authHeaders(), body: JSON.stringify({ data, meta: {} }),
    });
    if (!res.ok) throw new Error(`[Usyncro] updatePlace ${placeId} falló: ${res.status} ${await res.text()}`);
  }

  async createInvoice(recordId: string, data: Record<string, unknown>): Promise<void> {
    const res = await fetch(`${usyncroConfig.baseUrl}/records/${recordId}/invoices`, {
      method: 'POST', headers: this.authHeaders(), body: JSON.stringify({ data, meta: {} }),
    });
    if (!res.ok) throw new Error(`[Usyncro] createInvoice falló: ${res.status} ${await res.text()}`);
  }
}
