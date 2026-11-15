import type { Session } from './types';
let csrf = '';
let principal: string | undefined; let sessionEpoch = 0;
export class ApiError extends Error {
  constructor(public status: number, public code: string) { super(code); }
}
export async function api<T>(path: string, body?: unknown): Promise<T> {
  const epoch = sessionEpoch;
  const response = await fetch('/api/' + path, {
    method: body === undefined ? 'GET' : 'POST', credentials: 'same-origin',
    headers: body === undefined ? {} : { 'Content-Type': 'application/json', 'X-Raft-CSRF': csrf },
    body: body === undefined ? undefined : JSON.stringify(body),
  });
  if (!response.ok) {
    const error = await response.json().catch(() => ({}));
    throw new ApiError(response.status, error.code ?? (response.status === 403 ? 'reauth' : 'http_' + response.status));
  }
  const value = response.status === 204 ? undefined as T : await response.text().then(text => text ? JSON.parse(text) as T : undefined as T);
  if (epoch !== sessionEpoch) throw new ApiError(401, 'session_changed');
  return value;
}
export async function session(): Promise<Session> {
  const state = await api<Session>('session'); csrf = state.csrf;
  if (principal !== state.username) { principal = state.username; sessionEpoch++; }
  return state;
}
export function operationId(): string {
  if (crypto.randomUUID) return crypto.randomUUID();
  const bytes = crypto.getRandomValues(new Uint8Array(16));
  bytes[6] = (bytes[6] & 15) | 64; bytes[8] = (bytes[8] & 63) | 128;
  const hex = Array.from(bytes, byte => byte.toString(16).padStart(2, '0')).join('');
  return [hex.slice(0, 8), hex.slice(8, 12), hex.slice(12, 16), hex.slice(16, 20), hex.slice(20)].join('-');
}
