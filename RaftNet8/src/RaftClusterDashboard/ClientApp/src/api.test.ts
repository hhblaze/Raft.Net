import { afterEach, describe, expect, it, vi } from 'vitest';
import { api, ApiError, session } from './api';
afterEach(() => vi.unstubAllGlobals());
describe('browser backend boundary', () => {
  it('uses the current anti-forgery token for mutations and does not send it on reads', async () => {
    const fetch = vi.fn()
      .mockResolvedValueOnce(new Response(JSON.stringify({ csrf: 'csrf-test', username: 'admin' })))
      .mockResolvedValueOnce(new Response('{}'))
      .mockResolvedValueOnce(new Response('{}'));
    vi.stubGlobal('fetch', fetch);
    await session();
    await api('admin/clusters', { name: 'test' });
    await api('admin/observations');
    const mutation = fetch.mock.calls[1][1] as RequestInit;
    expect(mutation.method).toBe('POST');
    expect(mutation.credentials).toBe('same-origin');
    expect(mutation.headers).toMatchObject({ 'X-Raft-CSRF': 'csrf-test' });
    expect(fetch.mock.calls[2][1].headers).toEqual({});
  });
  it('keeps authentication failure structured even if an upstream error body is not JSON', async () => {
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(new Response('<html>upstream error</html>', { status: 401 })));
    const failure = await api('admin/clusters').catch(error => error);
    expect(failure).toBeInstanceOf(ApiError);
    if (!(failure instanceof ApiError)) throw new Error('Expected structured API failure.');
    expect(failure.status).toBe(401);
    expect(failure.code).toBe('http_401');
  });
  it('discards a late secret-bearing response after the signed-in account changes', async () => {
    let complete: (response: Response) => void = () => {};
    const late = new Promise<Response>(resolve => { complete = resolve; });
    vi.stubGlobal('fetch', vi.fn()
      .mockResolvedValueOnce(new Response(JSON.stringify({ csrf: 'a', username: 'admin' })))
      .mockReturnValueOnce(late)
      .mockResolvedValueOnce(new Response(JSON.stringify({ csrf: 'b', username: 'viewer' }))));
    await session();
    const result = api('admin/tokens', {}).catch(error => error);
    await session();
    complete(new Response(JSON.stringify({ secret: 'test-only-never-deliver-to-viewer' })));
    expect(await result).toMatchObject({ status: 401, code: 'session_changed' });
  });
});
