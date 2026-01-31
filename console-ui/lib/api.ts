export function getBaseUrl() {
  if (typeof window === 'undefined') return process.env.NEXT_PUBLIC_OBSERVER_URL || 'http://127.0.0.1:3011';
  return localStorage.getItem('observer_url') || process.env.NEXT_PUBLIC_OBSERVER_URL || 'http://127.0.0.1:3011';
}

export function getToken() {
  if (typeof window === 'undefined') return '';
  return localStorage.getItem('observer_token') || '';
}

export async function apiFetch(path: string, options: RequestInit = {}) {
  const base = getBaseUrl();
  const token = getToken();
  const headers = new Headers(options.headers || {});
  if (token) headers.set('Authorization', `Bearer ${token}`);
  if (!headers.has('Content-Type') && options.method && options.method !== 'GET') {
    headers.set('Content-Type', 'application/json');
  }
  const res = await fetch(`${base}${path}`, { ...options, headers });
  if (!res.ok) {
    const text = await res.text();
    throw new Error(text || res.statusText);
  }
  return res.json();
}
