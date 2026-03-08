export type ApiMethod = 'GET' | 'POST' | 'PUT' | 'DELETE';

function getCookie(name: string): string {
  const m = document.cookie.match(new RegExp(`(?:^|; )${name}=([^;]*)`));
  return m ? decodeURIComponent(m[1]) : '';
}

export async function api<T = any>(path: string, method: ApiMethod = 'GET', body?: unknown): Promise<T> {
  const headers: Record<string, string> = {};
  const csrf = getCookie('csrf_token');
  if (method !== 'GET' && method !== 'DELETE') {
    headers['Content-Type'] = 'application/json';
    if (csrf) headers['X-CSRF-Token'] = csrf;
  } else if ((method === 'DELETE' || method === 'PUT' || method === 'POST') && csrf) {
    headers['X-CSRF-Token'] = csrf;
  }

  const res = await fetch(path, {
    method,
    headers,
    credentials: 'include',
    body: body !== undefined ? JSON.stringify(body) : undefined,
  });

  const text = await res.text();
  let data: any = {};
  try {
    data = text ? JSON.parse(text) : {};
  } catch {
    data = { detail: text };
  }

  if (!res.ok) {
    const detail = data?.detail || `HTTP ${res.status}`;
    throw new Error(typeof detail === 'string' ? detail : JSON.stringify(detail));
  }
  return data as T;
}

export function toDataUrl(file: File): Promise<string> {
  return new Promise((resolve, reject) => {
    const reader = new FileReader();
    reader.onload = () => resolve(String(reader.result || ''));
    reader.onerror = reject;
    reader.readAsDataURL(file);
  });
}

export function mskIso(date: string, time: string): string {
  // Build ISO in GMT+3
  const d = (date || '').trim();
  const t = (time || '').trim();
  if (!d || !t) return '';
  return `${d}T${t}:00+03:00`;
}
