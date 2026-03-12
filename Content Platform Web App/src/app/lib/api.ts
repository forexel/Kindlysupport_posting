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

async function readFileAsDataUrl(file: File): Promise<string> {
  return new Promise((resolve, reject) => {
    const reader = new FileReader();
    reader.onload = () => resolve(String(reader.result || ''));
    reader.onerror = reject;
    reader.readAsDataURL(file);
  });
}

export async function toDataUrl(file: File, opts?: { maxSide?: number; quality?: number }): Promise<string> {
  const maxSide = Math.max(640, Math.min(2400, Number(opts?.maxSide || 1600)));
  const quality = Math.max(0.5, Math.min(0.95, Number(opts?.quality || 0.82)));
  if (!file.type.startsWith('image/')) return readFileAsDataUrl(file);
  try {
    const dataUrl = await readFileAsDataUrl(file);
    const img = await new Promise<HTMLImageElement>((resolve, reject) => {
      const i = new Image();
      i.onload = () => resolve(i);
      i.onerror = reject;
      i.src = dataUrl;
    });
    const w = img.naturalWidth || img.width;
    const h = img.naturalHeight || img.height;
    if (!w || !h) return dataUrl;
    const scale = Math.min(1, maxSide / Math.max(w, h));
    const tw = Math.max(1, Math.round(w * scale));
    const th = Math.max(1, Math.round(h * scale));
    const canvas = document.createElement('canvas');
    canvas.width = tw;
    canvas.height = th;
    const ctx = canvas.getContext('2d');
    if (!ctx) return dataUrl;
    ctx.drawImage(img, 0, 0, tw, th);
    const out = canvas.toDataURL('image/jpeg', quality);
    return out || dataUrl;
  } catch {
    return readFileAsDataUrl(file);
  }
}

export function mskIso(date: string, time: string): string {
  // Build ISO in GMT+3
  const d = (date || '').trim();
  const t = (time || '').trim();
  if (!d || !t) return '';
  return `${d}T${t}:00+03:00`;
}
