function getApiBase(): string {
  if (process.env.NEXT_PUBLIC_API_BASE) return process.env.NEXT_PUBLIC_API_BASE;
  if (typeof window !== "undefined")
    return `${window.location.protocol}//${window.location.hostname}:8000`;
  return "http://127.0.0.1:8000";
}
export const API_BASE = getApiBase();

export async function apiGet<T>(path: string): Promise<T> {
  const res = await fetch(`${API_BASE}${path}`, { cache: "no-store" });
  if (!res.ok) {
    const text = await res.text().catch(() => "");
    throw new Error(`GET ${path} failed: ${res.status} ${text}`);
  }
  return (await res.json()) as T;
}

export async function apiDelete<T = unknown>(path: string): Promise<T> {
  const res = await fetch(`${API_BASE}${path}`, { method: "DELETE" });
  if (!res.ok) {
    const text = await res.text().catch(() => "");
    throw new Error(`DELETE ${path} failed: ${res.status} ${text}`);
  }
  try {
    return (await res.json()) as T;
  } catch {
    return {} as T;
  }
}

