import { useState, useEffect, useCallback, useRef } from "react";

const BASE = "/api";

async function apiFetch<T>(path: string, init?: RequestInit): Promise<T> {
  const res = await fetch(`${BASE}${path}`, {
    headers: { "Content-Type": "application/json", ...init?.headers },
    ...init,
  });
  if (!res.ok) {
    const body = await res.text();
    let message = `${res.status}: ${body}`;
    try {
      const json = JSON.parse(body);
      if (json.error) message = json.error;
    } catch { /* body wasn't JSON */ }
    throw new Error(message);
  }
  if (res.status === 204) return undefined as unknown as T;
  return res.json();
}

export interface UseGetOptions {
  deps?: unknown[];
  enabled?: boolean;
}

export function useGet<T>(path: string, opts?: UseGetOptions) {
  const { deps = [], enabled = true } = opts ?? {};
  const [data, setData] = useState<T | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const abortRef = useRef<AbortController | null>(null);

  const refetch = useCallback(() => {
    if (!enabled) {
      setData(null);
      setLoading(false);
      return;
    }
    abortRef.current?.abort();
    const controller = new AbortController();
    abortRef.current = controller;
    setLoading(true);
    apiFetch<T>(path, { signal: controller.signal })
      .then((d) => { if (!controller.signal.aborted) setData(d); })
      .catch((e) => { if (!controller.signal.aborted) setError(e.message); })
      .finally(() => { if (!controller.signal.aborted) setLoading(false); });
  }, [path, enabled, ...deps]);

  useEffect(() => {
    refetch();
    return () => { abortRef.current?.abort(); };
  }, [refetch]);

  return { data, loading, error, refetch };
}

export async function apiPost<T>(path: string, body: unknown): Promise<T> {
  return apiFetch<T>(path, { method: "POST", body: JSON.stringify(body) });
}

export async function apiPut<T>(path: string, body: unknown): Promise<T> {
  return apiFetch<T>(path, { method: "PUT", body: JSON.stringify(body) });
}

export async function apiDelete(path: string): Promise<void> {
  await apiFetch(path, { method: "DELETE" });
}

export { apiFetch };
