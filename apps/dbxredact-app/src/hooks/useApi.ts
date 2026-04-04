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
  /** Number of retries with exponential backoff (default 0). */
  retries?: number;
}

function delay(ms: number, signal: AbortSignal): Promise<void> {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(resolve, ms);
    signal.addEventListener("abort", () => { clearTimeout(timer); reject(new DOMException("Aborted", "AbortError")); }, { once: true });
  });
}

async function fetchWithRetry<T>(path: string, signal: AbortSignal, retries: number): Promise<T> {
  let lastError: Error | undefined;
  for (let attempt = 0; attempt <= retries; attempt++) {
    try {
      return await apiFetch<T>(path, { signal });
    } catch (e: any) {
      if (signal.aborted) throw e;
      lastError = e;
      if (attempt < retries) await delay(1000 * 2 ** attempt, signal);
    }
  }
  throw lastError;
}

export function useGet<T>(path: string, opts?: UseGetOptions) {
  const { deps = [], enabled = true, retries = 0 } = opts ?? {};
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
    setError(null);
    fetchWithRetry<T>(path, controller.signal, retries)
      .then((d) => { if (!controller.signal.aborted) setData(d); })
      .catch((e) => { if (!controller.signal.aborted) setError(e.message); })
      .finally(() => { if (!controller.signal.aborted) setLoading(false); });
  }, [path, enabled, retries, ...deps]);

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
