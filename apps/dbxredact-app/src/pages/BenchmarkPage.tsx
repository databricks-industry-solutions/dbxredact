import { useState, useEffect, useRef } from "react";
import { Link } from "react-router-dom";
import { useGet, apiPost } from "../hooks/useApi";
import TablePicker, { type TableRef, emptyTableRef, toQualified, isComplete } from "../components/TablePicker";
import ErrorBanner from "../components/ErrorBanner";
import DataTable, { type Column } from "../components/DataTable";
import { SkeletonRows } from "../components/Skeleton";
import { useToast } from "../hooks/useToast";
import type { Config, RunStatus, JobHistoryItem } from "../types";
import { TERMINAL_STATES } from "../constants";

export default function BenchmarkPage() {
  const [sourceTable, setSourceTable] = useState<TableRef>(emptyTableRef);
  const [configId, setConfigId] = useState("");
  const [runStatus, setRunStatus] = useState<RunStatus | null>(null);
  const [running, setRunning] = useState(false);
  const [error, setError] = useState("");
  const failCountRef = useRef(0);
  const { toast } = useToast();
  const { data: configs, loading: loadingConfigs, error: configsError } = useGet<Config[]>("/config/");
  const { data: history, loading: loadingHistory, refetch: refetchHistory, error: historyError } = useGet<JobHistoryItem[]>("/benchmark/history");

  const displayError = error || configsError || historyError || "";

  useEffect(() => {
    if (configsError) setError(configsError);
  }, [configsError]);

  useEffect(() => {
    if (configs?.length && !configId) setConfigId(configs[0].config_id);
  }, [configs]);

  const pollCountRef = useRef(0);

  useEffect(() => {
    if (!runStatus?.state || TERMINAL_STATES.includes(runStatus.state)) return;
    failCountRef.current = 0;
    pollCountRef.current = 0;
    let timeoutId: ReturnType<typeof setTimeout> | null = null;

    async function poll() {
      try {
        const res = await fetch(`/api/benchmark/status/${runStatus!.run_id}`);
        if (res.ok) {
          failCountRef.current = 0;
          const updated: RunStatus = await res.json();
          setRunStatus(updated);
          if (updated.state && TERMINAL_STATES.includes(updated.state)) {
            refetchHistory();
            return;
          }
        } else {
          failCountRef.current++;
          if (failCountRef.current >= 5) {
            setRunStatus((prev) => prev ? { ...prev, state: "TERMINATED", result_state: "UNKNOWN" } : prev);
            refetchHistory();
            return;
          }
        }
      } catch {
        failCountRef.current++;
        if (failCountRef.current >= 5) {
          setRunStatus((prev) => prev ? { ...prev, state: "TERMINATED", result_state: "UNKNOWN" } : prev);
          refetchHistory();
          return;
        }
      }
      pollCountRef.current++;
      const interval = Math.min(3000 * Math.pow(1.5, Math.min(pollCountRef.current, 10)), 30000);
      timeoutId = setTimeout(poll, interval);
    }

    timeoutId = setTimeout(poll, 3000);
    return () => { if (timeoutId) clearTimeout(timeoutId); };
  }, [runStatus?.run_id, runStatus?.state]);

  const qualified = toQualified(sourceTable);
  const hasTable = isComplete(sourceTable);

  async function launch() {
    setRunning(true);
    try {
      const status = await apiPost<RunStatus>("/benchmark/run", {
        source_table: qualified || undefined,
        config_id: configId || undefined,
      });
      setRunStatus(status);
      toast("Benchmark launched");
      refetchHistory();
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : "Failed to launch benchmark");
    }
    setRunning(false);
  }

  const isActive = runStatus?.state && !TERMINAL_STATES.includes(runStatus.state);
  const selectedConfig = configs?.find((c) => c.config_id === configId);

  const historyColumns: Column<JobHistoryItem & Record<string, unknown>>[] = [
    { key: "run_id", header: "Run ID", render: (h) => <span className="font-mono text-xs">{h.run_id}</span> },
    { key: "config_id", header: "Config", render: (h) => <span className="font-mono text-xs">{String(h.config_id || "").slice(0, 8)}</span> },
    { key: "source_table", header: "Source" },
    { key: "status", header: "Status", render: (h) => (
      <span className={`inline-block px-2 py-0.5 text-xs rounded-full font-medium ${
        h.status === "RUNNING" ? "bg-amber-100 dark:bg-amber-900/30 text-amber-700 dark:text-amber-300"
        : h.status === "SUCCESS" ? "bg-emerald-100 dark:bg-emerald-900/30 text-emerald-700 dark:text-emerald-300"
        : "bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-300"
      }`}>{h.status}</span>
    )},
    { key: "started_at", header: "Started", render: (h) => <span className="text-gray-500 dark:text-gray-400">{h.started_at}</span> },
    { key: "run_page_url", header: "", sortable: false, searchable: false, render: (h) =>
      h.run_page_url ? (
        <a href={h.run_page_url as string} target="_blank" rel="noreferrer"
          className="text-blue-600 dark:text-blue-400 underline text-xs">View</a>
      ) : null
    },
  ];

  return (
    <div>
      <ErrorBanner message={displayError} onDismiss={() => setError("")} />
      <h2 className="text-xl font-semibold mb-1">Benchmark</h2>
      <p className="text-sm text-gray-500 dark:text-gray-400 mb-6">
        Run the full benchmark pipeline (detection, evaluation, redaction) against a labeled dataset.
        Results populate the Review and Metrics pages.
      </p>

      <div className="card p-5 mb-6 space-y-4 max-w-2xl">
        <TablePicker value={sourceTable} onChange={setSourceTable} label="Source Table (labeled benchmark data)" />

        <div>
          <label className="block text-sm font-medium mb-1">Configuration</label>
          {loadingConfigs ? (
            <SkeletonRows rows={2} />
          ) : !configs?.length ? (
            <p className="text-xs text-gray-400">No configs found. Create one on the Config page first.</p>
          ) : (
            <>
              <select className="input-field" value={configId} onChange={(e) => setConfigId(e.target.value)}>
                {configs.map((c) => (
                  <option key={c.config_id} value={c.config_id}>{c.name}</option>
                ))}
              </select>
              {selectedConfig && (
                <div className="mt-2 text-xs text-gray-500 dark:text-gray-400 grid grid-cols-2 gap-x-4 gap-y-1">
                  <span>Endpoint: <b>{selectedConfig.endpoint || "default"}</b></span>
                  <span>Alignment: <b>{selectedConfig.alignment_mode}</b></span>
                  <span>Presidio: <b>{selectedConfig.use_presidio ? "on" : "off"}</b></span>
                  <span>AI Query: <b>{selectedConfig.use_ai_query ? "on" : "off"}</b></span>
                  <span>GliNER: <b>{selectedConfig.use_gliner ? "on" : "off"}</b></span>
                  <span>Threshold: <b>{selectedConfig.score_threshold}</b></span>
                </div>
              )}
            </>
          )}
        </div>

        {hasTable && (
          <p className="text-xs text-gray-500 dark:text-gray-400">
            Detection results will be written to <code className="font-mono text-xs">{qualified}_detection_results</code>
          </p>
        )}

        <button className="btn-primary" disabled={running || !!isActive || !hasTable} onClick={launch}>
          {running ? "Launching..." : isActive ? "Running..." : "Run Benchmark"}
        </button>
      </div>

      {runStatus && (
        <div className={`status-banner mb-6 ${isActive ? "status-running"
          : runStatus.result_state === "SUCCESS" ? "status-success" : "status-error"}`}>
          <div className="flex items-center gap-2">
            {isActive && <span className="inline-block w-2 h-2 rounded-full bg-yellow-500 animate-pulse" />}
            <span>Run #{runStatus.run_id} -- <b>{runStatus.state}</b></span>
            {runStatus.result_state && <span className="opacity-70">({runStatus.result_state})</span>}
          </div>
          <div className="flex items-center gap-3 mt-1">
            {runStatus.run_page_url && (
              <a href={runStatus.run_page_url} target="_blank" rel="noreferrer"
                className="text-blue-600 dark:text-blue-400 underline text-xs">
                View in Databricks
              </a>
            )}
            {!isActive && (runStatus.result_state === "SUCCESS" || runStatus.state === "TERMINATED") && (
              <Link to="/metrics" className="text-xs px-3 py-1.5 rounded-lg bg-blue-600 text-white hover:bg-blue-700 transition-colors font-medium">
                View Metrics
              </Link>
            )}
            {isActive && (
              <button className="text-xs text-red-600 dark:text-red-400 underline"
                onClick={async () => {
                  try {
                    await apiPost(`/benchmark/cancel/${runStatus.run_id}`, {});
                    setRunStatus((prev) => prev ? { ...prev, state: "CANCELLED", result_state: "CANCELLED" } : prev);
                    toast("Benchmark cancelled");
                    refetchHistory();
                  } catch (e: unknown) {
                    setError(e instanceof Error ? e.message : "Failed to cancel");
                  }
                }}>Cancel</button>
            )}
          </div>
        </div>
      )}

      <h3 className="text-lg font-semibold mb-3 flex items-center gap-2">
        Recent Benchmark Runs
        <button type="button" onClick={() => refetchHistory()} className="btn-secondary text-sm">Refresh</button>
      </h3>
      {loadingHistory && <SkeletonRows rows={3} className="mb-4" />}
      <DataTable<JobHistoryItem & Record<string, unknown>>
        data={(history ?? []) as (JobHistoryItem & Record<string, unknown>)[]}
        rowKey={(h) => String(h.run_id)}
        emptyMessage="No benchmark runs yet."
        columns={historyColumns}
      />
    </div>
  );
}
