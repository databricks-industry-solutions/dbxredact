import { useState, useEffect, useRef } from "react";
import { useGet, apiPost } from "../hooks/useApi";
import TablePicker from "../components/TablePicker";
import ErrorBanner from "../components/ErrorBanner";
import type { Config, RunStatus, JobHistoryItem } from "../types";

interface TableInfo {
  columns: string[];
  row_count: number;
}

interface CostEstimate {
  row_count: number;
  total_chars: number;
  estimated_input_tokens: number;
  estimated_output_tokens: number;
  ai_query_cost_usd: number;
  compute_cost_usd: number;
  estimated_cost_usd: number;
  estimated_minutes: number;
  endpoint: string;
  cluster_profile: string;
  use_ai_query: boolean;
}

const TERMINAL_STATES = ["TERMINATED", "SKIPPED", "INTERNAL_ERROR"];

export default function RunPage() {
  const { data: configs, loading: configsLoading, error: configsError } = useGet<Config[]>("/config/");
  const { data: history, refetch: refetchHistory, error: historyError } = useGet<JobHistoryItem[]>("/pipeline/history");
  const [configId, setConfigId] = useState("");
  const [sourceTable, setSourceTable] = useState("");
  const [outputTable, setOutputTable] = useState("");
  const [textCol, setTextCol] = useState("text");
  const [docIdCol, setDocIdCol] = useState("doc_id");
  const [maxRows, setMaxRows] = useState(10000);
  const [clusterSize, setClusterSize] = useState<"small" | "medium" | "large">("small");
  const [useGpu, setUseGpu] = useState(false);
  const [refreshApproach, setRefreshApproach] = useState<"full" | "incremental">("full");
  const [runStatus, setRunStatus] = useState<RunStatus | null>(null);
  const [running, setRunning] = useState(false);
  const [error, setError] = useState("");

  const displayError = error || configsError || historyError || "";

  const [tableInfo, setTableInfo] = useState<TableInfo | null>(null);
  const [loadingTable, setLoadingTable] = useState(false);
  const [costEstimate, setCostEstimate] = useState<CostEstimate | null>(null);
  const [loadingCost, setLoadingCost] = useState(false);
  const pollRef = useRef<ReturnType<typeof setInterval> | null>(null);
  const failCountRef = useRef(0);

  const parts = sourceTable.split(".");
  const hasTable = parts.length === 3 && parts[2] !== "";

  useEffect(() => {
    if (!configId && configs?.length) setConfigId(configs[0].config_id);
  }, [configs, configId]);

  useEffect(() => {
    if (!hasTable) { setTableInfo(null); return; }
    setLoadingTable(true);
    fetch(`/api/pipeline/table-info?table=${encodeURIComponent(sourceTable)}`)
      .then((r) => r.json())
      .then((info: TableInfo) => {
        setTableInfo(info);
        if (info.columns.includes("text")) setTextCol("text");
        else if (info.columns.length) setTextCol(info.columns[0]);
        if (info.columns.includes("doc_id")) setDocIdCol("doc_id");
        else if (info.columns.length) setDocIdCol(info.columns[0]);
      })
      .catch((e: any) => { setError(e.message || "Failed to load table info"); setTableInfo(null); })
      .finally(() => setLoadingTable(false));
  }, [sourceTable, hasTable]);

  const selectedConfig = configs?.find((c) => c.config_id === configId);
  const usesAiQuery = selectedConfig?.use_ai_query ?? false;
  const usesGliner = selectedConfig?.use_gliner ?? false;
  const clusterProfile = `${useGpu ? "gpu" : "cpu"}_${clusterSize}`;

  const showCostPanel = usesAiQuery || usesGliner;

  useEffect(() => {
    if (!hasTable || !showCostPanel || !selectedConfig) { setCostEstimate(null); return; }
    setLoadingCost(true);
    const params = new URLSearchParams({
      table: sourceTable,
      text_column: textCol,
      endpoint: selectedConfig.endpoint || "databricks-gpt-oss-120b",
      max_rows: String(maxRows),
      cluster_profile: clusterProfile,
      use_gliner: String(usesGliner),
      use_ai_query: String(usesAiQuery),
      detection_profile: selectedConfig.detection_profile || "fast",
    });
    fetch(`/api/pipeline/cost-estimate?${params}`)
      .then((r) => r.json())
      .then(setCostEstimate)
      .catch((e: any) => { setError(e.message || "Failed to load cost estimate"); setCostEstimate(null); })
      .finally(() => setLoadingCost(false));
  }, [sourceTable, hasTable, showCostPanel, usesAiQuery, selectedConfig?.endpoint, textCol, maxRows, clusterProfile, usesGliner, selectedConfig?.detection_profile]);

  useEffect(() => {
    if (runStatus && runStatus.state && !TERMINAL_STATES.includes(runStatus.state)) {
      failCountRef.current = 0;
      pollRef.current = setInterval(async () => {
        try {
          const res = await fetch(`/api/pipeline/status/${runStatus.run_id}`);
          if (res.ok) {
            failCountRef.current = 0;
            const updated: RunStatus = await res.json();
            setRunStatus(updated);
            if (updated.state && TERMINAL_STATES.includes(updated.state)) {
              clearInterval(pollRef.current!);
              pollRef.current = null;
              refetchHistory();
            }
          } else {
            failCountRef.current++;
            if (failCountRef.current >= 5) {
              clearInterval(pollRef.current!);
              pollRef.current = null;
              setRunStatus((prev) => prev ? { ...prev, state: "TERMINATED", result_state: "UNKNOWN" } : prev);
              refetchHistory();
            }
          }
        } catch {
          failCountRef.current++;
          if (failCountRef.current >= 5) {
            clearInterval(pollRef.current!);
            pollRef.current = null;
            setRunStatus((prev) => prev ? { ...prev, state: "TERMINATED", result_state: "UNKNOWN" } : prev);
            refetchHistory();
          }
        }
      }, 5000);
    }
    return () => { if (pollRef.current) clearInterval(pollRef.current); };
  }, [runStatus?.run_id, runStatus?.state]);

  async function launch() {
    setRunning(true);
    try {
      const status = await apiPost<RunStatus>("/pipeline/run", {
        config_id: configId,
        source_table: sourceTable,
        output_table: outputTable || undefined,
        text_column: textCol,
        doc_id_column: docIdCol,
        max_rows: maxRows,
        cluster_profile: clusterProfile,
        refresh_approach: refreshApproach,
      });
      setRunStatus(status);
      refetchHistory();
    } catch (e: any) {
      setError(e.message || "Failed to launch pipeline");
    }
    setRunning(false);
  }

  const columnOptions = tableInfo?.columns || [];
  const isRunning = runStatus?.state && !TERMINAL_STATES.includes(runStatus.state);

  return (
    <div>
      <h2 className="page-title">Run Pipeline</h2>
      <p className="page-desc">Execute the PII redaction pipeline on a Unity Catalog table.</p>
      <ErrorBanner message={displayError} onDismiss={() => setError("")} />

      <div className="card p-5 mb-6 grid grid-cols-2 gap-4 max-w-2xl">
        <div className="col-span-2">
          <label className="block text-sm font-medium mb-1.5">Config</label>
          <select className="input-field" value={configId}
            onChange={(e) => setConfigId(e.target.value)}>
            {configsLoading && <option value="">Loading configs...</option>}
            {!configsLoading && !configs?.length && <option value="">No configs found</option>}
            {configs?.map((c) => <option key={c.config_id} value={c.config_id}>{c.name}</option>)}
          </select>
        </div>
        <div className="col-span-2">
          <TablePicker value={sourceTable} onChange={setSourceTable} label="Source Table" />
          {loadingTable && <p className="text-xs text-gray-400 mt-1 animate-pulse">Loading table info...</p>}
          {tableInfo && (
            <p className="text-xs text-gray-500 dark:text-gray-400 mt-1">
              {tableInfo.row_count.toLocaleString()} rows, {tableInfo.columns.length} columns
            </p>
          )}
        </div>
        <div className="col-span-2">
          <label className="block text-sm font-medium mb-1.5">
            Output Table <span className="text-gray-400 font-normal">(defaults to source_table_redacted)</span>
          </label>
          <input className="input-field" value={outputTable}
            onChange={(e) => setOutputTable(e.target.value)} placeholder="catalog.schema.output_table" />
        </div>
        <div>
          <label className="block text-sm font-medium mb-1.5">Text Column</label>
          {columnOptions.length ? (
            <select className="input-field" value={textCol}
              onChange={(e) => setTextCol(e.target.value)}>
              {columnOptions.map((c) => <option key={c} value={c}>{c}</option>)}
            </select>
          ) : (
            <input className="input-field" value={textCol}
              onChange={(e) => setTextCol(e.target.value)} />
          )}
        </div>
        <div>
          <label className="block text-sm font-medium mb-1.5">Doc ID Column</label>
          {columnOptions.length ? (
            <select className="input-field" value={docIdCol}
              onChange={(e) => setDocIdCol(e.target.value)}>
              {columnOptions.map((c) => <option key={c} value={c}>{c}</option>)}
            </select>
          ) : (
            <input className="input-field" value={docIdCol}
              onChange={(e) => setDocIdCol(e.target.value)} />
          )}
        </div>
        <div>
          <label className="block text-sm font-medium mb-1.5">
            Max Rows
            {tableInfo && <span className="text-gray-400 font-normal ml-1">({tableInfo.row_count.toLocaleString()} total)</span>}
          </label>
          <input type="number" className="input-field" value={maxRows}
            onChange={(e) => setMaxRows(parseInt(e.target.value))} />
        </div>
        <div>
          <label className="block text-sm font-medium mb-1.5">Cluster Size</label>
          <select className="input-field" value={clusterSize}
            onChange={(e) => setClusterSize(e.target.value as "small" | "medium" | "large")}>
            <option value="small">Small (2 workers)</option>
            <option value="medium">Medium (5 workers)</option>
            <option value="large">Large (10 workers)</option>
          </select>
        </div>
        <div className="col-span-2 flex items-center gap-3">
          <label className="relative inline-flex items-center cursor-pointer">
            <input type="checkbox" className="sr-only peer" checked={useGpu}
              onChange={(e) => setUseGpu(e.target.checked)} />
            <div className="w-9 h-5 bg-gray-200 peer-focus:outline-none rounded-full peer dark:bg-gray-600
              peer-checked:after:translate-x-full peer-checked:after:border-white after:content-['']
              after:absolute after:top-[2px] after:left-[2px] after:bg-white after:border-gray-300
              after:border after:rounded-full after:h-4 after:w-4 after:transition-all
              peer-checked:bg-blue-600" />
          </label>
          <span className="text-sm font-medium">GPU cluster</span>
          {usesGliner && !useGpu && (
            <span className="text-xs text-amber-600 dark:text-amber-400">GPU recommended when GLiNER is enabled</span>
          )}
        </div>
        <div>
          <label className="block text-sm font-medium mb-1.5">Refresh Mode</label>
          <select className="input-field" value={refreshApproach}
            onChange={(e) => setRefreshApproach(e.target.value as "full" | "incremental")}>
            <option value="full">Full (overwrite)</option>
            <option value="incremental">Incremental (append)</option>
          </select>
        </div>
        {refreshApproach === "full" && outputTable && (
          <div className="col-span-2 text-xs text-amber-600 dark:text-amber-400 bg-amber-50 dark:bg-amber-900/10 border border-amber-200 dark:border-amber-800 rounded-lg px-3 py-2">
            Full refresh will overwrite the existing output table if it exists.
          </div>
        )}
        {showCostPanel && hasTable && (
          <details className="col-span-2 border border-blue-200 dark:border-blue-800 rounded-lg bg-blue-50/50 dark:bg-blue-900/10">
            <summary
              className="text-xs font-semibold text-blue-700 dark:text-blue-300 cursor-pointer select-none p-3"
              title="Estimates are directional, based on observed benchmarks with ensemble detection. Actual costs vary with data shape, cluster load, and model endpoint."
            >
              Cost Estimate
              <span className="text-[10px] bg-amber-200 dark:bg-amber-800 text-amber-800 dark:text-amber-200 px-1.5 py-0.5 rounded-full ml-1.5 font-medium">Beta</span>
            </summary>
            <div className="px-3 pb-3">
              {loadingCost && <p className="text-xs text-gray-400 animate-pulse">Calculating...</p>}
              {costEstimate && !loadingCost && (
                <div className="grid grid-cols-3 gap-2 text-xs">
                  <div><span className="text-gray-500">Rows:</span> <span className="font-medium">{costEstimate.row_count.toLocaleString()}</span></div>
                  <div><span className="text-gray-500">Characters:</span> <span className="font-medium">{costEstimate.total_chars.toLocaleString()}</span></div>
                  <div><span className="text-gray-500">Est. runtime:</span> <span className="font-medium">{costEstimate.estimated_minutes} min</span></div>
                  {costEstimate.use_ai_query && <>
                    <div><span className="text-gray-500">Endpoint:</span> <span className="font-medium">{costEstimate.endpoint}</span></div>
                    <div><span className="text-gray-500">Input tokens:</span> <span className="font-medium">{costEstimate.estimated_input_tokens.toLocaleString()}</span></div>
                    <div><span className="text-gray-500">Output tokens:</span> <span className="font-medium">{costEstimate.estimated_output_tokens.toLocaleString()}</span></div>
                    <div><span className="text-gray-500">AI Query cost:</span> <span className="font-medium">${costEstimate.ai_query_cost_usd.toFixed(4)}</span></div>
                  </>}
                  <div><span className="text-gray-500">Compute cost:</span> <span className="font-medium">${costEstimate.compute_cost_usd.toFixed(4)}</span></div>
                  <div><span className="text-gray-500">Total est.:</span> <span className="font-bold text-blue-700 dark:text-blue-300">${costEstimate.estimated_cost_usd.toFixed(4)}</span></div>
                </div>
              )}
            </div>
          </details>
        )}

        <div className="col-span-2 pt-2">
          <button className="btn-success" disabled={running || !configId || !hasTable} onClick={launch}>
            {running ? "Launching..." : "Run Pipeline"}
          </button>
        </div>
      </div>

      {runStatus && (
        <div className={`status-banner mb-6 ${isRunning ? "status-running"
          : runStatus.result_state === "SUCCESS" ? "status-success" : "status-error"}`}>
          <div className="flex items-center gap-2">
            {isRunning && <span className="inline-block w-2 h-2 rounded-full bg-amber-500 animate-pulse" />}
            <span>Run #{runStatus.run_id} -- <b>{runStatus.state}</b></span>
            {runStatus.result_state && <span className="opacity-70">({runStatus.result_state})</span>}
          </div>
          {runStatus.run_page_url && (
            <a href={runStatus.run_page_url} target="_blank" rel="noreferrer"
              className="text-blue-600 dark:text-blue-400 underline text-xs mt-1 inline-block">
              View in Databricks
            </a>
          )}
        </div>
      )}

      <h3 className="text-lg font-semibold mb-3 flex items-center gap-2">
        Recent Runs
        <button type="button" onClick={() => refetchHistory()} className="btn-secondary text-sm">Refresh</button>
      </h3>
      {history?.length ? (
        <table className="data-table">
          <thead>
            <tr><th>Run ID</th><th>Config</th><th>Source</th><th>Status</th><th>Started</th></tr>
          </thead>
          <tbody>
            {history.map((h) => (
              <tr key={h.run_id}>
                <td className="font-mono text-xs">{h.run_id}</td>
                <td className="font-mono text-xs">{h.config_id.slice(0, 8)}</td>
                <td>{h.source_table}</td>
                <td>
                  <span className={`inline-block px-2 py-0.5 text-xs rounded-full font-medium ${
                    h.status === "RUNNING" ? "bg-amber-100 dark:bg-amber-900/30 text-amber-700 dark:text-amber-300"
                    : h.status === "SUCCESS" ? "bg-emerald-100 dark:bg-emerald-900/30 text-emerald-700 dark:text-emerald-300"
                    : "bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-300"
                  }`}>{h.status}</span>
                </td>
                <td className="text-gray-500 dark:text-gray-400">{h.started_at}</td>
              </tr>
            ))}
          </tbody>
        </table>
      ) : (
        <p className="text-sm text-gray-400">No runs yet.</p>
      )}
    </div>
  );
}
