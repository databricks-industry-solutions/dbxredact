import { useState, useEffect, useRef } from "react";
import { useGet, apiPost } from "../hooks/useApi";
import type { Config, RunStatus, JobHistoryItem } from "../types";

interface TableInfo {
  columns: string[];
  row_count: number;
}

const TERMINAL_STATES = ["TERMINATED", "SKIPPED", "INTERNAL_ERROR"];

export default function RunPage() {
  const { data: configs, loading: configsLoading } = useGet<Config[]>("/config/");
  const { data: history, refetch: refetchHistory } = useGet<JobHistoryItem[]>("/pipeline/history");
  const [configId, setConfigId] = useState("");
  const [sourceTable, setSourceTable] = useState("");
  const [outputTable, setOutputTable] = useState("");
  const [textCol, setTextCol] = useState("text");
  const [docIdCol, setDocIdCol] = useState("doc_id");
  const [maxRows, setMaxRows] = useState(10000);
  const [runStatus, setRunStatus] = useState<RunStatus | null>(null);
  const [running, setRunning] = useState(false);

  const [tableInfo, setTableInfo] = useState<TableInfo | null>(null);
  const [loadingTable, setLoadingTable] = useState(false);
  const [tableError, setTableError] = useState("");
  const pollRef = useRef<ReturnType<typeof setInterval> | null>(null);

  useEffect(() => {
    if (!configId && configs?.length) setConfigId(configs[0].config_id);
  }, [configs, configId]);

  useEffect(() => {
    if (runStatus && runStatus.state && !TERMINAL_STATES.includes(runStatus.state)) {
      pollRef.current = setInterval(async () => {
        try {
          const res = await fetch(`/api/pipeline/status/${runStatus.run_id}`);
          if (res.ok) {
            const updated: RunStatus = await res.json();
            setRunStatus(updated);
            if (updated.state && TERMINAL_STATES.includes(updated.state)) {
              clearInterval(pollRef.current!);
              pollRef.current = null;
              refetchHistory();
            }
          }
        } catch { /* ignore transient */ }
      }, 5000);
    }
    return () => { if (pollRef.current) clearInterval(pollRef.current); };
  }, [runStatus?.run_id, runStatus?.state]);

  async function fetchTableInfo() {
    if (!sourceTable.includes(".")) return;
    setLoadingTable(true);
    setTableError("");
    setTableInfo(null);
    try {
      const res = await fetch(`/api/pipeline/table-info?table=${encodeURIComponent(sourceTable)}`);
      if (res.ok) {
        const info: TableInfo = await res.json();
        setTableInfo(info);
        if (info.columns.includes("text")) setTextCol("text");
        else if (info.columns.length) setTextCol(info.columns[0]);
        if (info.columns.includes("doc_id")) setDocIdCol("doc_id");
        else if (info.columns.length) setDocIdCol(info.columns[0]);
      } else {
        const body = await res.json().catch(() => ({ error: "Table not found" }));
        setTableError(body.error || `Error ${res.status}`);
      }
    } catch {
      setTableError("Failed to connect");
    }
    setLoadingTable(false);
  }

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
      });
      setRunStatus(status);
      refetchHistory();
    } catch (e: any) {
      alert(e.message || "Failed to launch pipeline");
    }
    setRunning(false);
  }

  const columnOptions = tableInfo?.columns || [];
  const isRunning = runStatus?.state && !TERMINAL_STATES.includes(runStatus.state);

  return (
    <div>
      <h2 className="page-title">Run Pipeline</h2>
      <p className="page-desc">Execute the PII redaction pipeline on a Unity Catalog table.</p>

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
          <label className="block text-sm font-medium mb-1.5">Source Table</label>
          <div className="flex gap-2">
            <input className="input-field flex-1" value={sourceTable}
              onChange={(e) => setSourceTable(e.target.value)} placeholder="catalog.schema.table"
              onBlur={fetchTableInfo} />
            <button className="btn-ghost border border-gray-200 dark:border-gray-600"
              onClick={fetchTableInfo} disabled={loadingTable}>
              {loadingTable ? "..." : "Load"}
            </button>
          </div>
        </div>
        {tableError && (
          <div className="col-span-2 text-red-600 dark:text-red-400 text-sm">{tableError}</div>
        )}
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
        <div className="col-span-2 pt-2">
          <button className="btn-success" disabled={running || !configId || !sourceTable} onClick={launch}>
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

      <h3 className="text-lg font-semibold mb-3">Recent Runs</h3>
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
