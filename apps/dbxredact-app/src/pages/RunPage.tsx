import { useState, useEffect, useRef } from "react";
import { Link } from "react-router-dom";
import { useGet, apiPost } from "../hooks/useApi";
import TablePicker, { type TableRef, emptyTableRef, toQualified, isComplete } from "../components/TablePicker";
import ErrorBanner from "../components/ErrorBanner";
import ConfirmDialog from "../components/ConfirmDialog";
import DataTable, { type Column } from "../components/DataTable";
import { SkeletonRows } from "../components/Skeleton";
import Badge, { statusToVariant } from "../components/Badge";
import Button from "../components/Button";
import { useToast } from "../hooks/useToast";
import type { Config, RunStatus, JobHistoryItem, DiscoverColumnsResponse, DiscoverAllColumnsResponse, MetagenPIRequest } from "../types";
import { TERMINAL_STATES } from "../constants";

/** Maps a Databricks life-cycle state to a human phase label for the running pipeline. */
function phaseLabel(state: string | undefined): string {
  switch ((state || "").toUpperCase()) {
    case "PENDING":
    case "QUEUED":
      return "Starting cluster";
    case "RUNNING":
      return "Detecting & redacting";
    case "TERMINATING":
      return "Writing output";
    default:
      return "In progress";
  }
}

/** Formats an elapsed millisecond span as "Xm Ys" (or "Ys" under a minute). */
function formatElapsed(ms: number): string {
  const totalSec = Math.max(0, Math.floor(ms / 1000));
  const m = Math.floor(totalSec / 60);
  const s = totalSec % 60;
  return m > 0 ? `${m}m ${s}s` : `${s}s`;
}

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

export default function RunPage() {
  const { data: configs, loading: configsLoading, error: configsError } = useGet<Config[]>("/config/");
  const { data: history, loading: loadingHistory, refetch: refetchHistory, error: historyError } = useGet<JobHistoryItem[]>("/pipeline/history");
  const [configId, setConfigId] = useState("");
  const [sourceTable, setSourceTable] = useState<TableRef>(emptyTableRef);
  const [outputTable, setOutputTable] = useState("");
  const [textCol, setTextCol] = useState("text");
  const [docIdCol, setDocIdCol] = useState("doc_id");
  const [maxRows, setMaxRows] = useState(10000);
  const [clusterSize, setClusterSize] = useState<"small" | "medium" | "large">("small");
  const [useGpu, setUseGpu] = useState(false);
  const [refreshApproach, setRefreshApproach] = useState<"full" | "incremental">("full");
  const [outputMode, setOutputMode] = useState<"separate" | "in_place">("separate");
  const [redactionScope, setRedactionScope] = useState<"single_column" | "tagged_columns" | "full_table" | "ingestion">("single_column");
  const [selectedTextCols, setSelectedTextCols] = useState<string[]>([]);
  const [selectedStructCols, setSelectedStructCols] = useState<Record<string, string>>({});
  const [maskingStrategy, setMaskingStrategy] = useState<"mask" | "hash" | "encrypt">("mask");
  const [outputStrategy, setOutputStrategy] = useState<"production" | "validation">("production");
  const [maxCostUsd, setMaxCostUsd] = useState<number | null>(null);

  // Ingestion state
  const [ingSourcePath, setIngSourcePath] = useState("");
  const [ingFileFormat, setIngFileFormat] = useState<"json" | "csv" | "parquet" | "text">("json");
  const [ingOutputTable, setIngOutputTable] = useState("");
  const [ingTextCols, setIngTextCols] = useState("");
  const [ingStructCols, setIngStructCols] = useState("");
  const [ingDocIdCol, setIngDocIdCol] = useState("doc_id");
  const [ingCheckpoint, setIngCheckpoint] = useState("");
  const [ingSchemaLoc, setIngSchemaLoc] = useState("");
  const [ingMaxFiles, setIngMaxFiles] = useState(50);
  const [ingTriggerTime, setIngTriggerTime] = useState("");
  const [ingMultiColStrategy, setIngMultiColStrategy] = useState<"sequential" | "concat">("sequential");

  // PI Discovery state
  const [piTableNames, setPiTableNames] = useState("");
  const [piCatalog, setPiCatalog] = useState("");
  const [piSchema, setPiSchema] = useState("");
  const [piOutputMode, setPiOutputMode] = useState<"tag" | "knowledge_base">("tag");
  const [piModel, setPiModel] = useState("databricks-meta-llama-3-3-70b-instruct");
  const [piDeterministic, setPiDeterministic] = useState(true);
  const [piRunning, setPiRunning] = useState(false);
  const [piStatus, setPiStatus] = useState<RunStatus | null>(null);
  const [runStatus, setRunStatus] = useState<RunStatus | null>(null);
  const [running, setRunning] = useState(false);
  const [confirmOpen, setConfirmOpen] = useState(false);
  const [error, setError] = useState("");
  const { toast } = useToast();

  const displayError = error || configsError || historyError || "";
  const hasTable = isComplete(sourceTable);
  const qualified = toQualified(sourceTable);

  const { data: tableInfo, loading: loadingTable } = useGet<TableInfo>(
    `/pipeline/table-info?table=${encodeURIComponent(qualified)}`,
    { enabled: hasTable, deps: [qualified] },
  );

  const isIngestion = redactionScope === "ingestion";
  const isMultiColumn = redactionScope === "full_table" || redactionScope === "tagged_columns";
  const isTaggedColumns = redactionScope === "tagged_columns";
  const isFullTable = redactionScope === "full_table";
  const { data: discovered, loading: loadingDiscovery } = useGet<DiscoverColumnsResponse>(
    `/pipeline/discover-columns?table=${encodeURIComponent(qualified)}`,
    { enabled: hasTable && isTaggedColumns, deps: [qualified, isTaggedColumns] },
  );
  const { data: allColumns, loading: loadingAllColumns } = useGet<DiscoverAllColumnsResponse>(
    `/pipeline/discover-all-columns?table=${encodeURIComponent(qualified)}`,
    { enabled: hasTable && isFullTable, deps: [qualified, isFullTable] },
  );

  useEffect(() => {
    if (!discovered) return;
    setSelectedTextCols(discovered.text_columns);
    setSelectedStructCols({ ...discovered.structured_columns });
    if (discovered.doc_id_candidates.length) setDocIdCol(discovered.doc_id_candidates[0]);
  }, [discovered]);

  useEffect(() => {
    if (!allColumns) return;
    setSelectedTextCols(allColumns.text_columns);
    setSelectedStructCols(
      Object.fromEntries(allColumns.non_text_columns.map((c) => [c, "unknown"]))
    );
    if (allColumns.doc_id_candidates.length) setDocIdCol(allColumns.doc_id_candidates[0]);
  }, [allColumns]);

  const failCountRef = useRef(0);

  useEffect(() => {
    if (!configId && configs?.length) setConfigId(configs[0].config_id);
  }, [configs, configId]);

  useEffect(() => {
    if (!tableInfo) return;
    if (tableInfo.columns.includes("text")) setTextCol("text");
    else if (tableInfo.columns.length) setTextCol(tableInfo.columns[0]);
    if (tableInfo.columns.includes("doc_id")) setDocIdCol("doc_id");
    else if (tableInfo.columns.length) setDocIdCol(tableInfo.columns[0]);
  }, [tableInfo]);

  const selectedConfig = configs?.find((c) => c.config_id === configId);
  const usesAiQuery = selectedConfig?.use_ai_query ?? false;
  const nerBackend = selectedConfig?.ner_backend || (selectedConfig?.use_gliner ? "gliner" : "none");
  const usesLocalNer = nerBackend !== "none";

  useEffect(() => {
    if (usesLocalNer) setUseGpu(true);
  }, [usesLocalNer]);
  const clusterProfile = `${useGpu ? "gpu" : "cpu"}_${clusterSize}`;
  const showCostPanel = usesAiQuery || usesLocalNer;

  const costParams = new URLSearchParams({
    table: qualified,
    text_column: textCol,
    endpoint: selectedConfig?.endpoint || "databricks-gpt-oss-120b",
    max_rows: String(maxRows),
    cluster_profile: clusterProfile,
    use_gliner: String(usesLocalNer),
    use_ai_query: String(usesAiQuery),
    detection_profile: selectedConfig?.detection_profile || "fast",
  }).toString();

  const { data: costEstimate, loading: loadingCost } = useGet<CostEstimate>(
    `/pipeline/cost-estimate?${costParams}`,
    {
      enabled: hasTable && showCostPanel && !!selectedConfig,
      deps: [qualified, textCol, selectedConfig?.endpoint, maxRows, clusterProfile, usesLocalNer, usesAiQuery, selectedConfig?.detection_profile],
    },
  );

  const pollCountRef = useRef(0);

  useEffect(() => {
    if (!runStatus?.state || TERMINAL_STATES.includes(runStatus.state)) return;
    failCountRef.current = 0;
    pollCountRef.current = 0;
    let timeoutId: ReturnType<typeof setTimeout> | null = null;

    async function poll() {
      try {
        const res = await fetch(`/api/pipeline/status/${runStatus!.run_id}`);
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

  // Tick every second while a run is active so the elapsed-time display stays live.
  const [nowTs, setNowTs] = useState(() => Date.now());
  const runActive = runStatus?.state != null && !TERMINAL_STATES.includes(runStatus.state);
  useEffect(() => {
    if (!runActive) return;
    const id = setInterval(() => setNowTs(Date.now()), 1000);
    return () => clearInterval(id);
  }, [runActive]);

  async function launchIngestion() {
    setRunning(true);
    try {
      const textCols = ingTextCols.split(",").map((c) => c.trim()).filter(Boolean);
      let structCols: Record<string, string> | undefined;
      if (ingStructCols.trim()) {
        try { structCols = JSON.parse(ingStructCols); } catch { setError("Invalid JSON for structured columns"); setRunning(false); return; }
      }
      const payload = {
        config_id: configId,
        source_path: ingSourcePath,
        file_format: ingFileFormat,
        output_table: ingOutputTable,
        text_columns: textCols,
        structured_columns: structCols,
        masking_strategy: maskingStrategy,
        doc_id_column: ingDocIdCol,
        checkpoint_path: ingCheckpoint || undefined,
        schema_location: ingSchemaLoc || undefined,
        max_files_per_trigger: ingMaxFiles,
        trigger_processing_time: ingTriggerTime || undefined,
        multi_column_strategy: ingMultiColStrategy,
        cluster_profile: clusterProfile,
      };
      const status = await apiPost<RunStatus>("/pipeline/run-ingestion", payload);
      setRunStatus(status);
      toast("Ingestion pipeline launched");
      refetchHistory();
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : "Failed to launch ingestion");
    }
    setRunning(false);
  }

  async function launch() {
    if (isIngestion) { launchIngestion(); return; }
    setRunning(true);
    try {
      const payload: Record<string, unknown> = {
        config_id: configId,
        source_table: qualified,
        redaction_scope: redactionScope,
        output_table: outputMode === "in_place" ? undefined : (outputTable || undefined),
        text_column: textCol,
        doc_id_column: docIdCol,
        max_rows: maxRows,
        cluster_profile: clusterProfile,
        refresh_approach: refreshApproach,
        output_mode: outputMode,
        output_strategy: outputStrategy,
        max_cost_usd: maxCostUsd ?? undefined,
      };
      if (isMultiColumn) {
        payload.masking_strategy = maskingStrategy;
        payload.text_columns = selectedTextCols;
        payload.structured_columns = Object.keys(selectedStructCols).length ? selectedStructCols : undefined;
      }
      const status = await apiPost<RunStatus>("/pipeline/run", payload);
      setRunStatus(status);
      toast("Pipeline launched");
      refetchHistory();
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : "Failed to launch pipeline");
    }
    setRunning(false);
  }

  async function retryRun(runId: number) {
    try {
      const status = await apiPost<RunStatus>(`/pipeline/retry/${runId}`, {});
      setRunStatus(status);
      toast("Retrying pipeline run");
      refetchHistory();
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : "Failed to retry run");
    }
  }

  const columnOptions = tableInfo?.columns || [];
  const isRunning = runStatus?.state && !TERMINAL_STATES.includes(runStatus.state);

  const historyColumns: Column<JobHistoryItem & Record<string, unknown>>[] = [
    { key: "run_id", header: "Run ID", render: (h) => <span className="font-mono text-xs">{h.run_id}</span> },
    { key: "config_id", header: "Config", render: (h) => <span className="text-xs">{h.config_name || h.config_id.slice(0, 8)}</span> },
    { key: "source_table", header: "Source" },
    { key: "status", header: "Status", render: (h) => (
      <Badge variant={statusToVariant(h.status)}>{h.status}</Badge>
    )},
    { key: "started_at", header: "Started", render: (h) => <span className="text-gray-500 dark:text-gray-400">{h.started_at}</span> },
    { key: "run_page_url", header: "", sortable: false, searchable: false, render: (h) => (
      <span className="flex items-center gap-2">
        {h.run_page_url && (
          <a href={h.run_page_url as string} target="_blank" rel="noreferrer"
            className="text-blue-600 dark:text-blue-400 underline text-xs">View</a>
        )}
        {h.status !== "SUCCESS" && h.status !== "RUNNING" && (
          <button className="text-amber-600 dark:text-amber-400 underline text-xs"
            onClick={() => retryRun(h.run_id)}>Retry</button>
        )}
      </span>
    )},
  ];

  const [showAdvanced, setShowAdvanced] = useState(false);

  const canLaunchIngestion = !running && !!configId && !!ingSourcePath && !!ingOutputTable && !!ingTextCols;
  const canLaunch = isIngestion ? canLaunchIngestion : (!running && !!configId && hasTable);

  // Human-readable list of what's still missing, so a disabled Launch button explains itself.
  const missingReqs: string[] = [];
  if (!configId) missingReqs.push("select a config");
  if (isIngestion) {
    if (!ingSourcePath) missingReqs.push("set a source volume path");
    if (!ingTextCols) missingReqs.push("set text columns");
    if (!ingOutputTable) missingReqs.push("set an output table");
  } else if (!hasTable) {
    missingReqs.push("choose a source table");
  }

  return (
    <div>
      <h2 className="page-title">Run Pipeline</h2>
      <p className="page-desc">Execute the PII redaction pipeline on a Unity Catalog table.</p>
      <ErrorBanner message={displayError} onDismiss={() => setError("")} />

      {/* Step 1: Essentials */}
      <div className="card p-5 mb-4 max-w-2xl space-y-4">
        <div>
          <label className="block text-sm font-medium mb-1.5">Config</label>
          {configsLoading ? (
            <SkeletonRows rows={1} />
          ) : !configs?.length ? (
            <div className="text-sm text-gray-500 dark:text-gray-400 bg-amber-50 dark:bg-amber-900/20 border border-amber-200 dark:border-amber-700 rounded-lg p-3">
              No detection configs found.{" "}
              <a href="/config" className="text-blue-600 dark:text-blue-400 font-medium hover:underline">Create one first</a>.
            </div>
          ) : (
            <select className="input-field" value={configId}
              onChange={(e) => setConfigId(e.target.value)}>
              {configs.map((c) => <option key={c.config_id} value={c.config_id}>{c.name}</option>)}
            </select>
          )}
        </div>
        {!isIngestion && (
          <div>
            <TablePicker value={sourceTable} onChange={setSourceTable} label="Source Table" />
            {loadingTable && <SkeletonRows rows={1} className="mt-1" />}
            {tableInfo && (
              <p className="text-xs text-gray-500 dark:text-gray-400 mt-1">
                {tableInfo.row_count.toLocaleString()} rows, {tableInfo.columns.length} columns
              </p>
            )}
          </div>
        )}
        <div>
          <label className="block text-sm font-medium mb-1.5">Redaction Scope</label>
          <div className="flex rounded-lg border border-gray-200 dark:border-gray-600 overflow-hidden">
            {([
              ["full_table", "Full Table"],
              ["tagged_columns", "Tagged Columns"],
              ["single_column", "Single Column"],
              ["ingestion", "Ingestion (Volume)"],
            ] as const).map(([scope, label]) => (
              <button key={scope} type="button"
                className={`flex-1 px-4 py-2 text-sm font-medium transition-colors ${
                  redactionScope === scope
                    ? "bg-blue-600 text-white"
                    : "bg-white dark:bg-gray-800 text-gray-600 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-700"
                }`}
                onClick={() => {
                  setRedactionScope(scope as typeof redactionScope);
                  if (scope !== "single_column" && scope !== "ingestion" && outputMode === "in_place") setOutputMode("separate");
                }}>
                {label}
              </button>
            ))}
          </div>
          <p className="text-xs text-gray-400 mt-1">
            {redactionScope === "full_table"
              ? "Choose which columns to redact. Output excludes unredacted originals of selected columns."
              : redactionScope === "tagged_columns"
              ? "Detects PII across columns tagged with data_classification=protected."
              : redactionScope === "ingestion"
              ? "Redact files from a UC Volume path using AutoLoader (streaming). PII never persists raw."
              : "Runs NER detection on a single text column."}
          </p>
        </div>

        {/* Column config -- single column mode */}
        {redactionScope === "single_column" && (
          <div className="grid grid-cols-2 gap-4">
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
          </div>
        )}

        {/* Column config -- full table mode */}
        {redactionScope === "full_table" && hasTable && (
          <div className="border border-gray-200 dark:border-gray-600 rounded-lg p-4 space-y-4">
            <p className="text-sm text-gray-500 dark:text-gray-400">
              Select which columns to redact. Unselected columns pass through unchanged.
              Original values of redacted columns are excluded from the output.
            </p>
            {loadingAllColumns && <p className="text-sm text-gray-400 animate-pulse">Discovering columns...</p>}
            {allColumns && !loadingAllColumns && (
              <>
                {allColumns.text_columns.length > 0 && (
                  <div>
                    <label className="block text-sm font-medium mb-2">Text Columns (NER detection)</label>
                    <div className="flex flex-wrap gap-2">
                      {allColumns.text_columns.map((col) => (
                        <label key={col} className="inline-flex items-center gap-1.5 text-sm cursor-pointer">
                          <input type="checkbox" checked={selectedTextCols.includes(col)}
                            onChange={(e) => setSelectedTextCols((prev) =>
                              e.target.checked ? [...prev, col] : prev.filter((c) => c !== col)
                            )} />
                          {col}
                        </label>
                      ))}
                    </div>
                  </div>
                )}
                {allColumns.non_text_columns.length > 0 && (
                  <div>
                    <label className="block text-sm font-medium mb-2">Structured Columns (rule-based masking)</label>
                    <div className="flex flex-wrap gap-2">
                      {allColumns.non_text_columns.map((col) => (
                        <label key={col} className="inline-flex items-center gap-1.5 text-sm cursor-pointer">
                          <input type="checkbox" checked={col in selectedStructCols}
                            onChange={(e) => setSelectedStructCols((prev) => {
                              if (e.target.checked) return { ...prev, [col]: "unknown" };
                              const next = { ...prev }; delete next[col]; return next;
                            })} />
                          {col}
                        </label>
                      ))}
                    </div>
                  </div>
                )}
              </>
            )}
            <div className="grid grid-cols-2 gap-4">
              <div>
                <label className="block text-sm font-medium mb-1.5">Masking Strategy</label>
                <select className="input-field" value={maskingStrategy}
                  onChange={(e) => setMaskingStrategy(e.target.value as "mask" | "hash" | "encrypt")}>
                  <option value="mask">Mask (replace with label)</option>
                  <option value="hash">Hash (SHA-256)</option>
                  <option value="encrypt">Encrypt (AES)</option>
                </select>
                {maskingStrategy === "encrypt" && (
                  <p className="text-xs text-amber-600 dark:text-amber-400 mt-1">
                    Requires an Encryption Secret (scope/key) set in the config's Advanced Settings.
                  </p>
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
            </div>
          </div>
        )}

        {/* Column config -- tagged columns mode */}
        {isTaggedColumns && hasTable && (
          <div className="border border-gray-200 dark:border-gray-600 rounded-lg p-4 space-y-4">
            {loadingDiscovery && <p className="text-sm text-gray-400 animate-pulse">Discovering tagged columns...</p>}
            {discovered && !loadingDiscovery && (
              <>
                {discovered.warnings.map((w, i) => (
                  <div key={i} className="text-xs bg-amber-50 dark:bg-amber-900/20 border border-amber-200 dark:border-amber-700 rounded px-3 py-2 text-amber-700 dark:text-amber-300">{w}</div>
                ))}
                {discovered.text_columns.length === 0 && Object.keys(discovered.structured_columns).length === 0 && (
                  <div className="text-sm text-gray-500">
                    No columns with UC PII tags found. Tag columns with <code>data_classification=protected</code> first,
                    or use Single Column mode for untagged text.
                  </div>
                )}
                {discovered.text_columns.length > 0 && (
                  <div>
                    <label className="block text-sm font-medium mb-2">Text Columns (NER detection)</label>
                    <div className="flex flex-wrap gap-2">
                      {discovered.text_columns.map((col) => (
                        <label key={col} className="inline-flex items-center gap-1.5 text-sm cursor-pointer">
                          <input type="checkbox" checked={selectedTextCols.includes(col)}
                            onChange={(e) => setSelectedTextCols((prev) =>
                              e.target.checked ? [...prev, col] : prev.filter((c) => c !== col)
                            )} />
                          {col}
                        </label>
                      ))}
                    </div>
                  </div>
                )}
                {Object.keys(discovered.structured_columns).length > 0 && (
                  <div>
                    <label className="block text-sm font-medium mb-2">Structured Columns (rule-based masking)</label>
                    <div className="flex flex-wrap gap-2">
                      {Object.entries(discovered.structured_columns).map(([col, piiType]) => (
                        <label key={col} className="inline-flex items-center gap-1.5 text-sm cursor-pointer">
                          <input type="checkbox" checked={col in selectedStructCols}
                            onChange={(e) => setSelectedStructCols((prev) => {
                              if (e.target.checked) return { ...prev, [col]: piiType };
                              const next = { ...prev }; delete next[col]; return next;
                            })} />
                          {col} <span className="text-xs text-gray-400">({piiType})</span>
                        </label>
                      ))}
                    </div>
                  </div>
                )}
                <div className="grid grid-cols-2 gap-4">
                  <div>
                    <label className="block text-sm font-medium mb-1.5">Masking Strategy</label>
                    <select className="input-field" value={maskingStrategy}
                      onChange={(e) => setMaskingStrategy(e.target.value as "mask" | "hash" | "encrypt")}>
                      <option value="mask">Mask (replace with label)</option>
                      <option value="hash">Hash (SHA-256)</option>
                      <option value="encrypt">Encrypt (AES)</option>
                    </select>
                    {maskingStrategy === "encrypt" && (
                      <p className="text-xs text-amber-600 dark:text-amber-400 mt-1">
                        Requires an Encryption Secret (scope/key) set in the config's Advanced Settings.
                      </p>
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
                </div>
              </>
            )}
          </div>
        )}

        {/* Ingestion (Volume) mode */}
        {isIngestion && (
          <div className="border border-gray-200 dark:border-gray-600 rounded-lg p-4 space-y-4">
            <div className="grid grid-cols-2 gap-4">
              <div className="col-span-2">
                <label className="block text-sm font-medium mb-1.5">Source Volume Path</label>
                <input className="input-field" value={ingSourcePath}
                  onChange={(e) => setIngSourcePath(e.target.value)}
                  placeholder="/Volumes/catalog/schema/volume/path/" />
              </div>
              <div>
                <label className="block text-sm font-medium mb-1.5">File Format</label>
                <select className="input-field" value={ingFileFormat}
                  onChange={(e) => setIngFileFormat(e.target.value as typeof ingFileFormat)}>
                  <option value="json">JSON</option>
                  <option value="csv">CSV</option>
                  <option value="parquet">Parquet</option>
                  <option value="text">Text</option>
                </select>
              </div>
              <div>
                <label className="block text-sm font-medium mb-1.5">Output Table</label>
                <input className="input-field" value={ingOutputTable}
                  onChange={(e) => setIngOutputTable(e.target.value)}
                  placeholder="catalog.schema.redacted_output" />
              </div>
              <div className="col-span-2">
                <label className="block text-sm font-medium mb-1.5">Text Columns <span className="text-gray-400 font-normal">(comma-separated)</span></label>
                <input className="input-field" value={ingTextCols}
                  onChange={(e) => setIngTextCols(e.target.value)}
                  placeholder="text, notes, body" />
              </div>
              <div className="col-span-2">
                <label className="block text-sm font-medium mb-1.5">Structured Columns <span className="text-gray-400 font-normal">(JSON, optional)</span></label>
                <input className="input-field" value={ingStructCols}
                  onChange={(e) => setIngStructCols(e.target.value)}
                  placeholder='{"ssn_col": "ssn", "email_col": "email"}' />
              </div>
              <div>
                <label className="block text-sm font-medium mb-1.5">Masking Strategy</label>
                <select className="input-field" value={maskingStrategy}
                  onChange={(e) => setMaskingStrategy(e.target.value as typeof maskingStrategy)}>
                  <option value="mask">Mask (replace with label)</option>
                  <option value="hash">Hash (SHA-256)</option>
                  <option value="encrypt">Encrypt (AES)</option>
                </select>
              </div>
              <div>
                <label className="block text-sm font-medium mb-1.5">Doc ID Column</label>
                <input className="input-field" value={ingDocIdCol}
                  onChange={(e) => setIngDocIdCol(e.target.value)} />
              </div>
              <div>
                <label className="block text-sm font-medium mb-1.5">Checkpoint Path <span className="text-gray-400 font-normal">(auto if empty)</span></label>
                <input className="input-field" value={ingCheckpoint}
                  onChange={(e) => setIngCheckpoint(e.target.value)}
                  placeholder="auto-derived" />
              </div>
              <div>
                <label className="block text-sm font-medium mb-1.5">Schema Location <span className="text-gray-400 font-normal">(auto if empty)</span></label>
                <input className="input-field" value={ingSchemaLoc}
                  onChange={(e) => setIngSchemaLoc(e.target.value)}
                  placeholder="auto-derived" />
              </div>
              <div>
                <label className="block text-sm font-medium mb-1.5">Max Files Per Trigger</label>
                <input type="number" className="input-field" min={1}
                  value={ingMaxFiles}
                  onChange={(e) => { const v = parseInt(e.target.value); if (!isNaN(v) && v > 0) setIngMaxFiles(v); }} />
              </div>
              <div>
                <label className="block text-sm font-medium mb-1.5">Trigger Mode</label>
                <input className="input-field" value={ingTriggerTime}
                  onChange={(e) => setIngTriggerTime(e.target.value)}
                  placeholder="empty = process all then stop" />
                <p className="text-xs text-gray-400 mt-0.5">e.g. "30 seconds" for continuous, or leave empty</p>
              </div>
              <div>
                <label className="block text-sm font-medium mb-1.5">Multi-Column Strategy</label>
                <select className="input-field" value={ingMultiColStrategy}
                  onChange={(e) => setIngMultiColStrategy(e.target.value as typeof ingMultiColStrategy)}>
                  <option value="sequential">Sequential (safer)</option>
                  <option value="concat">Concat (faster)</option>
                </select>
              </div>
            </div>
          </div>
        )}

        {/* Cost estimate -- always visible before launch so cost is never a surprise */}
        {showCostPanel && hasTable && !isIngestion && (
          <div className="border border-blue-200 dark:border-blue-800 rounded-lg bg-blue-50/50 dark:bg-blue-900/10">
            <div
              className="text-xs font-semibold text-blue-700 dark:text-blue-300 p-3 pb-0"
              title="Estimates are directional, based on observed benchmarks with ensemble detection. Actual costs vary with data shape, cluster load, and model endpoint."
            >
              Cost Estimate
              <span className="text-[10px] bg-amber-200 dark:bg-amber-800 text-amber-800 dark:text-amber-200 px-1.5 py-0.5 rounded-full ml-1.5 font-medium">Beta</span>
            </div>
            <div className="px-3 pb-3 pt-2">
              {isMultiColumn && (
                <p className="text-xs text-amber-600 dark:text-amber-400 mb-2">
                  Estimate is based on a single column. Multi-column scopes process all text columns and may cost significantly more.
                </p>
              )}
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
              <div className="flex items-center gap-2 mt-2 pt-2 border-t border-blue-200 dark:border-blue-800">
                <label className="text-xs text-gray-500 whitespace-nowrap">Cost limit (USD):</label>
                <input type="number" step="0.01" min="0" className="input-field w-28 text-xs"
                  placeholder="none"
                  value={maxCostUsd ?? ""}
                  onChange={(e) => {
                    const v = e.target.value;
                    setMaxCostUsd(v === "" ? null : Math.max(0, Number(v)));
                  }} />
                {maxCostUsd != null && <span className="text-[10px] text-gray-400">Run will be rejected if estimate exceeds this</span>}
              </div>
            </div>
          </div>
        )}

        {/* Launch */}
        <div className="pt-1">
          <Button variant="success" className="w-full sm:w-auto" loading={running} loadingText="Launching..."
            disabled={!canLaunch}
            onClick={() => outputMode === "in_place" && !isIngestion ? setConfirmOpen(true) : launch()}>
            {isIngestion ? "Run Ingestion Pipeline" : "Run Pipeline"}
          </Button>
          {!canLaunch && !running && missingReqs.length > 0 && (
            <p className="text-xs text-amber-600 dark:text-amber-400 mt-2">
              To launch: {missingReqs.join(", ")}.
            </p>
          )}
        </div>
      </div>

      {/* Step 2: Advanced settings -- collapsed by default */}
      <details className="card mb-6 max-w-2xl" open={showAdvanced}
        onToggle={(e) => setShowAdvanced((e.target as HTMLDetailsElement).open)}>
        <summary className="px-5 py-3 text-sm font-medium cursor-pointer select-none text-gray-600 dark:text-gray-300 hover:text-gray-900 dark:hover:text-white transition-colors">
          Advanced Settings
        </summary>
        <div className="px-5 pb-5 grid grid-cols-2 gap-4 border-t border-gray-100 dark:border-gray-700 pt-4">
          {outputMode === "separate" && (
            <div className="col-span-2">
              <label className="block text-sm font-medium mb-1.5">
                Output Table <span className="text-gray-400 font-normal">(defaults to source_table_redacted)</span>
              </label>
              <input className="input-field" value={outputTable}
                onChange={(e) => setOutputTable(e.target.value)} placeholder="catalog.schema.output_table" />
            </div>
          )}
          <div>
            <label className="block text-sm font-medium mb-1.5">
              Max Rows
              {tableInfo && <span className="text-gray-400 font-normal ml-1">({tableInfo.row_count.toLocaleString()} total)</span>}
            </label>
            <input type="number" className="input-field" min={1}
              value={maxRows}
              onChange={(e) => {
                const v = parseInt(e.target.value);
                if (!isNaN(v) && v > 0) setMaxRows(v);
              }} />
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
            {usesLocalNer && !useGpu && (
              <span className="text-xs text-amber-600 dark:text-amber-400">GPU recommended when NER model is enabled</span>
            )}
          </div>
          <div>
            <label className="block text-sm font-medium mb-1.5">Output Mode</label>
            <select className="input-field" value={outputMode}
              onChange={(e) => setOutputMode(e.target.value as "separate" | "in_place")}
              disabled={isMultiColumn}>
              <option value="separate">Separate table</option>
              <option value="in_place" disabled={isMultiColumn}>In-place (destructive)</option>
            </select>
          </div>
          <div>
            <label className="block text-sm font-medium mb-1.5">Refresh Mode</label>
            <select className="input-field" value={refreshApproach}
              onChange={(e) => setRefreshApproach(e.target.value as "full" | "incremental")}
              disabled={isMultiColumn}>
              <option value="full">Full (overwrite)</option>
              <option value="incremental" disabled={isMultiColumn}>Incremental (append)</option>
            </select>
          </div>
          <div>
            <label className="block text-sm font-medium mb-1.5">Output Detail</label>
            <select className="input-field" value={outputStrategy}
              onChange={(e) => setOutputStrategy(e.target.value as "production" | "validation")}>
              <option value="production">Minimal (redacted text only)</option>
              <option value="validation">Full (includes detection details for debugging)</option>
            </select>
            {outputStrategy === "validation" && (
              <p className="text-xs text-amber-600 dark:text-amber-400 mt-1">
                Validation output includes original entity text and detection spans. The output table will contain PII.
              </p>
            )}
          </div>
          {outputMode === "in_place" && !isMultiColumn && (
            <div className="col-span-2 rounded-lg bg-amber-50 dark:bg-amber-900/30 border border-amber-300 dark:border-amber-700 p-3 text-sm text-amber-800 dark:text-amber-200">
              <strong>Destructive operation:</strong> The <code>{textCol}</code> column in the source table will be permanently overwritten with redacted text.
              {selectedConfig?.translate_to && selectedConfig.translate_to !== "none" && (
                <span className="block mt-1 text-xs">Translation and review columns are computed but not persisted in in-place mode. Use separate output mode to retain them.</span>
              )}
            </div>
          )}
          {refreshApproach === "full" && outputTable && (
            <div className="col-span-2 text-xs text-amber-600 dark:text-amber-400 bg-amber-50 dark:bg-amber-900/10 border border-amber-200 dark:border-amber-800 rounded-lg px-3 py-2">
              Full refresh will overwrite the existing output table if it exists.
            </div>
          )}
        </div>
      </details>

      {/* PI Discovery -- separate card */}
      <details className="card mb-6 max-w-2xl">
        <summary className="px-5 py-3 text-sm font-medium cursor-pointer select-none text-gray-600 dark:text-gray-300 hover:text-gray-900 dark:hover:text-white transition-colors">
          PI Discovery (dbxmetagen)
        </summary>
        <div className="px-5 pb-5 space-y-4 border-t border-gray-100 dark:border-gray-700 pt-4">
          <p className="text-xs text-gray-500 dark:text-gray-400">
            Run LLM-powered PI/PHI/PCI detection via dbxmetagen. Tags tables or writes to a knowledge base.
          </p>
          <div className="grid grid-cols-2 gap-4">
            <div className="col-span-2">
              <label className="block text-sm font-medium mb-1.5">Table Names <span className="text-gray-400 font-normal">(wildcard or comma-sep)</span></label>
              <input className="input-field" value={piTableNames}
                onChange={(e) => setPiTableNames(e.target.value)}
                placeholder="my_catalog.my_schema.*" />
            </div>
            <div>
              <label className="block text-sm font-medium mb-1.5">Destination Catalog</label>
              <input className="input-field" value={piCatalog}
                onChange={(e) => setPiCatalog(e.target.value)} placeholder="catalog" />
            </div>
            <div>
              <label className="block text-sm font-medium mb-1.5">Destination Schema</label>
              <input className="input-field" value={piSchema}
                onChange={(e) => setPiSchema(e.target.value)} placeholder="schema" />
            </div>
            <div>
              <label className="block text-sm font-medium mb-1.5">Output Mode</label>
              <select className="input-field" value={piOutputMode}
                onChange={(e) => setPiOutputMode(e.target.value as "tag" | "knowledge_base")}>
                <option value="tag">Tag (apply UC tags directly)</option>
                <option value="knowledge_base">Knowledge Base (write to log only)</option>
              </select>
            </div>
            <div>
              <label className="block text-sm font-medium mb-1.5">LLM Model</label>
              <input className="input-field" value={piModel}
                onChange={(e) => setPiModel(e.target.value)} />
            </div>
            <div className="col-span-2 flex items-center gap-3">
              <label className="relative inline-flex items-center cursor-pointer">
                <input type="checkbox" className="sr-only peer" checked={piDeterministic}
                  onChange={(e) => setPiDeterministic(e.target.checked)} />
                <div className="w-9 h-5 bg-gray-200 peer-focus:outline-none rounded-full peer dark:bg-gray-600
                  peer-checked:after:translate-x-full peer-checked:after:border-white after:content-['']
                  after:absolute after:top-[2px] after:left-[2px] after:bg-white after:border-gray-300
                  after:border after:rounded-full after:h-4 after:w-4 after:transition-all
                  peer-checked:bg-blue-600" />
              </label>
              <span className="text-sm">Include Presidio pre-scan (deterministic PI)</span>
            </div>
          </div>
          <button
            className="btn-success"
            disabled={piRunning || !piTableNames || !piCatalog || !piSchema}
            onClick={async () => {
              setPiRunning(true);
              try {
                const payload: MetagenPIRequest = {
                  table_names: piTableNames,
                  catalog_name: piCatalog,
                  schema_name: piSchema,
                  output_mode: piOutputMode,
                  model: piModel,
                  include_deterministic_pi: piDeterministic,
                };
                const status = await apiPost<RunStatus>("/pipeline/run-metagen-pi", payload);
                setPiStatus(status);
                toast("PI Discovery launched");
                refetchHistory();
              } catch (e: unknown) {
                setError(e instanceof Error ? e.message : "Failed to launch PI Discovery");
              }
              setPiRunning(false);
            }}>
            {piRunning ? "Launching..." : "Run PI Discovery"}
          </button>
          {piStatus && (
            <div className="text-xs mt-2">
              <span>Run #{piStatus.run_id} -- {piStatus.state}</span>
              {piStatus.run_page_url && (
                <a href={piStatus.run_page_url} target="_blank" rel="noreferrer"
                  className="text-blue-600 dark:text-blue-400 underline ml-2">View in Databricks</a>
              )}
            </div>
          )}
        </div>
      </details>

      <ConfirmDialog
        open={confirmOpen}
        variant="danger"
        title="Confirm In-Place Redaction"
        message={`This will permanently overwrite the "${textCol}" column in ${qualified}. This cannot be undone.`}
        confirmLabel="Redact In-Place"
        onConfirm={() => { setConfirmOpen(false); launch(); }}
        onCancel={() => setConfirmOpen(false)}
      />

      {runStatus && (
        <div className={`status-banner mb-6 ${isRunning ? "status-running"
          : runStatus.result_state === "SUCCESS" ? "status-success" : "status-error"}`}>
          <div className="flex items-center gap-2">
            {isRunning && <span className="inline-block w-2 h-2 rounded-full bg-amber-500 animate-pulse" />}
            <span>Run #{runStatus.run_id} -- <b>{isRunning ? phaseLabel(runStatus.state) : runStatus.state}</b></span>
            {runStatus.result_state && <span className="opacity-70">({runStatus.result_state})</span>}
            {isRunning && runStatus.start_time && (
              <span className="opacity-70 text-xs">-- {formatElapsed(nowTs - runStatus.start_time)} elapsed</span>
            )}
          </div>
          {isRunning && (
            <div className="mt-2 h-1 w-full overflow-hidden rounded-full bg-amber-200/60 dark:bg-amber-900/40">
              <div className="h-full w-1/3 animate-progress-indeterminate rounded-full bg-amber-500" />
            </div>
          )}
          <div className="flex items-center gap-3 mt-1">
            {runStatus.run_page_url && (
              <a href={runStatus.run_page_url} target="_blank" rel="noreferrer"
                className="text-blue-600 dark:text-blue-400 underline text-xs">
                View in Databricks
              </a>
            )}
            {!isRunning && (runStatus.result_state === "SUCCESS" || runStatus.state === "TERMINATED") && (
              <Link to="/review" className="text-xs px-3 py-1.5 rounded-lg bg-blue-600 text-white hover:bg-blue-700 transition-colors font-medium">
                Review Results
              </Link>
            )}
            {isRunning && (
              <button className="text-xs text-red-600 dark:text-red-400 underline"
                onClick={async () => {
                  try {
                    await apiPost(`/pipeline/cancel/${runStatus.run_id}`, {});
                    setRunStatus((prev) => prev ? { ...prev, state: "CANCELLED", result_state: "CANCELLED" } : prev);
                    toast("Pipeline cancelled");
                    refetchHistory();
                  } catch (e: unknown) {
                    setError(e instanceof Error ? e.message : "Failed to cancel");
                  }
                }}>Cancel</button>
            )}
            {!isRunning && runStatus.result_state !== "SUCCESS" && (
              <button className="text-xs px-3 py-1.5 rounded-lg bg-amber-600 text-white hover:bg-amber-700 transition-colors font-medium"
                onClick={() => retryRun(runStatus.run_id)}>
                Retry
              </button>
            )}
          </div>
        </div>
      )}

      <h3 className="text-lg font-semibold mb-3 flex items-center gap-2">
        Recent Runs
        <button type="button" onClick={() => refetchHistory()} className="btn-secondary text-sm">Refresh</button>
      </h3>
      {loadingHistory && <SkeletonRows rows={3} />}
      <DataTable<JobHistoryItem & Record<string, unknown>>
        data={(history ?? []) as (JobHistoryItem & Record<string, unknown>)[]}
        rowKey={(h) => String(h.run_id)}
        emptyMessage="No runs yet."
        columns={historyColumns}
      />
    </div>
  );
}
