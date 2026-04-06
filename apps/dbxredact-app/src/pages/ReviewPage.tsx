import { useState, useEffect } from "react";
import TablePicker, { type TableRef, emptyTableRef, toQualified, isComplete } from "../components/TablePicker";
import ErrorBanner from "../components/ErrorBanner";
import { SkeletonRows } from "../components/Skeleton";
import { useGet } from "../hooks/useApi";
import type { JobHistoryItem } from "../types";

interface TableInfo {
  columns: string[];
  row_count: number;
}

interface CompareData {
  rows: CompareRow[];
  total: number;
}

interface CompareRow {
  doc_id: string;
  original_text: string;
  redacted_text: string;
}

function parseTableRef(qualified: string): TableRef | null {
  const parts = qualified?.split(".");
  if (parts?.length === 3 && parts.every(Boolean)) return { catalog: parts[0], schema: parts[1], table: parts[2] };
  return null;
}

export default function ReviewPage() {
  const [sourceTable, setSourceTable] = useState<TableRef>(emptyTableRef);
  const [outputTable, setOutputTable] = useState<TableRef>(emptyTableRef);
  const [sourceCol, setSourceCol] = useState("text");
  const [outputCol, setOutputCol] = useState("text_redacted");
  const [docIdCol, setDocIdCol] = useState("doc_id");
  const [offset, setOffset] = useState(0);
  const [error, setError] = useState("");
  const [selectedRunId, setSelectedRunId] = useState<string>("");

  const srcQualified = toQualified(sourceTable);
  const outQualified = toQualified(outputTable);
  const isSourceReady = isComplete(sourceTable);
  const isOutputReady = isComplete(outputTable);

  const { data: historyData, error: historyError } = useGet<JobHistoryItem[]>("/pipeline/history");

  const completedRuns = (historyData ?? []).filter((h) => h.status === "SUCCESS" && h.output_table);

  useEffect(() => {
    if (selectedRunId || !completedRuns.length) return;
    const run = completedRuns[0];
    setSelectedRunId(String(run.run_id));
    const src = parseTableRef(run.source_table);
    const out = parseTableRef(run.output_table);
    if (src) setSourceTable(src);
    if (out) setOutputTable(out);
  }, [completedRuns.length]);

  function selectRun(runId: string) {
    setSelectedRunId(runId);
    setOffset(0);
    const run = completedRuns.find((r) => String(r.run_id) === runId);
    if (run) {
      const src = parseTableRef(run.source_table);
      const out = parseTableRef(run.output_table);
      if (src) setSourceTable(src);
      if (out) setOutputTable(out);
    }
  }

  const { data: sourceInfo } = useGet<TableInfo>(
    `/pipeline/table-info?table=${encodeURIComponent(srcQualified)}`,
    { enabled: isSourceReady, deps: [srcQualified] },
  );
  const { data: outputInfo } = useGet<TableInfo>(
    `/pipeline/table-info?table=${encodeURIComponent(outQualified)}`,
    { enabled: isOutputReady, deps: [outQualified] },
  );

  useEffect(() => {
    if (!sourceInfo) return;
    if (sourceInfo.columns.includes("text")) setSourceCol("text");
    else if (sourceInfo.columns.length) setSourceCol(sourceInfo.columns[0]);
    if (sourceInfo.columns.includes("doc_id")) setDocIdCol("doc_id");
  }, [sourceInfo]);

  const redactedCols = (outputInfo?.columns ?? []).filter((c) => c.endsWith("_redacted"));

  useEffect(() => {
    if (!outputInfo) return;
    if (redactedCols.length) setOutputCol(redactedCols[0]);
    else if (outputInfo.columns.includes("redacted_text")) setOutputCol("redacted_text");
    else if (outputInfo.columns.length) setOutputCol(outputInfo.columns[0]);
  }, [outputInfo]);

  const compareParams = new URLSearchParams({
    source_table: srcQualified, source_column: sourceCol,
    output_table: outQualified, output_column: outputCol,
    doc_id_column: docIdCol, limit: "1", offset: String(offset),
  }).toString();

  const { data: compareData, loading, error: compareError } = useGet<CompareData>(
    `/review/compare?${compareParams}`,
    { enabled: isSourceReady && isOutputReady && !!sourceInfo && !!outputInfo, retries: 2, deps: [srcQualified, sourceCol, outQualified, outputCol, docIdCol, offset] },
  );

  const displayError = error || compareError || historyError || "";
  const doc = compareData?.rows?.[0];
  const total = compareData?.total ?? 0;
  const sourceCols = sourceInfo?.columns ?? [];
  const outputCols = outputInfo?.columns ?? [];

  return (
    <div>
      <ErrorBanner message={displayError} onDismiss={() => setError("")} />
      <h2 className="page-title">Review Redaction Output</h2>
      <p className="page-desc">
        Compare original text side-by-side with the redacted output. Select a completed run or manually pick tables.
      </p>

      {/* Quick-select from run history */}
      {completedRuns.length > 0 && (
        <div className="mb-4 max-w-4xl">
          <label className="block text-sm font-medium mb-1.5">Select a completed run</label>
          <select className="input-field max-w-lg" value={selectedRunId}
            onChange={(e) => selectRun(e.target.value)}>
            {completedRuns.map((r) => (
              <option key={r.run_id} value={String(r.run_id)}>
                {r.source_table} {r.started_at ? `(${r.started_at})` : ""}
              </option>
            ))}
          </select>
        </div>
      )}
      {!completedRuns.length && !historyError && (
        <div className="mb-4 text-sm text-gray-500 dark:text-gray-400 bg-gray-50 dark:bg-gray-800 rounded-lg p-3 max-w-4xl">
          No completed pipeline runs found. <a href="/run" className="text-blue-600 dark:text-blue-400 font-medium hover:underline">Run a pipeline first</a>.
        </div>
      )}

      <div className="card p-5 mb-6 space-y-4 max-w-4xl">
        <div className="grid grid-cols-2 gap-4">
          <div>
            <TablePicker value={sourceTable} onChange={(v) => { setSourceTable(v); setOffset(0); }} label="Source Table (original text)" />
            {sourceCols.length > 0 && (
              <div className="mt-2 grid grid-cols-2 gap-2">
                <div>
                  <label className="block text-xs font-medium mb-1 text-gray-500 dark:text-gray-400">Text column</label>
                  <select className="input-field text-sm" value={sourceCol} onChange={(e) => setSourceCol(e.target.value)}>
                    {sourceCols.map((c) => <option key={c} value={c}>{c}</option>)}
                  </select>
                </div>
                <div>
                  <label className="block text-xs font-medium mb-1 text-gray-500 dark:text-gray-400">Doc ID column</label>
                  <select className="input-field text-sm" value={docIdCol} onChange={(e) => setDocIdCol(e.target.value)}>
                    {sourceCols.map((c) => <option key={c} value={c}>{c}</option>)}
                  </select>
                </div>
              </div>
            )}
          </div>
          <div>
            <TablePicker value={outputTable} onChange={(v) => { setOutputTable(v); setOffset(0); }} label="Output Table (redacted text)" />
            {redactedCols.length > 1 && (
              <div className="mt-2 text-xs bg-blue-50 dark:bg-blue-900/20 border border-blue-200 dark:border-blue-700 rounded px-3 py-2 text-blue-700 dark:text-blue-300">
                This table has {redactedCols.length} redacted columns. Use the dropdown to compare each.
              </div>
            )}
            {outputCols.length > 0 && (
              <div className="mt-2">
                <label className="block text-xs font-medium mb-1 text-gray-500 dark:text-gray-400">Redacted text column</label>
                <select className="input-field text-sm" value={outputCol} onChange={(e) => setOutputCol(e.target.value)}>
                  {outputCols.map((c) => <option key={c} value={c}>{c}</option>)}
                </select>
              </div>
            )}
          </div>
        </div>
      </div>

      {loading && <SkeletonRows rows={4} />}

      {doc && (
        <>
          <div className="flex items-center justify-between mb-4 max-w-4xl">
            <span className="text-sm text-gray-500 dark:text-gray-400">
              Document {offset + 1} of {total} -- <span className="font-mono text-xs">{doc.doc_id}</span>
            </span>
            <div className="flex gap-2">
              <button className="btn-ghost text-xs" disabled={offset === 0}
                onClick={() => setOffset((o) => Math.max(0, o - 1))}>Prev</button>
              <button className="btn-ghost text-xs" disabled={offset >= total - 1}
                onClick={() => setOffset((o) => o + 1)}>Next</button>
            </div>
          </div>

          <div className="grid grid-cols-2 gap-4 max-w-4xl">
            <div>
              <h3 className="text-sm font-semibold mb-2 text-gray-600 dark:text-gray-300">Original</h3>
              <div className="card p-4 text-sm leading-relaxed whitespace-pre-wrap max-h-[70vh] overflow-y-auto">
                {doc.original_text}
              </div>
            </div>
            <div>
              <h3 className="text-sm font-semibold mb-2 text-gray-600 dark:text-gray-300">Redacted</h3>
              <div className="card p-4 text-sm leading-relaxed whitespace-pre-wrap max-h-[70vh] overflow-y-auto">
                {doc.redacted_text}
              </div>
            </div>
          </div>
        </>
      )}

      {!loading && isSourceReady && isOutputReady && total === 0 && (
        <p className="text-sm text-gray-400">No matching documents found. Ensure both tables share a common doc ID column.</p>
      )}
    </div>
  );
}
