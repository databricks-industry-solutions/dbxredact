import { useState, useEffect } from "react";
import TablePicker from "../components/TablePicker";
import ErrorBanner from "../components/ErrorBanner";
import type { JobHistoryItem } from "../types";

interface TableInfo {
  columns: string[];
  row_count: number;
}

interface CompareRow {
  doc_id: string;
  original_text: string;
  redacted_text: string;
}

export default function ReviewPage() {
  const [sourceTable, setSourceTable] = useState("");
  const [outputTable, setOutputTable] = useState("");
  const [sourceCol, setSourceCol] = useState("text");
  const [outputCol, setOutputCol] = useState("text_redacted");
  const [docIdCol, setDocIdCol] = useState("doc_id");
  const [sourceCols, setSourceCols] = useState<string[]>([]);
  const [outputCols, setOutputCols] = useState<string[]>([]);
  const [rows, setRows] = useState<CompareRow[]>([]);
  const [total, setTotal] = useState(0);
  const [offset, setOffset] = useState(0);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const [suggested, setSuggested] = useState(false);

  const isSourceReady = sourceTable.split(".").length === 3 && sourceTable.split(".")[2] !== "";
  const isOutputReady = outputTable.split(".").length === 3 && outputTable.split(".")[2] !== "";

  // Auto-suggest from most recent completed pipeline run
  useEffect(() => {
    if (suggested) return;
    fetch("/api/pipeline/history")
      .then((r) => r.json())
      .then((history: JobHistoryItem[]) => {
        const completed = history.find((h) => h.status === "TERMINATED");
        if (completed) {
          setSourceTable(completed.source_table);
          setOutputTable(completed.output_table);
        }
        setSuggested(true);
      })
      .catch(() => setSuggested(true));
  }, [suggested]);

  // Fetch columns when source table changes
  useEffect(() => {
    if (!isSourceReady) { setSourceCols([]); return; }
    fetch(`/api/pipeline/table-info?table=${encodeURIComponent(sourceTable)}`)
      .then((r) => r.json())
      .then((info: TableInfo) => {
        setSourceCols(info.columns);
        if (info.columns.includes("text")) setSourceCol("text");
        else if (info.columns.length) setSourceCol(info.columns[0]);
        if (info.columns.includes("doc_id")) setDocIdCol("doc_id");
      })
      .catch((e) => setError(e.message || "Failed to load source table columns"));
  }, [sourceTable, isSourceReady]);

  // Fetch columns when output table changes
  useEffect(() => {
    if (!isOutputReady) { setOutputCols([]); return; }
    fetch(`/api/pipeline/table-info?table=${encodeURIComponent(outputTable)}`)
      .then((r) => r.json())
      .then((info: TableInfo) => {
        setOutputCols(info.columns);
        if (info.columns.includes("text_redacted")) setOutputCol("text_redacted");
        else if (info.columns.includes("redacted_text")) setOutputCol("redacted_text");
        else if (info.columns.length) setOutputCol(info.columns[0]);
      })
      .catch((e) => setError(e.message || "Failed to load output table columns"));
  }, [outputTable, isOutputReady]);

  // Fetch comparison data
  useEffect(() => {
    if (!isSourceReady || !isOutputReady) { setRows([]); setTotal(0); return; }
    setLoading(true);
    const params = new URLSearchParams({
      source_table: sourceTable,
      source_column: sourceCol,
      output_table: outputTable,
      output_column: outputCol,
      doc_id_column: docIdCol,
      limit: "1",
      offset: String(offset),
    });
    fetch(`/api/review/compare?${params}`)
      .then((r) => { if (!r.ok) throw new Error(`${r.status}`); return r.json(); })
      .then((data) => { setRows(data.rows); setTotal(data.total); })
      .catch((e) => setError(e.message || "Failed to load comparison"))
      .finally(() => setLoading(false));
  }, [sourceTable, sourceCol, outputTable, outputCol, docIdCol, offset, isSourceReady, isOutputReady]);

  const doc = rows[0];

  return (
    <div>
      <ErrorBanner message={error} onDismiss={() => setError("")} />
      <h2 className="page-title">Review Redaction Output</h2>
      <p className="page-desc">
        Compare original text side-by-side with the redacted output. Pick a source table and a redacted output table,
        then navigate through documents to review results.
      </p>

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

      {loading && <p className="text-sm text-gray-500 dark:text-gray-400 animate-pulse">Loading...</p>}

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
