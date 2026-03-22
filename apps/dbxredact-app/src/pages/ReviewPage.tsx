import { useState, useEffect, useMemo } from "react";
import TablePicker, { type TableRef, emptyTableRef, toQualified, isComplete } from "../components/TablePicker";
import ErrorBanner from "../components/ErrorBanner";
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

export default function ReviewPage() {
  const [sourceTable, setSourceTable] = useState<TableRef>(emptyTableRef);
  const [outputTable, setOutputTable] = useState<TableRef>(emptyTableRef);
  const [sourceCol, setSourceCol] = useState("text");
  const [outputCol, setOutputCol] = useState("text_redacted");
  const [docIdCol, setDocIdCol] = useState("doc_id");
  const [offset, setOffset] = useState(0);
  const [error, setError] = useState("");
  const [suggested, setSuggested] = useState(false);

  const srcQualified = toQualified(sourceTable);
  const outQualified = toQualified(outputTable);
  const isSourceReady = isComplete(sourceTable);
  const isOutputReady = isComplete(outputTable);

  const { data: historyData, error: historyError } = useGet<JobHistoryItem[]>("/pipeline/history", { enabled: !suggested });

  useEffect(() => {
    if (suggested || !historyData) return;
    const completed = historyData.find((h) => h.status === "SUCCESS");
    if (completed) {
      const srcParts = completed.source_table.split(".");
      const outParts = completed.output_table.split(".");
      if (srcParts.length === 3) setSourceTable({ catalog: srcParts[0], schema: srcParts[1], table: srcParts[2] });
      if (outParts.length === 3) setOutputTable({ catalog: outParts[0], schema: outParts[1], table: outParts[2] });
    }
    setSuggested(true);
  }, [historyData, suggested]);

  const { data: sourceInfo } = useGet<TableInfo>(
    `/pipeline/table-info?table=${encodeURIComponent(srcQualified)}`,
    { enabled: isSourceReady, deps: [srcQualified] },
  );
  const { data: outputInfo } = useGet<TableInfo>(
    `/pipeline/table-info?table=${encodeURIComponent(outQualified)}`,
    { enabled: isOutputReady, deps: [outQualified] },
  );

  const redactedColumns = useMemo(() => {
    if (!outputInfo) return [];
    return outputInfo.columns.filter((c) => c.endsWith("_redacted"));
  }, [outputInfo]);

  const structuredColumns = useMemo(() => {
    if (!outputInfo || !sourceInfo) return [];
    return outputInfo.columns.filter((c) =>
      !c.endsWith("_redacted") && c !== "doc_id" && !c.endsWith("_id") && sourceInfo.columns.includes(c)
    );
  }, [outputInfo, sourceInfo]);

  useEffect(() => {
    if (!sourceInfo) return;
    if (sourceInfo.columns.includes("text")) setSourceCol("text");
    else if (sourceInfo.columns.length) setSourceCol(sourceInfo.columns[0]);
    if (sourceInfo.columns.includes("doc_id")) setDocIdCol("doc_id");
  }, [sourceInfo]);

  useEffect(() => {
    if (!outputInfo) return;
    if (redactedColumns.length > 0) {
      setOutputCol(redactedColumns[0]);
      const baseName = redactedColumns[0].replace(/_redacted$/, "");
      if (sourceInfo?.columns.includes(baseName)) setSourceCol(baseName);
    } else if (outputInfo.columns.includes("text_redacted")) {
      setOutputCol("text_redacted");
    } else if (outputInfo.columns.includes("redacted_text")) {
      setOutputCol("redacted_text");
    } else if (outputInfo.columns.length) {
      setOutputCol(outputInfo.columns[0]);
    }
  }, [outputInfo, redactedColumns]);

  function handleOutputColChange(col: string) {
    setOutputCol(col);
    setOffset(0);
    const baseName = col.replace(/_redacted$/, "");
    if (sourceInfo?.columns.includes(baseName)) setSourceCol(baseName);
  }

  const compareParams = new URLSearchParams({
    source_table: srcQualified, source_column: sourceCol,
    output_table: outQualified, output_column: outputCol,
    doc_id_column: docIdCol, limit: "1", offset: String(offset),
  }).toString();

  const { data: compareData, loading, error: compareError } = useGet<CompareData>(
    `/review/compare?${compareParams}`,
    { enabled: isSourceReady && isOutputReady, deps: [srcQualified, sourceCol, outQualified, outputCol, docIdCol, offset] },
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
                <select className="input-field text-sm" value={outputCol} onChange={(e) => handleOutputColChange(e.target.value)}>
                  {outputCols.map((c) => <option key={c} value={c}>{c}</option>)}
                </select>
              </div>
            )}
          </div>
        </div>

        {redactedColumns.length > 1 && (
          <div className="border border-blue-200 dark:border-blue-800 rounded-lg bg-blue-50/50 dark:bg-blue-900/10 p-3">
            <p className="text-xs font-medium text-blue-700 dark:text-blue-300 mb-2">
              Multi-column output detected ({redactedColumns.length} redacted columns)
            </p>
            <div className="flex flex-wrap gap-1.5">
              {redactedColumns.map((rc) => (
                <button key={rc} type="button"
                  className={`px-2.5 py-1 text-xs rounded-md border transition-colors ${
                    outputCol === rc
                      ? "bg-blue-600 text-white border-blue-600"
                      : "bg-white dark:bg-gray-800 border-gray-300 dark:border-gray-600 hover:border-blue-400"
                  }`}
                  onClick={() => handleOutputColChange(rc)}
                >
                  {rc}
                </button>
              ))}
            </div>
          </div>
        )}

        {structuredColumns.length > 0 && (
          <div className="border border-gray-200 dark:border-gray-700 rounded-lg p-3">
            <p className="text-xs font-medium text-gray-500 dark:text-gray-400 mb-1">
              Structured columns (masked in place):
            </p>
            <div className="flex flex-wrap gap-2 text-xs text-gray-600 dark:text-gray-300">
              {structuredColumns.map((c) => (
                <span key={c} className="bg-gray-100 dark:bg-gray-800 px-2 py-0.5 rounded">{c}</span>
              ))}
            </div>
          </div>
        )}
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
