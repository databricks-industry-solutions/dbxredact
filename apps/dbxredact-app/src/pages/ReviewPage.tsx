import { useState, useEffect } from "react";
import TablePicker from "../components/TablePicker";
import DocumentViewer from "../components/DocumentViewer";
import { apiPost } from "../hooks/useApi";

export default function ReviewPage() {
  const [docs, setDocs] = useState<Record<string, unknown>[]>([]);
  const [table, setTable] = useState("");
  const [loading, setLoading] = useState(false);

  const parts = table.split(".");
  const hasTable = parts.length === 3 && parts[2] !== "";

  useEffect(() => {
    if (!hasTable) return;
    setLoading(true);
    fetch(`/api/review/documents?source_table=${encodeURIComponent(table)}`)
      .then(r => r.json()).then(setDocs)
      .catch(() => setDocs([]))
      .finally(() => setLoading(false));
  }, [table, hasTable]);

  async function submitCorrection(docId: string, entity: Record<string, unknown>, action: string) {
    await apiPost("/review/corrections", {
      doc_id: docId,
      source_table: table,
      entity_text: entity.entity,
      entity_type: entity.entity_type,
      start: entity.start,
      end: entity.end,
      action,
    });
  }

  return (
    <div>
      <h2 className="page-title">Document Review</h2>
      <p className="page-desc">
        Select a detection results table from a benchmark or pipeline run to review and correct entity annotations.
      </p>

      <div className="mb-6 max-w-2xl">
        <TablePicker value={table} onChange={setTable} label="Detection Results Table" />
      </div>

      {loading && <p className="text-sm text-gray-500 dark:text-gray-400 animate-pulse">Loading documents...</p>}

      <div className="space-y-4">
        {docs.map((doc, i) => (
          <div key={i} className="card p-4">
            <DocumentViewer doc={doc} />
            <div className="flex gap-2 mt-3 pt-3 border-t border-gray-100 dark:border-gray-700">
              <button className="text-xs font-medium px-3 py-1.5 rounded-lg bg-emerald-50 dark:bg-emerald-900/20 text-emerald-700 dark:text-emerald-300 hover:bg-emerald-100 dark:hover:bg-emerald-900/40 transition-colors"
                onClick={() => submitCorrection(String(doc.doc_id), doc as any, "confirm")}>
                Confirm All
              </button>
              <button className="text-xs font-medium px-3 py-1.5 rounded-lg bg-red-50 dark:bg-red-900/20 text-red-700 dark:text-red-300 hover:bg-red-100 dark:hover:bg-red-900/40 transition-colors"
                onClick={() => submitCorrection(String(doc.doc_id), doc as any, "reject")}>
                Reject All
              </button>
            </div>
          </div>
        ))}
        {hasTable && !loading && docs.length === 0 && (
          <p className="text-sm text-gray-400">No documents found in this table.</p>
        )}
      </div>
    </div>
  );
}
