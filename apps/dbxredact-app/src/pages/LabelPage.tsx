import { useState, useEffect } from "react";
import TablePicker from "../components/TablePicker";
import EntityHighlighter from "../components/EntityHighlighter";
import { apiPost } from "../hooks/useApi";

interface LabelEntry {
  entity_text: string;
  entity_type: string;
  start: number;
  end: number;
}

const ENTITY_TYPES = [
  "PERSON", "PHONE_NUMBER", "EMAIL_ADDRESS", "LOCATION", "DATE_TIME",
  "US_SSN", "ADDRESS", "MEDICAL_RECORD_NUMBER", "IP_ADDRESS", "URL", "OTHER",
];

export default function LabelPage() {
  const [docs, setDocs] = useState<Record<string, unknown>[]>([]);
  const [table, setTable] = useState("");
  const [currentIdx, setCurrentIdx] = useState(0);
  const [labels, setLabels] = useState<LabelEntry[]>([]);
  const [entityType, setEntityType] = useState("PERSON");
  const [loading, setLoading] = useState(false);

  const parts = table.split(".");
  const hasTable = parts.length === 3 && parts[2] !== "";

  useEffect(() => {
    if (!hasTable) { setDocs([]); return; }
    setLoading(true);
    fetch(`/api/labels/documents?source_table=${encodeURIComponent(table)}`)
      .then(r => r.json()).then(d => { setDocs(d); setCurrentIdx(0); setLabels([]); })
      .catch(() => setDocs([]))
      .finally(() => setLoading(false));
  }, [table, hasTable]);

  function handleTextSelect() {
    const sel = window.getSelection();
    if (!sel || sel.isCollapsed) return;
    const text = sel.toString().trim();
    if (!text) return;
    const doc = docs[currentIdx];
    const fullText = String(doc?.text || "");
    const start = fullText.indexOf(text);
    if (start === -1) return;
    setLabels((prev) => [...prev, { entity_text: text, entity_type: entityType, start, end: start + text.length }]);
    sel.removeAllRanges();
  }

  async function saveLabels() {
    const doc = docs[currentIdx];
    await apiPost(`/api/labels/batch?doc_id=${doc.doc_id}&source_table=${encodeURIComponent(table)}`, labels);
    setLabels([]);
    setCurrentIdx((i) => Math.min(i + 1, docs.length - 1));
  }

  const doc = docs[currentIdx];

  return (
    <div>
      <h2 className="page-title">Entity Labeling</h2>
      <p className="page-desc">
        Create ground-truth labels by highlighting text spans in source documents. These labels are stored
        as corrections and used for evaluation benchmarks. Select text in the document below, choose an entity
        type, and it will be tagged. Click "Save & Next" to persist labels and advance to the next document.
      </p>

      <div className="card p-4 mb-6">
        <div className="text-xs text-gray-500 dark:text-gray-400 leading-relaxed">
          <b>Workflow:</b> 1) Pick a source table containing raw text documents.
          2) Select a span of text that contains PII. 3) Choose the entity type from the dropdown.
          4) Repeat for all PII in the document. 5) Click "Save & Next" to commit.
          Documents that already have labels are automatically skipped.
        </div>
      </div>

      <div className="mb-6 max-w-2xl">
        <TablePicker value={table} onChange={setTable} label="Source Table" />
      </div>

      {loading && <p className="text-sm text-gray-500 dark:text-gray-400 animate-pulse">Loading documents...</p>}

      {doc && (
        <div className="max-w-3xl">
          <div className="flex items-center justify-between mb-3">
            <div className="text-sm text-gray-500 dark:text-gray-400">
              Document {currentIdx + 1} of {docs.length} -- <span className="font-mono text-xs">{String(doc.doc_id)}</span>
            </div>
            <div className="flex gap-2 items-center">
              <button className="btn-ghost text-xs" disabled={currentIdx === 0}
                onClick={() => { setCurrentIdx(i => i - 1); setLabels([]); }}>Prev</button>
              <button className="btn-ghost text-xs" disabled={currentIdx >= docs.length - 1}
                onClick={() => { setCurrentIdx(i => i + 1); setLabels([]); }}>Next</button>
            </div>
          </div>

          <div className="flex gap-3 mb-4 items-center">
            <label className="text-sm font-medium">Entity type:</label>
            <select className="input-field w-auto" value={entityType}
              onChange={(e) => setEntityType(e.target.value)}>
              {ENTITY_TYPES.map((t) => <option key={t} value={t}>{t}</option>)}
            </select>
            <span className="text-xs text-gray-400">Highlight text below to label it</span>
          </div>

          <div className="card p-5 mb-4 cursor-text select-text" onMouseUp={handleTextSelect}>
            <EntityHighlighter
              text={String(doc.text || "")}
              entities={labels.map((l) => ({
                entity: l.entity_text, entity_type: l.entity_type,
                start: l.start, end: l.end,
              }))}
            />
          </div>

          {labels.length > 0 && (
            <div className="mb-4">
              <h4 className="text-sm font-medium mb-2">Labels ({labels.length})</h4>
              <div className="flex flex-wrap gap-2">
                {labels.map((l, i) => (
                  <span key={i} className="bg-amber-100 dark:bg-amber-900/30 text-amber-800 dark:text-amber-200 text-xs px-2.5 py-1 rounded-full inline-flex items-center gap-1">
                    {l.entity_text} <span className="opacity-60">[{l.entity_type}]</span>
                    <button className="ml-0.5 text-red-500 hover:text-red-700" onClick={() => setLabels((p) => p.filter((_, j) => j !== i))}>x</button>
                  </span>
                ))}
              </div>
            </div>
          )}

          <div className="flex gap-2">
            <button className="btn-primary" disabled={labels.length === 0} onClick={saveLabels}>Save & Next</button>
            <button className="btn-ghost border border-gray-200 dark:border-gray-600"
              onClick={() => { setCurrentIdx((i) => Math.min(i + 1, docs.length - 1)); setLabels([]); }}>
              Skip
            </button>
          </div>
        </div>
      )}

      {hasTable && !loading && docs.length === 0 && (
        <p className="text-sm text-gray-400">All documents in this table have been labeled, or the table is empty.</p>
      )}
    </div>
  );
}
