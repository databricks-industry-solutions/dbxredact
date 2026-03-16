import { useState, useEffect, useRef } from "react";
import TablePicker from "../components/TablePicker";
import EntityHighlighter from "../components/EntityHighlighter";
import ErrorBanner from "../components/ErrorBanner";
import { apiPost } from "../hooks/useApi";

interface LabelEntry {
  entity_text: string;
  entity_type: string;
  start: number;
  end: number;
}

interface PreLabeledDoc {
  doc_id: string;
  text: string;
  labels: LabelEntry[];
}

interface TableInfo {
  columns: string[];
  row_count: number;
}

const ENTITY_TYPES = [
  "PERSON", "PHONE_NUMBER", "EMAIL_ADDRESS", "LOCATION", "DATE_TIME",
  "US_SSN", "ADDRESS", "MEDICAL_RECORD_NUMBER", "IP_ADDRESS", "URL",
  "HOSPITAL_NAME", "ORGANIZATION", "ID_NUMBER", "OTHER",
];

type Mode = "unlabeled" | "prelabeled";

export default function LabelPage() {
  const [mode, setMode] = useState<Mode>("unlabeled");
  const [table, setTable] = useState("");
  const [columns, setColumns] = useState<string[]>([]);
  const [textCol, setTextCol] = useState("text");
  const [docIdCol, setDocIdCol] = useState("doc_id");
  const [error, setError] = useState("");

  // Pre-labeled column mapping
  const [entityTextCol, setEntityTextCol] = useState("entity_text");
  const [entityTypeCol, setEntityTypeCol] = useState("entity_type");
  const [startCol, setStartCol] = useState("start");
  const [endCol, setEndCol] = useState("end");

  // Unlabeled mode state
  const [docs, setDocs] = useState<Record<string, unknown>[]>([]);
  const [currentIdx, setCurrentIdx] = useState(0);
  const [labels, setLabels] = useState<LabelEntry[]>([]);
  const [entityType, setEntityType] = useState("PERSON");
  const [loading, setLoading] = useState(false);

  // Pre-labeled mode state
  const [preDocs, setPreDocs] = useState<PreLabeledDoc[]>([]);
  const [preIdx, setPreIdx] = useState(0);
  const [editLabels, setEditLabels] = useState<LabelEntry[]>([]);
  const [preLoading, setPreLoading] = useState(false);

  // Dirty / saving guards
  const [isDirty, setIsDirty] = useState(false);
  const [saving, setSaving] = useState(false);

  // Labeling progress stats
  const [stats, setStats] = useState<{ total_docs: number; labeled_docs: number } | null>(null);

  const textContainerRef = useRef<HTMLDivElement>(null);

  const parts = table.split(".");
  const hasTable = parts.length === 3 && parts[2] !== "";

  // Fetch columns when table changes
  useEffect(() => {
    if (!hasTable) { setColumns([]); return; }
    fetch(`/api/pipeline/table-info?table=${encodeURIComponent(table)}`)
      .then((r) => r.json())
      .then((info: TableInfo) => {
        setColumns(info.columns);
        if (info.columns.includes("text")) setTextCol("text");
        else if (info.columns.length) setTextCol(info.columns[0]);
        if (info.columns.includes("doc_id")) setDocIdCol("doc_id");
        if (info.columns.includes("entity_text")) setEntityTextCol("entity_text");
        if (info.columns.includes("entity_type")) setEntityTypeCol("entity_type");
        if (info.columns.includes("start")) setStartCol("start");
        if (info.columns.includes("end")) setEndCol("end");
      })
      .catch((e) => setError(e.message || "Failed to load table info"));
  }, [table, hasTable]);

  // Fetch unlabeled documents
  useEffect(() => {
    if (mode !== "unlabeled" || !hasTable) { setDocs([]); return; }
    setLoading(true);
    const params = new URLSearchParams({
      source_table: table,
      text_column: textCol,
      doc_id_column: docIdCol,
    });
    fetch(`/api/labels/documents?${params}`)
      .then((r) => r.json())
      .then((d) => { setDocs(d); setCurrentIdx(0); setLabels([]); })
      .catch((e) => { setDocs([]); setError(e.message || "Failed to load documents"); })
      .finally(() => setLoading(false));
  }, [table, hasTable, mode, textCol, docIdCol]);

  // Fetch pre-labeled documents
  useEffect(() => {
    if (mode !== "prelabeled" || !hasTable) { setPreDocs([]); return; }
    setPreLoading(true);
    const params = new URLSearchParams({
      source_table: table,
      text_column: textCol,
      doc_id_column: docIdCol,
      entity_text_column: entityTextCol,
      entity_type_column: entityTypeCol,
      start_column: startCol,
      end_column: endCol,
    });
    fetch(`/api/labels/documents-with-labels?${params}`)
      .then((r) => { if (!r.ok) throw new Error(`${r.status}`); return r.json(); })
      .then((d: PreLabeledDoc[]) => {
        setPreDocs(d);
        setPreIdx(0);
        if (d.length) setEditLabels(d[0].labels);
      })
      .catch((e) => { setPreDocs([]); setError(e.message || "Failed to load labeled documents"); })
      .finally(() => setPreLoading(false));
  }, [table, hasTable, mode, textCol, docIdCol, entityTextCol, entityTypeCol, startCol, endCol]);

  // Sync edit labels when switching pre-labeled documents
  useEffect(() => {
    if (mode === "prelabeled" && preDocs[preIdx]) {
      setEditLabels(preDocs[preIdx].labels);
    }
  }, [preIdx, preDocs, mode]);

  // Fetch labeling progress stats
  function fetchStats() {
    if (!hasTable) { setStats(null); return; }
    fetch(`/api/labels/stats?source_table=${encodeURIComponent(table)}`)
      .then((r) => r.json())
      .then((s) => setStats(s))
      .catch(() => setStats(null));
  }

  useEffect(fetchStats, [table, hasTable]);

  function getTextOffsetInContainer(container: Node, targetNode: Node, targetOffset: number): number {
    let offset = 0;
    const walker = document.createTreeWalker(container, NodeFilter.SHOW_TEXT);
    let node: Node | null;
    while ((node = walker.nextNode())) {
      if (node === targetNode) return offset + targetOffset;
      offset += (node.textContent?.length ?? 0);
    }
    return offset + targetOffset;
  }

  function handleTextSelect() {
    const sel = window.getSelection();
    if (!sel || sel.isCollapsed || sel.rangeCount === 0) return;
    const text = sel.toString().trim();
    if (!text) return;
    const container = textContainerRef.current;
    if (!container) return;

    const range = sel.getRangeAt(0);
    const start = getTextOffsetInContainer(container, range.startContainer, range.startOffset);
    const end = getTextOffsetInContainer(container, range.endContainer, range.endOffset);
    if (start === end) return;

    const entry: LabelEntry = { entity_text: text, entity_type: entityType, start, end };
    if (mode === "unlabeled") setLabels((prev) => [...prev, entry]);
    else setEditLabels((prev) => [...prev, entry]);
    setIsDirty(true);
    sel.removeAllRanges();
  }

  function toBatchBody(docId: string, labelList: LabelEntry[]) {
    return {
      doc_id: String(docId),
      source_table: table,
      labels: labelList.map((l) => ({
        entity_text: l.entity_text,
        entity_type: l.entity_type,
        start: l.start,
        end_pos: l.end,
      })),
    };
  }

  async function saveUnlabeled() {
    const doc = docs[currentIdx];
    setSaving(true);
    try {
      await apiPost("/labels/batch", toBatchBody(doc.doc_id, labels));
      setLabels([]);
      setIsDirty(false);
      fetchStats();
      setCurrentIdx((i) => Math.min(i + 1, docs.length - 1));
    } catch (e: any) {
      setError(e.message || "Failed to save labels");
    } finally {
      setSaving(false);
    }
  }

  async function savePreLabeled() {
    const doc = preDocs[preIdx];
    setSaving(true);
    try {
      await apiPost("/labels/batch", toBatchBody(doc.doc_id, editLabels));
      setIsDirty(false);
      fetchStats();
      setPreIdx((i) => Math.min(i + 1, preDocs.length - 1));
    } catch (e: any) {
      setError(e.message || "Failed to save labels");
    } finally {
      setSaving(false);
    }
  }

  function confirmDiscardIfDirty(): boolean {
    if (!isDirty) return true;
    return window.confirm("You have unsaved labels. Discard changes?");
  }

  const unlabeledDoc = docs[currentIdx];
  const preDoc = preDocs[preIdx];

  return (
    <div>
      <ErrorBanner message={error} onDismiss={() => setError("")} />
      <h2 className="page-title">Entity Labeling</h2>
      <p className="page-desc">
        Create or edit ground-truth labels by highlighting text spans. Labels are stored
        as annotations and used for evaluation benchmarks.
      </p>

      {stats && (
        <div className="mb-4 inline-flex items-center gap-2 px-3 py-1.5 rounded-lg bg-blue-50 dark:bg-blue-900/20 text-sm text-blue-700 dark:text-blue-300">
          <span className="font-semibold">{stats.labeled_docs}</span> / {stats.total_docs} documents labeled
          {stats.total_docs > 0 && (
            <span className="text-xs opacity-70">
              ({Math.round((stats.labeled_docs / stats.total_docs) * 100)}%)
            </span>
          )}
        </div>
      )}

      {/* Mode selector */}
      <div className="flex gap-2 mb-6">
        <button className={`px-4 py-2 rounded-lg text-sm font-medium transition-colors ${
          mode === "unlabeled" ? "bg-blue-600 text-white" : "bg-gray-100 dark:bg-gray-800 text-gray-600 dark:text-gray-300"
        }`} onClick={() => setMode("unlabeled")}>
          Label Unlabeled Data
        </button>
        <button className={`px-4 py-2 rounded-lg text-sm font-medium transition-colors ${
          mode === "prelabeled" ? "bg-blue-600 text-white" : "bg-gray-100 dark:bg-gray-800 text-gray-600 dark:text-gray-300"
        }`} onClick={() => setMode("prelabeled")}>
          Edit Pre-Labeled Data
        </button>
      </div>

      {/* Table + column config */}
      <div className="card p-5 mb-6 max-w-3xl space-y-3">
        <TablePicker value={table} onChange={setTable} label="Source Table" />
        {columns.length > 0 && (
          <div className="grid grid-cols-2 gap-3">
            <div>
              <label className="block text-xs font-medium mb-1 text-gray-500 dark:text-gray-400">Text column</label>
              <select className="input-field text-sm" value={textCol} onChange={(e) => setTextCol(e.target.value)}>
                {columns.map((c) => <option key={c} value={c}>{c}</option>)}
              </select>
            </div>
            <div>
              <label className="block text-xs font-medium mb-1 text-gray-500 dark:text-gray-400">Doc ID column</label>
              <select className="input-field text-sm" value={docIdCol} onChange={(e) => setDocIdCol(e.target.value)}>
                {columns.map((c) => <option key={c} value={c}>{c}</option>)}
              </select>
            </div>
            {mode === "prelabeled" && (
              <>
                <div>
                  <label className="block text-xs font-medium mb-1 text-gray-500 dark:text-gray-400">Entity text column</label>
                  <select className="input-field text-sm" value={entityTextCol} onChange={(e) => setEntityTextCol(e.target.value)}>
                    {columns.map((c) => <option key={c} value={c}>{c}</option>)}
                  </select>
                </div>
                <div>
                  <label className="block text-xs font-medium mb-1 text-gray-500 dark:text-gray-400">Entity type column</label>
                  <select className="input-field text-sm" value={entityTypeCol} onChange={(e) => setEntityTypeCol(e.target.value)}>
                    {columns.map((c) => <option key={c} value={c}>{c}</option>)}
                  </select>
                </div>
                <div>
                  <label className="block text-xs font-medium mb-1 text-gray-500 dark:text-gray-400">Start column</label>
                  <select className="input-field text-sm" value={startCol} onChange={(e) => setStartCol(e.target.value)}>
                    {columns.map((c) => <option key={c} value={c}>{c}</option>)}
                  </select>
                </div>
                <div>
                  <label className="block text-xs font-medium mb-1 text-gray-500 dark:text-gray-400">End column</label>
                  <select className="input-field text-sm" value={endCol} onChange={(e) => setEndCol(e.target.value)}>
                    {columns.map((c) => <option key={c} value={c}>{c}</option>)}
                  </select>
                </div>
              </>
            )}
          </div>
        )}
      </div>

      {/* Entity type selector */}
      <div className="flex gap-3 mb-4 items-center max-w-3xl">
        <label className="text-sm font-medium">Entity type:</label>
        <select className="input-field w-auto" value={entityType}
          onChange={(e) => setEntityType(e.target.value)}>
          {ENTITY_TYPES.map((t) => <option key={t} value={t}>{t}</option>)}
        </select>
        <span className="text-xs text-gray-400">Highlight text below to label it</span>
      </div>

      {(loading || preLoading) && <p className="text-sm text-gray-500 dark:text-gray-400 animate-pulse">Loading documents...</p>}

      {/* Mode A: Unlabeled */}
      {mode === "unlabeled" && unlabeledDoc && (
        <div className="max-w-3xl">
          <div className="flex items-center justify-between mb-3">
            <div className="text-sm text-gray-500 dark:text-gray-400">
              Document {currentIdx + 1} of {docs.length} -- <span className="font-mono text-xs">{String(unlabeledDoc.doc_id)}</span>
            </div>
            <div className="flex gap-2">
              <button className="btn-ghost text-xs" disabled={currentIdx === 0 || saving}
                onClick={() => { if (!confirmDiscardIfDirty()) return; setCurrentIdx((i) => i - 1); setLabels([]); setIsDirty(false); }}>Prev</button>
              <button className="btn-ghost text-xs" disabled={currentIdx >= docs.length - 1 || saving}
                onClick={() => { if (!confirmDiscardIfDirty()) return; setCurrentIdx((i) => i + 1); setLabels([]); setIsDirty(false); }}>Next</button>
            </div>
          </div>

          <div ref={textContainerRef} className="card p-5 mb-4 cursor-text select-text" onMouseUp={handleTextSelect}>
            <EntityHighlighter
              text={String(unlabeledDoc.text || "")}
              entities={labels.map((l) => ({
                entity: l.entity_text, entity_type: l.entity_type,
                start: l.start, end: l.end,
              }))}
              showIndices
            />
          </div>

          {labels.length > 0 && <LabelChips labels={labels} onRemove={(i) => { setLabels((p) => p.filter((_, j) => j !== i)); setIsDirty(true); }} />}

          <div className="flex gap-2">
            <button className="btn-primary" disabled={labels.length === 0 || saving} onClick={saveUnlabeled}>
              {saving ? "Saving..." : "Save & Next"}
            </button>
            <button className="btn-ghost border border-gray-200 dark:border-gray-600" disabled={saving}
              onClick={() => { if (!confirmDiscardIfDirty()) return; setCurrentIdx((i) => Math.min(i + 1, docs.length - 1)); setLabels([]); setIsDirty(false); }}>
              Skip
            </button>
          </div>
        </div>
      )}

      {/* Mode B: Pre-labeled */}
      {mode === "prelabeled" && preDoc && (
        <div className="max-w-3xl">
          <div className="flex items-center justify-between mb-3">
            <div className="text-sm text-gray-500 dark:text-gray-400">
              Document {preIdx + 1} of {preDocs.length} -- <span className="font-mono text-xs">{preDoc.doc_id}</span>
            </div>
            <div className="flex gap-2">
              <button className="btn-ghost text-xs" disabled={preIdx === 0 || saving}
                onClick={() => { if (!confirmDiscardIfDirty()) return; setPreIdx((i) => i - 1); setIsDirty(false); }}>Prev</button>
              <button className="btn-ghost text-xs" disabled={preIdx >= preDocs.length - 1 || saving}
                onClick={() => { if (!confirmDiscardIfDirty()) return; setPreIdx((i) => i + 1); setIsDirty(false); }}>Next</button>
            </div>
          </div>

          <div ref={textContainerRef} className="card p-5 mb-4 cursor-text select-text" onMouseUp={handleTextSelect}>
            <EntityHighlighter
              text={preDoc.text}
              entities={editLabels.map((l) => ({
                entity: l.entity_text, entity_type: l.entity_type,
                start: l.start, end: l.end,
              }))}
              showIndices
            />
          </div>

          {editLabels.length > 0 && (
            <LabelChips
              labels={editLabels}
              onRemove={(i) => { setEditLabels((p) => p.filter((_, j) => j !== i)); setIsDirty(true); }}
              editable
              onChangeType={(i, t) => { setEditLabels((p) => p.map((l, j) => j === i ? { ...l, entity_type: t } : l)); setIsDirty(true); }}
            />
          )}

          <div className="flex gap-2">
            <button className="btn-primary" disabled={saving} onClick={savePreLabeled}>
              {saving ? "Saving..." : "Save & Next"}
            </button>
            <button className="btn-ghost border border-gray-200 dark:border-gray-600" disabled={saving}
              onClick={() => { if (!confirmDiscardIfDirty()) return; setPreIdx((i) => Math.min(i + 1, preDocs.length - 1)); setIsDirty(false); }}>
              Skip
            </button>
          </div>
        </div>
      )}

      {mode === "unlabeled" && hasTable && !loading && docs.length === 0 && (
        <p className="text-sm text-gray-400">All documents have been labeled, or the table is empty.</p>
      )}
      {mode === "prelabeled" && hasTable && !preLoading && preDocs.length === 0 && (
        <p className="text-sm text-gray-400">No documents found in the selected table.</p>
      )}
    </div>
  );
}

function LabelChips({
  labels,
  onRemove,
  editable = false,
  onChangeType,
}: {
  labels: LabelEntry[];
  onRemove: (idx: number) => void;
  editable?: boolean;
  onChangeType?: (idx: number, newType: string) => void;
}) {
  return (
    <div className="mb-4">
      <h4 className="text-sm font-medium mb-2">Labels ({labels.length})</h4>
      <div className="flex flex-wrap gap-2">
        {labels.map((l, i) => (
          <span key={i} className="bg-amber-100 dark:bg-amber-900/30 text-amber-800 dark:text-amber-200 text-xs px-2.5 py-1 rounded-full inline-flex items-center gap-1">
            <sup className="text-[9px] font-bold opacity-70">{i + 1}</sup>
            {l.entity_text}
            {editable && onChangeType ? (
              <select className="text-[10px] bg-transparent border-none outline-none cursor-pointer ml-1" value={l.entity_type}
                onChange={(e) => onChangeType(i, e.target.value)}>
                {["PERSON", "PHONE_NUMBER", "EMAIL_ADDRESS", "LOCATION", "DATE_TIME",
                  "US_SSN", "ADDRESS", "MEDICAL_RECORD_NUMBER", "IP_ADDRESS", "URL",
                  "HOSPITAL_NAME", "ORGANIZATION", "ID_NUMBER", "OTHER"].map((t) => (
                  <option key={t} value={t}>{t}</option>
                ))}
              </select>
            ) : (
              <span className="opacity-60">[{l.entity_type}]</span>
            )}
            <button className="ml-0.5 text-red-500 hover:text-red-700" onClick={() => onRemove(i)}>x</button>
          </span>
        ))}
      </div>
    </div>
  );
}
