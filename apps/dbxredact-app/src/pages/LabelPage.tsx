import { useState, useEffect, useRef } from "react";
import TablePicker, { type TableRef, emptyTableRef, toQualified, isComplete } from "../components/TablePicker";
import EntityHighlighter from "../components/EntityHighlighter";
import ErrorBanner from "../components/ErrorBanner";
import Tabs from "../components/Tabs";
import { useGet, apiPost } from "../hooks/useApi";
import { useToast } from "../hooks/useToast";
import { ENTITY_TYPES } from "../constants";

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

type Mode = "unlabeled" | "prelabeled";

export default function LabelPage() {
  const [mode, setMode] = useState<Mode>("unlabeled");
  const [table, setTable] = useState<TableRef>(emptyTableRef);
  const [textCol, setTextCol] = useState("text");
  const [docIdCol, setDocIdCol] = useState("doc_id");
  const [error, setError] = useState("");

  const [entityTextCol, setEntityTextCol] = useState("entity_text");
  const [entityTypeCol, setEntityTypeCol] = useState("entity_type");
  const [startCol, setStartCol] = useState("start");
  const [endCol, setEndCol] = useState("end");

  const [docs, setDocs] = useState<Record<string, unknown>[]>([]);
  const [currentIdx, setCurrentIdx] = useState(0);
  const [labels, setLabels] = useState<LabelEntry[]>([]);
  const [entityType, setEntityType] = useState("PERSON");

  const [preDocs, setPreDocs] = useState<PreLabeledDoc[]>([]);
  const [preIdx, setPreIdx] = useState(0);
  const [editLabels, setEditLabels] = useState<LabelEntry[]>([]);

  const [isDirty, setIsDirty] = useState(false);
  const [saving, setSaving] = useState(false);
  const { toast } = useToast();

  const textContainerRef = useRef<HTMLDivElement>(null);

  const hasTable = isComplete(table);
  const qualified = toQualified(table);

  const { data: tableInfo } = useGet<TableInfo>(
    `/pipeline/table-info?table=${encodeURIComponent(qualified)}`,
    { enabled: hasTable, deps: [qualified] },
  );
  const columns = tableInfo?.columns ?? [];

  useEffect(() => {
    if (!tableInfo) return;
    const cols = tableInfo.columns;
    if (cols.includes("text")) setTextCol("text");
    else if (cols.length) setTextCol(cols[0]);
    if (cols.includes("doc_id")) setDocIdCol("doc_id");
    if (cols.includes("entity_text")) setEntityTextCol("entity_text");
    if (cols.includes("entity_type")) setEntityTypeCol("entity_type");
    if (cols.includes("start")) setStartCol("start");
    if (cols.includes("end")) setEndCol("end");
  }, [tableInfo]);

  const unlabeledParams = new URLSearchParams({
    source_table: qualified, text_column: textCol, doc_id_column: docIdCol,
  }).toString();

  const { data: unlabeledDocs, loading } = useGet<Record<string, unknown>[]>(
    `/labels/documents?${unlabeledParams}`,
    { enabled: mode === "unlabeled" && hasTable, deps: [qualified, textCol, docIdCol, mode] },
  );

  useEffect(() => {
    if (unlabeledDocs) { setDocs(unlabeledDocs); setCurrentIdx(0); setLabels([]); }
    else setDocs([]);
  }, [unlabeledDocs]);

  const preParams = new URLSearchParams({
    source_table: qualified, text_column: textCol, doc_id_column: docIdCol,
    entity_text_column: entityTextCol, entity_type_column: entityTypeCol,
    start_column: startCol, end_column: endCol,
  }).toString();

  const { data: preLabeledDocs, loading: preLoading } = useGet<PreLabeledDoc[]>(
    `/labels/documents-with-labels?${preParams}`,
    { enabled: mode === "prelabeled" && hasTable, deps: [qualified, textCol, docIdCol, entityTextCol, entityTypeCol, startCol, endCol, mode] },
  );

  useEffect(() => {
    if (preLabeledDocs) {
      setPreDocs(preLabeledDocs);
      setPreIdx(0);
      if (preLabeledDocs.length) setEditLabels(preLabeledDocs[0].labels);
    } else {
      setPreDocs([]);
    }
  }, [preLabeledDocs]);

  useEffect(() => {
    if (mode === "prelabeled" && preDocs[preIdx]) {
      setEditLabels(preDocs[preIdx].labels);
    }
  }, [preIdx, preDocs, mode]);

  const { data: stats, refetch: refetchStats } = useGet<{ total_docs: number; labeled_docs: number }>(
    `/labels/stats?source_table=${encodeURIComponent(qualified)}`,
    { enabled: hasTable, deps: [qualified] },
  );

  function getTextOffsetInContainer(container: Node, targetNode: Node, targetOffset: number): number {
    let offset = 0;
    const walker = document.createTreeWalker(container, NodeFilter.SHOW_TEXT);
    let node: Node | null;
    while ((node = walker.nextNode())) {
      if (node.parentElement?.tagName === "SUP") continue;
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
      source_table: qualified,
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
      await apiPost("/labels/batch", toBatchBody(doc.doc_id as string, labels));
      toast("Labels saved");
      setLabels([]);
      setIsDirty(false);
      refetchStats();
      setCurrentIdx((i) => Math.min(i + 1, docs.length - 1));
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : "Failed to save labels");
    } finally {
      setSaving(false);
    }
  }

  async function savePreLabeled() {
    const doc = preDocs[preIdx];
    setSaving(true);
    try {
      await apiPost("/labels/batch", toBatchBody(doc.doc_id, editLabels));
      toast("Labels saved");
      setIsDirty(false);
      refetchStats();
      setPreIdx((i) => Math.min(i + 1, preDocs.length - 1));
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : "Failed to save labels");
    } finally {
      setSaving(false);
    }
  }

  function confirmDiscardIfDirty(): boolean {
    if (!isDirty) return true;
    return window.confirm("You have unsaved labels. Discard changes?");
  }

  useEffect(() => {
    function handleKeys(e: KeyboardEvent) {
      if ((e.metaKey || e.ctrlKey) && e.key === "s") {
        e.preventDefault();
        if (mode === "unlabeled" && labels.length > 0 && !saving) saveUnlabeled();
        else if (mode === "prelabeled" && !saving) savePreLabeled();
      }
      if (e.key === "ArrowLeft" && e.altKey) {
        e.preventDefault();
        if (mode === "unlabeled") { if (!confirmDiscardIfDirty()) return; setCurrentIdx((i) => Math.max(0, i - 1)); setLabels([]); setIsDirty(false); }
        else { if (!confirmDiscardIfDirty()) return; setPreIdx((i) => Math.max(0, i - 1)); setIsDirty(false); }
      }
      if (e.key === "ArrowRight" && e.altKey) {
        e.preventDefault();
        if (mode === "unlabeled") { if (!confirmDiscardIfDirty()) return; setCurrentIdx((i) => Math.min(i + 1, docs.length - 1)); setLabels([]); setIsDirty(false); }
        else { if (!confirmDiscardIfDirty()) return; setPreIdx((i) => Math.min(i + 1, preDocs.length - 1)); setIsDirty(false); }
      }
    }
    window.addEventListener("keydown", handleKeys);
    return () => window.removeEventListener("keydown", handleKeys);
  });

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

      <Tabs
        variant="pill"
        tabs={[
          { key: "unlabeled", label: "Label Unlabeled Data" },
          { key: "prelabeled", label: "Edit Pre-Labeled Data" },
        ]}
        active={mode}
        onChange={(k) => setMode(k as Mode)}
      />

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

      <div className="flex gap-3 mb-4 items-center max-w-3xl">
        <label className="text-sm font-medium">Entity type:</label>
        <select className="input-field w-auto" value={entityType}
          onChange={(e) => setEntityType(e.target.value)}>
          {ENTITY_TYPES.map((t) => <option key={t} value={t}>{t}</option>)}
        </select>
        <span className="text-xs text-gray-400">Highlight text (mouse or Shift+Arrow) to label it</span>
      </div>

      {(loading || preLoading) && <p className="text-sm text-gray-500 dark:text-gray-400 animate-pulse">Loading documents...</p>}

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

          <div ref={textContainerRef} className="card p-5 mb-4 cursor-text select-text" onMouseUp={handleTextSelect} onKeyUp={handleTextSelect}>
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

          <div className="flex gap-2 items-center">
            <button className="btn-primary" disabled={labels.length === 0 || saving} onClick={saveUnlabeled}>
              {saving ? "Saving..." : "Save & Next"}
            </button>
            <button className="btn-ghost border border-gray-200 dark:border-gray-600" disabled={saving}
              onClick={() => { if (!confirmDiscardIfDirty()) return; setCurrentIdx((i) => Math.min(i + 1, docs.length - 1)); setLabels([]); setIsDirty(false); }}>
              Skip
            </button>
          </div>
          <KeyboardLegend />
        </div>
      )}

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

          <div ref={textContainerRef} className="card p-5 mb-4 cursor-text select-text" onMouseUp={handleTextSelect} onKeyUp={handleTextSelect}>
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

          <div className="flex gap-2 items-center">
            <button className="btn-primary" disabled={saving} onClick={savePreLabeled}>
              {saving ? "Saving..." : "Save & Next"}
            </button>
            <button className="btn-ghost border border-gray-200 dark:border-gray-600" disabled={saving}
              onClick={() => { if (!confirmDiscardIfDirty()) return; setPreIdx((i) => Math.min(i + 1, preDocs.length - 1)); setIsDirty(false); }}>
              Skip
            </button>
          </div>
          <KeyboardLegend />
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

const isMac = typeof navigator !== "undefined" && navigator.platform.toUpperCase().includes("MAC");
const mod = isMac ? "\u2318" : "Ctrl";

function KeyboardLegend() {
  return (
    <div className="mt-3 flex flex-wrap gap-x-4 gap-y-1 text-[11px] text-gray-400 dark:text-gray-500">
      <span><kbd className="font-mono bg-gray-100 dark:bg-gray-700 px-1 rounded">{mod}+S</kbd> Save & Next</span>
      <span><kbd className="font-mono bg-gray-100 dark:bg-gray-700 px-1 rounded">Alt+\u2190</kbd> Prev</span>
      <span><kbd className="font-mono bg-gray-100 dark:bg-gray-700 px-1 rounded">Alt+\u2192</kbd> Next</span>
    </div>
  );
}

function LabelChips({
  labels,
  onRemove,
  editable = false,
  onChangeType,
}: {
  labels: { entity_text: string; entity_type: string; start: number; end: number }[];
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
                {ENTITY_TYPES.map((t) => <option key={t} value={t}>{t}</option>)}
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
