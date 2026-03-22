import { useState } from "react";
import { useGet, apiPost, apiDelete } from "../hooks/useApi";
import type { ListEntry } from "../types";
import TablePicker, { type TableRef, emptyTableRef, toQualified, isComplete } from "../components/TablePicker";
import ErrorBanner from "../components/ErrorBanner";
import ConfirmDialog from "../components/ConfirmDialog";
import DataTable, { type Column } from "../components/DataTable";
import Tabs from "../components/Tabs";
import { useToast } from "../hooks/useToast";
import { ENTITY_TYPES } from "../constants";

export default function ListsPage() {
  const { data: blockList, refetch: refetchBlock, error: blockError } = useGet<ListEntry[]>("/lists/block");
  const { data: safeList, refetch: refetchSafe, error: safeError } = useGet<ListEntry[]>("/lists/safe");

  const [value, setValue] = useState("");
  const [isPattern, setIsPattern] = useState(false);
  const [entityType, setEntityType] = useState("");
  const [tab, setTab] = useState<"block" | "safe">("block");
  const [error, setError] = useState("");
  const [removeTarget, setRemoveTarget] = useState<{ id: string; type: "block" | "safe"; value: string } | null>(null);
  const [flashId, setFlashId] = useState<string | null>(null);
  const { toast } = useToast();

  const displayError = error || blockError || safeError || "";

  async function add() {
    try {
      const created = await apiPost<ListEntry>(`/lists/${tab}`, { value, is_pattern: isPattern, entity_type: entityType || null });
      setValue("");
      setEntityType("");
      setIsPattern(false);
      toast(`Entry added to ${tab} list`);
      if (created?.entry_id) { setFlashId(created.entry_id); setTimeout(() => setFlashId(null), 2000); }
      tab === "block" ? refetchBlock() : refetchSafe();
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : "Failed to add entry");
    }
  }

  async function confirmRemove() {
    if (!removeTarget) return;
    try {
      await apiDelete(`/lists/${removeTarget.type}/${removeTarget.id}`);
      toast("Entry removed");
      removeTarget.type === "block" ? refetchBlock() : refetchSafe();
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : "Failed to remove entry");
    }
    setRemoveTarget(null);
  }

  const list = tab === "block" ? blockList : safeList;

  const listColumns: Column<ListEntry & Record<string, unknown>>[] = [
    { key: "value", header: "Value", render: (e) => <span className="font-mono text-xs">{e.value}</span> },
    { key: "is_pattern", header: "Type", sortable: false, searchable: false, render: (e) =>
      e.is_pattern
        ? <span className="text-xs px-2 py-0.5 rounded-full bg-purple-100 dark:bg-purple-900/30 text-purple-700 dark:text-purple-300">regex</span>
        : <span className="text-xs px-2 py-0.5 rounded-full bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-300">exact</span>
    },
    { key: "entity_type", header: "Entity Type", render: (e) => e.entity_type || <span className="text-gray-400">any</span> },
    { key: "_actions", header: "", sortable: false, searchable: false, render: (e) => (
      <button className="text-red-500 dark:text-red-400 hover:text-red-700 text-xs font-medium transition-colors"
        onClick={() => setRemoveTarget({ id: e.entry_id!, type: tab, value: e.value })}>Remove</button>
    )},
  ];

  return (
    <div>
      <ConfirmDialog
        open={!!removeTarget}
        title="Remove List Entry"
        message={`Remove "${removeTarget?.value}" from the ${removeTarget?.type} list?`}
        confirmLabel="Remove"
        variant="danger"
        onConfirm={confirmRemove}
        onCancel={() => setRemoveTarget(null)}
      />
      <h2 className="page-title">Block / Safe Lists</h2>
      <ErrorBanner message={displayError} onDismiss={() => setError("")} />
      <p className="page-desc">
        Control detection behavior with static lists. <b>Block list</b> entries force the pipeline to always
        flag matching text as PII, even if detectors miss it. <b>Safe list</b> entries suppress false positives --
        matching text will never be redacted. Use regex patterns for flexible matching (e.g. company names, product codes).
      </p>

      <div className="card p-4 mb-6">
        <div className="text-xs text-gray-500 dark:text-gray-400 mb-3 leading-relaxed">
          <b>How to use:</b> After reviewing detection results, add entries here for corrections that should apply globally.
          For example, if "Acme Corp" keeps being flagged as a PERSON, add it to the Safe list.
          If a known SSN format is being missed, add a regex pattern to the Block list.
        </div>
      </div>

      <Tabs
        variant="pill"
        tabs={[
          { key: "block", label: `Block List (${blockList?.length || 0})` },
          { key: "safe", label: `Safe List (${safeList?.length || 0})` },
        ]}
        active={tab}
        onChange={(k) => setTab(k as "block" | "safe")}
      />

      <div className="card p-4 mb-6 flex gap-3 items-end flex-wrap">
        <div className="flex-1 min-w-[200px]">
          <label className="block text-sm font-medium mb-1.5">Value</label>
          <input className="input-field" value={value}
            onChange={(e) => setValue(e.target.value)} placeholder={isPattern ? "\\b\\d{3}-\\d{2}-\\d{4}\\b" : "John Smith"} />
        </div>
        <div className="w-44">
          <label className="block text-sm font-medium mb-1.5">Entity Type</label>
          <select className="input-field" value={entityType} onChange={(e) => setEntityType(e.target.value)}>
            <option value="">Any type</option>
            {ENTITY_TYPES.map(t => <option key={t} value={t}>{t}</option>)}
          </select>
        </div>
        <label className="flex items-center gap-1.5 text-sm pb-2.5 cursor-pointer">
          <input type="checkbox" className="rounded" checked={isPattern} onChange={(e) => setIsPattern(e.target.checked)} /> Regex
        </label>
        <button className="btn-primary" disabled={!value.trim()} onClick={add}>Add</button>
      </div>

      <DataTable<ListEntry & Record<string, unknown>>
        data={(list ?? []) as (ListEntry & Record<string, unknown>)[]}
        rowKey={(e) => e.entry_id ?? ""}
        rowClassName={(e) => flashId === e.entry_id ? "flash-row" : ""}
        emptyMessage={`No ${tab} list entries yet. Add entries above to get started.`}
        columns={listColumns}
      />

      <SuggestionsSection
        onApprove={async (val, listType) => {
          await apiPost(`/lists/${listType}`, { value: val, is_pattern: false, entity_type: null });
          refetchBlock(); refetchSafe();
          toast(`Added to ${listType} list`);
        }}
      />
    </div>
  );
}

function SuggestionsSection({ onApprove }: { onApprove: (value: string, listType: "block" | "safe") => Promise<void> }) {
  const [sourceTable, setSourceTable] = useState<TableRef>(emptyTableRef);
  const [dismissed, setDismissed] = useState<Set<number>>(new Set());
  const [approveError, setApproveError] = useState("");

  const hasTable = isComplete(sourceTable);
  const recsTable = `${toQualified(sourceTable)}_recommendations`;

  const { data: suggestions, loading } = useGet<{ action: string; rationale?: string }[]>(
    `/metrics/recommendations-for-lists?recs_table=${encodeURIComponent(recsTable)}`,
    { enabled: hasTable, deps: [recsTable] },
  );

  const visible = suggestions?.filter((_, i) => !dismissed.has(i));

  return (
    <div className="mt-8 pt-6 border-t border-gray-200 dark:border-gray-700">
      <h3 className="text-lg font-semibold mb-2">AI Suggestions</h3>
      <p className="text-xs text-gray-500 dark:text-gray-400 mb-4">
        Select a benchmark source table to see AI-generated recommendations for block/safe list changes.
      </p>
      <div className="max-w-xl mb-4">
        <TablePicker value={sourceTable} onChange={(v) => { setSourceTable(v); setDismissed(new Set()); }} label="Benchmark Source Table" />
      </div>
      {loading && <p className="text-xs text-gray-400 animate-pulse">Loading suggestions...</p>}
      {approveError && (
        <div className="text-xs text-red-600 dark:text-red-400 mb-2">{approveError}</div>
      )}
      {visible?.length ? (
        <div className="space-y-3">
          {suggestions!.map((s, i) => dismissed.has(i) ? null : (
            <div key={i} className="card p-4 flex items-start gap-3">
              <div className="flex-1">
                <p className="text-sm font-medium">{s.action}</p>
                {s.rationale && <p className="text-xs text-gray-500 dark:text-gray-400 mt-1">{s.rationale}</p>}
              </div>
              <div className="flex gap-2 shrink-0">
                <button className="text-xs font-medium px-2.5 py-1 rounded-lg bg-red-50 dark:bg-red-900/20 text-red-700 dark:text-red-300 hover:bg-red-100 transition-colors"
                  onClick={async () => {
                    try { await onApprove(s.action, "block"); setDismissed(prev => new Set(prev).add(i)); }
                    catch (e) { setApproveError(e instanceof Error ? e.message : "Failed to add to block list"); }
                  }}>Add to Block</button>
                <button className="text-xs font-medium px-2.5 py-1 rounded-lg bg-emerald-50 dark:bg-emerald-900/20 text-emerald-700 dark:text-emerald-300 hover:bg-emerald-100 transition-colors"
                  onClick={async () => {
                    try { await onApprove(s.action, "safe"); setDismissed(prev => new Set(prev).add(i)); }
                    catch (e) { setApproveError(e instanceof Error ? e.message : "Failed to add to safe list"); }
                  }}>Add to Safe</button>
                <button className="text-xs font-medium px-2.5 py-1 rounded-lg bg-gray-100 dark:bg-gray-700 text-gray-500 dark:text-gray-400 hover:bg-gray-200 transition-colors"
                  onClick={() => setDismissed(prev => new Set(prev).add(i))}>Dismiss</button>
              </div>
            </div>
          ))}
        </div>
      ) : (hasTable && !loading && <p className="text-xs text-gray-400">No block/safe suggestions found in this benchmark's recommendations.</p>)}
    </div>
  );
}
