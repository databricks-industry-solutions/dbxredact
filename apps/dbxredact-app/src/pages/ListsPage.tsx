import { useState, useEffect } from "react";
import { useGet, apiPost, apiDelete } from "../hooks/useApi";
import type { ListEntry } from "../types";
import TablePicker from "../components/TablePicker";

const ENTITY_TYPES = [
  "", "PERSON", "PHONE_NUMBER", "EMAIL_ADDRESS", "LOCATION", "DATE_TIME",
  "US_SSN", "ADDRESS", "MEDICAL_RECORD_NUMBER", "IP_ADDRESS", "URL", "OTHER",
];

export default function ListsPage() {
  const { data: denyList, refetch: refetchDeny } = useGet<ListEntry[]>("/lists/deny");
  const { data: allowList, refetch: refetchAllow } = useGet<ListEntry[]>("/lists/allow");

  const [value, setValue] = useState("");
  const [isPattern, setIsPattern] = useState(false);
  const [entityType, setEntityType] = useState("");
  const [tab, setTab] = useState<"deny" | "allow">("deny");

  async function add() {
    await apiPost(`/lists/${tab}`, { value, is_pattern: isPattern, entity_type: entityType || null });
    setValue("");
    setEntityType("");
    setIsPattern(false);
    tab === "deny" ? refetchDeny() : refetchAllow();
  }

  async function remove(id: string, type: "deny" | "allow") {
    await apiDelete(`/lists/${type}/${id}`);
    type === "deny" ? refetchDeny() : refetchAllow();
  }

  const list = tab === "deny" ? denyList : allowList;

  return (
    <div>
      <h2 className="page-title">Deny / Allow Lists</h2>
      <p className="page-desc">
        Control detection behavior with static lists. <b>Deny list</b> entries force the pipeline to always flag
        matching text as PII, even if detectors miss it. <b>Allow list</b> entries suppress false positives --
        matching text will never be redacted. Use regex patterns for flexible matching (e.g. company names, product codes).
      </p>

      <div className="card p-4 mb-6">
        <div className="text-xs text-gray-500 dark:text-gray-400 mb-3 leading-relaxed">
          <b>How to use:</b> After reviewing detection results, add entries here for corrections that should apply globally.
          For example, if "Acme Corp" keeps being flagged as a PERSON, add it to the Allow list.
          If a known SSN format is being missed, add a regex pattern to the Deny list.
        </div>
      </div>

      <div className="flex gap-2 mb-5">
        <button className={`px-4 py-2 rounded-lg text-sm font-medium transition-colors ${
          tab === "deny" ? "bg-red-600 text-white shadow-sm" : "bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-300 hover:bg-gray-200 dark:hover:bg-gray-600"
        }`} onClick={() => setTab("deny")}>Deny List ({denyList?.length || 0})</button>
        <button className={`px-4 py-2 rounded-lg text-sm font-medium transition-colors ${
          tab === "allow" ? "bg-emerald-600 text-white shadow-sm" : "bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-300 hover:bg-gray-200 dark:hover:bg-gray-600"
        }`} onClick={() => setTab("allow")}>Allow List ({allowList?.length || 0})</button>
      </div>

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
            {ENTITY_TYPES.filter(Boolean).map(t => <option key={t} value={t}>{t}</option>)}
          </select>
        </div>
        <label className="flex items-center gap-1.5 text-sm pb-2.5 cursor-pointer">
          <input type="checkbox" className="rounded" checked={isPattern} onChange={(e) => setIsPattern(e.target.checked)} /> Regex
        </label>
        <button className="btn-primary" disabled={!value.trim()} onClick={add}>Add</button>
      </div>

      {list?.length ? (
        <table className="data-table">
          <thead>
            <tr><th>Value</th><th>Type</th><th>Entity Type</th><th></th></tr>
          </thead>
          <tbody>
            {list.map((e) => (
              <tr key={e.entry_id}>
                <td className="font-mono text-xs">{e.value}</td>
                <td>{e.is_pattern
                  ? <span className="text-xs px-2 py-0.5 rounded-full bg-purple-100 dark:bg-purple-900/30 text-purple-700 dark:text-purple-300">regex</span>
                  : <span className="text-xs px-2 py-0.5 rounded-full bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-300">exact</span>
                }</td>
                <td>{e.entity_type || <span className="text-gray-400">any</span>}</td>
                <td>
                  <button className="text-red-500 dark:text-red-400 hover:text-red-700 text-xs font-medium transition-colors"
                    onClick={() => remove(e.entry_id!, tab)}>Remove</button>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      ) : (
        <p className="text-sm text-gray-400">No {tab} list entries yet. Add entries above to get started.</p>
      )}

      <SuggestionsSection
        onApprove={(val, listType) => {
          apiPost(`/lists/${listType}`, { value: val, is_pattern: false, entity_type: null })
            .then(() => { refetchDeny(); refetchAllow(); });
        }}
      />
    </div>
  );
}

function SuggestionsSection({ onApprove }: { onApprove: (value: string, listType: "deny" | "allow") => void }) {
  const [sourceTable, setSourceTable] = useState("");
  const [suggestions, setSuggestions] = useState<any[] | null>(null);
  const [dismissed, setDismissed] = useState<Set<number>>(new Set());
  const [loading, setLoading] = useState(false);

  const parts = sourceTable.split(".");
  const hasTable = parts.length === 3 && parts[2] !== "";

  useEffect(() => {
    if (!hasTable) { setSuggestions(null); return; }
    setLoading(true);
    setDismissed(new Set());
    const recsTable = `${sourceTable}_recommendations`;
    fetch(`/api/metrics/recommendations-for-lists?recs_table=${encodeURIComponent(recsTable)}`)
      .then(r => r.json()).then(setSuggestions)
      .catch(() => setSuggestions(null))
      .finally(() => setLoading(false));
  }, [sourceTable, hasTable]);

  const visible = suggestions?.filter((_, i) => !dismissed.has(i));

  return (
    <div className="mt-8 pt-6 border-t border-gray-200 dark:border-gray-700">
      <h3 className="text-lg font-semibold mb-2">AI Suggestions</h3>
      <p className="text-xs text-gray-500 dark:text-gray-400 mb-4">
        Select a benchmark source table to see AI-generated recommendations for deny/allow list changes.
      </p>
      <div className="max-w-xl mb-4">
        <TablePicker value={sourceTable} onChange={setSourceTable} label="Benchmark Source Table" />
      </div>
      {loading && <p className="text-xs text-gray-400 animate-pulse">Loading suggestions...</p>}
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
                  onClick={() => { onApprove(s.action, "deny"); setDismissed(prev => new Set(prev).add(i)); }}>Add to Deny</button>
                <button className="text-xs font-medium px-2.5 py-1 rounded-lg bg-emerald-50 dark:bg-emerald-900/20 text-emerald-700 dark:text-emerald-300 hover:bg-emerald-100 transition-colors"
                  onClick={() => { onApprove(s.action, "allow"); setDismissed(prev => new Set(prev).add(i)); }}>Add to Allow</button>
                <button className="text-xs font-medium px-2.5 py-1 rounded-lg bg-gray-100 dark:bg-gray-700 text-gray-500 dark:text-gray-400 hover:bg-gray-200 transition-colors"
                  onClick={() => setDismissed(prev => new Set(prev).add(i))}>Dismiss</button>
              </div>
            </div>
          ))}
        </div>
      ) : (hasTable && !loading && <p className="text-xs text-gray-400">No deny/allow suggestions found in this benchmark's recommendations.</p>)}
    </div>
  );
}
