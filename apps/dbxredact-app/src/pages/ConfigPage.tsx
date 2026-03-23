import { useState, useEffect } from "react";
import { useGet, apiPost, apiPut, apiDelete } from "../hooks/useApi";
import ErrorBanner from "../components/ErrorBanner";
import ConfirmDialog from "../components/ConfirmDialog";
import DataTable, { type Column } from "../components/DataTable";
import { useToast } from "../hooks/useToast";
import type { Config } from "../types";

const PROFILE_PRESETS: Record<string, Partial<typeof DEFAULTS>> = {
  fast: {
    use_presidio: true, use_ai_query: true, use_gliner: true,
    reasoning_effort: "low", gliner_max_words: 256, presidio_model_size: "lg",
    presidio_pattern_only: true,
  },
  deep: {
    use_presidio: true, use_ai_query: true, use_gliner: true,
    reasoning_effort: "medium", gliner_max_words: 256, presidio_model_size: "trf",
    presidio_pattern_only: false,
  },
};

const PROFILE_DESCRIPTIONS: Record<string, string> = {
  fast: "AI Query + GLiNER + Presidio (pattern-only). Highest accuracy (F1~0.86) and precision (P~0.92). Pattern-only Presidio adds deterministic regex backup (SSN, phone, MRN, dates) without spaCy. Best for routine redaction and large-scale batch jobs.",
  deep: "All three detectors with fine-grained GLiNER chunking and medium LLM reasoning. Maximum recall (R~0.95) for compliance-critical workloads. Slower and more expensive.",
  custom: "Configure detection methods and parameters manually.",
};

const DEFAULTS = {
  name: "default",
  detection_profile: "fast",
  use_presidio: true,
  use_ai_query: true,
  use_gliner: true,
  endpoint: "databricks-gpt-oss-120b",
  score_threshold: 0.5,
  gliner_model: "nvidia/gliner-PII",
  gliner_threshold: 0.2,
  redaction_strategy: "typed",
  alignment_mode: "union",
  reasoning_effort: "low",
  gliner_max_words: 256,
  presidio_model_size: "trf",
  presidio_pattern_only: true,
};

export default function ConfigPage() {
  const { data: configs, loading, refetch, error: fetchError } = useGet<Config[]>("/config/");
  const [form, setForm] = useState(DEFAULTS);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState("");
  const [showAdvanced, setShowAdvanced] = useState(false);
  const [deleteTarget, setDeleteTarget] = useState<Config | null>(null);
  const [editingId, setEditingId] = useState<string | null>(null);
  const { toast } = useToast();

  useEffect(() => { if (fetchError) setError(fetchError); }, [fetchError]);

  const isPreset = form.detection_profile !== "custom";
  const set = (k: string, v: unknown) => setForm((f) => ({ ...f, [k]: v }));

  function setProfile(profile: string) {
    const preset = PROFILE_PRESETS[profile];
    if (preset) {
      setForm((f) => ({ ...f, detection_profile: profile, ...preset }));
    } else {
      set("detection_profile", profile);
    }
  }

  async function save() {
    setSaving(true);
    try {
      if (editingId) {
        await apiPut(`/config/${editingId}`, form);
        toast("Config updated");
      } else {
        await apiPost("/config/", form);
        toast("Config saved");
      }
      setForm(DEFAULTS);
      setEditingId(null);
      refetch();
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : "Failed to save config");
    }
    setSaving(false);
  }

  function startEdit(c: Config) {
    setEditingId(c.config_id);
    setForm({
      name: c.name,
      detection_profile: c.detection_profile || "custom",
      use_presidio: c.use_presidio,
      use_ai_query: c.use_ai_query,
      use_gliner: c.use_gliner,
      endpoint: c.endpoint,
      score_threshold: c.score_threshold,
      gliner_model: c.gliner_model,
      gliner_threshold: c.gliner_threshold,
      redaction_strategy: c.redaction_strategy,
      alignment_mode: c.alignment_mode,
      reasoning_effort: c.reasoning_effort || "low",
      gliner_max_words: c.gliner_max_words || 256,
      presidio_model_size: c.presidio_model_size || "trf",
      presidio_pattern_only: c.presidio_pattern_only ?? true,
    });
    setShowAdvanced(true);
  }

  function cancelEdit() {
    setEditingId(null);
    setForm(DEFAULTS);
  }

  async function confirmDelete() {
    if (!deleteTarget) return;
    try {
      await apiDelete(`/config/${deleteTarget.config_id}`);
      toast("Config deleted");
      refetch();
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : "Failed to delete config");
    }
    setDeleteTarget(null);
  }

  const configColumns: Column<Config & Record<string, unknown>>[] = [
    { key: "name", header: "Name", render: (c) => <span className="font-medium">{c.name}</span> },
    { key: "detection_profile", header: "Profile", render: (c) => (
      <span className="inline-block px-2 py-0.5 text-xs rounded-full bg-purple-100 dark:bg-purple-900/30 text-purple-700 dark:text-purple-300">{c.detection_profile || "custom"}</span>
    )},
    { key: "use_presidio", header: "Presidio", render: (c) => c.use_presidio ? "Y" : "-", sortable: false, searchable: false },
    { key: "use_ai_query", header: "AI", render: (c) => c.use_ai_query ? "Y" : "-", sortable: false, searchable: false },
    { key: "use_gliner", header: "GLiNER", render: (c) => c.use_gliner ? "Y" : "-", sortable: false, searchable: false },
    { key: "redaction_strategy", header: "Strategy", render: (c) => (
      <span className="inline-block px-2 py-0.5 text-xs rounded-full bg-blue-100 dark:bg-blue-900/30 text-blue-700 dark:text-blue-300">{c.redaction_strategy}</span>
    )},
    { key: "alignment_mode", header: "Mode" },
    { key: "_actions", header: "", sortable: false, searchable: false, render: (c) => (
      <span className="space-x-2">
        <button className="text-blue-500 dark:text-blue-400 hover:text-blue-700 text-xs font-medium transition-colors" onClick={() => startEdit(c)}>Edit</button>
        <button className="text-red-500 dark:text-red-400 hover:text-red-700 text-xs font-medium transition-colors" onClick={() => setDeleteTarget(c)}>Delete</button>
      </span>
    )},
  ];

  return (
    <div>
      <ConfirmDialog
        open={!!deleteTarget}
        title="Delete Configuration"
        message={`Are you sure you want to delete "${deleteTarget?.name}"? This cannot be undone.`}
        confirmLabel="Delete"
        variant="danger"
        onConfirm={confirmDelete}
        onCancel={() => setDeleteTarget(null)}
      />
      <ErrorBanner message={error} onDismiss={() => setError("")} />
      <h2 className="page-title">Detection Configurations</h2>
      <p className="page-desc">Create and manage PII detection configurations for pipeline runs.</p>

      <div className="card p-5 mb-8 grid grid-cols-2 gap-4 max-w-2xl">
        <div className="col-span-2">
          <label className="block text-sm font-medium mb-1.5">Name</label>
          <input className="input-field" value={form.name}
            onChange={(e) => set("name", e.target.value)} />
        </div>

        {/* Profile selector */}
        <div className="col-span-2">
          <label className="block text-sm font-medium mb-1.5">Detection Profile</label>
          <select className="input-field" value={form.detection_profile}
            onChange={(e) => setProfile(e.target.value)}>
            <option value="fast">Fast Mode</option>
            <option value="deep">Deep Search</option>
            <option value="custom">Custom</option>
          </select>
          <p className="text-xs text-gray-500 dark:text-gray-400 mt-1">
            {PROFILE_DESCRIPTIONS[form.detection_profile]}
          </p>
        </div>

        {/* Detector toggles -- read-only for presets */}
        <div className="col-span-2 flex gap-6 py-1">
          {[
            { key: "use_presidio", label: "Presidio" },
            { key: "use_ai_query", label: "AI Query" },
            { key: "use_gliner", label: "GLiNER" },
          ].map(({ key, label }) => (
            <label key={key} className={`flex items-center gap-2 text-sm ${isPreset ? "opacity-60" : "cursor-pointer"}`}>
              <input type="checkbox" className="rounded" checked={form[key as keyof typeof form] as boolean}
                disabled={isPreset}
                onChange={(e) => set(key, e.target.checked)} />
              {label}
            </label>
          ))}
        </div>

        <div>
          <label className="block text-sm font-medium mb-1.5">Endpoint</label>
          <input className="input-field" value={form.endpoint}
            onChange={(e) => set("endpoint", e.target.value)} />
        </div>
        <div>
          <label className="block text-sm font-medium mb-1.5">Score Threshold</label>
          <input type="number" step={0.05} min={0} max={1} className="input-field"
            value={form.score_threshold} onChange={(e) => set("score_threshold", parseFloat(e.target.value))} />
        </div>
        <div>
          <label className="block text-sm font-medium mb-1.5">Redaction Strategy</label>
          <select className="input-field" value={form.redaction_strategy}
            onChange={(e) => set("redaction_strategy", e.target.value)}>
            <option value="generic">Generic</option>
            <option value="typed">Typed</option>
          </select>
        </div>
        <div>
          <label className="block text-sm font-medium mb-1.5">Alignment Mode</label>
          <select className="input-field" value={form.alignment_mode}
            onChange={(e) => set("alignment_mode", e.target.value)}>
            <option value="union">Union</option>
            <option value="consensus">Consensus</option>
          </select>
        </div>

        {/* Advanced settings */}
        <div className="col-span-2 border-t pt-3 mt-1">
          <button type="button"
            className="text-sm text-blue-600 dark:text-blue-400 hover:underline"
            onClick={() => setShowAdvanced(!showAdvanced)}>
            {showAdvanced ? "Hide" : "Show"} Advanced Settings
          </button>
        </div>
        {showAdvanced && (
          <>
            <div>
              <label className="block text-sm font-medium mb-1.5">Reasoning Effort</label>
              <select className="input-field" value={form.reasoning_effort}
                disabled={isPreset}
                onChange={(e) => set("reasoning_effort", e.target.value)}>
                <option value="low">Low</option>
                <option value="medium">Medium</option>
                <option value="high">High</option>
              </select>
            </div>
            <div>
              <label className="block text-sm font-medium mb-1.5">GLiNER Max Words</label>
              <input type="number" step={64} min={128} max={1024} className="input-field"
                value={form.gliner_max_words}
                disabled={isPreset}
                onChange={(e) => set("gliner_max_words", parseInt(e.target.value))} />
            </div>
            <div>
              <label className="block text-sm font-medium mb-1.5">Presidio Model</label>
              <select className="input-field" value={form.presidio_model_size}
                disabled={isPreset}
                onChange={(e) => set("presidio_model_size", e.target.value)}>
                <option value="trf">en_core_web_trf (best accuracy)</option>
                <option value="lg">en_core_web_lg (faster)</option>
              </select>
            </div>
            <div>
              <label className={`flex items-center gap-2 text-sm mt-2 ${isPreset ? "opacity-60" : "cursor-pointer"}`}>
                <input type="checkbox" className="rounded" checked={form.presidio_pattern_only}
                  disabled={isPreset}
                  onChange={(e) => set("presidio_pattern_only", e.target.checked)} />
                Presidio Pattern-Only (regex-only, no spaCy NER)
              </label>
            </div>
            <div>
              <label className="block text-sm font-medium mb-1.5">GLiNER Model</label>
              <input className="input-field" value={form.gliner_model}
                disabled={isPreset}
                onChange={(e) => set("gliner_model", e.target.value)} />
            </div>
            <div>
              <label className="block text-sm font-medium mb-1.5">GLiNER Threshold</label>
              <input type="number" step={0.05} min={0.05} max={1} className="input-field"
                value={form.gliner_threshold}
                disabled={isPreset}
                onChange={(e) => set("gliner_threshold", parseFloat(e.target.value))} />
            </div>
          </>
        )}

        <div className="col-span-2 pt-2 flex gap-2">
          <button className="btn-primary" disabled={saving || !form.name} onClick={save}>
            {saving ? "Saving..." : editingId ? "Update Config" : "Save Config"}
          </button>
          {editingId && (
            <button className="btn-secondary" onClick={cancelEdit}>Cancel Edit</button>
          )}
        </div>
      </div>

      {loading ? (
        <p className="text-sm text-gray-500">Loading...</p>
      ) : (
        <DataTable<Config & Record<string, unknown>>
          data={(configs ?? []) as (Config & Record<string, unknown>)[]}
          rowKey={(c) => c.config_id}
          emptyMessage="No configurations yet."
          columns={configColumns}
        />
      )}
    </div>
  );
}
