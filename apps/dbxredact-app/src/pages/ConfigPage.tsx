import { useState, useEffect } from "react";
import { useGet, apiPost, apiDelete } from "../hooks/useApi";
import ErrorBanner from "../components/ErrorBanner";
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
      await apiPost("/config/", form);
      setForm(DEFAULTS);
      refetch();
    } catch (e: any) {
      setError(e.message || "Failed to save config");
    }
    setSaving(false);
  }

  async function remove(id: string) {
    try {
      await apiDelete(`/config/${id}`);
      refetch();
    } catch (e: any) {
      setError(e.message || "Failed to delete config");
    }
  }

  return (
    <div>
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
          </>
        )}

        <div className="col-span-2 pt-2">
          <button className="btn-primary" disabled={saving || !form.name} onClick={save}>
            {saving ? "Saving..." : "Save Config"}
          </button>
        </div>
      </div>

      {loading ? (
        <p className="text-sm text-gray-500">Loading...</p>
      ) : configs?.length ? (
        <table className="data-table">
          <thead>
            <tr>
              <th>Name</th><th>Profile</th><th>Presidio</th><th>AI</th><th>GLiNER</th>
              <th>Strategy</th><th>Mode</th><th></th>
            </tr>
          </thead>
          <tbody>
            {configs.map((c) => (
              <tr key={c.config_id}>
                <td className="font-medium">{c.name}</td>
                <td><span className="inline-block px-2 py-0.5 text-xs rounded-full bg-purple-100 dark:bg-purple-900/30 text-purple-700 dark:text-purple-300">{c.detection_profile || "custom"}</span></td>
                <td>{c.use_presidio ? "Y" : "-"}</td>
                <td>{c.use_ai_query ? "Y" : "-"}</td>
                <td>{c.use_gliner ? "Y" : "-"}</td>
                <td><span className="inline-block px-2 py-0.5 text-xs rounded-full bg-blue-100 dark:bg-blue-900/30 text-blue-700 dark:text-blue-300">{c.redaction_strategy}</span></td>
                <td>{c.alignment_mode}</td>
                <td>
                  <button className="text-red-500 dark:text-red-400 hover:text-red-700 text-xs font-medium transition-colors" onClick={() => remove(c.config_id)}>
                    Delete
                  </button>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      ) : (
        <p className="text-sm text-gray-400">No configurations yet.</p>
      )}
    </div>
  );
}
