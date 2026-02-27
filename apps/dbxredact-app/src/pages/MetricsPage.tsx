import { useState, useEffect } from "react";
import { BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer, PieChart, Pie, Cell, Legend, RadarChart, Radar, PolarGrid, PolarAngleAxis, PolarRadiusAxis } from "recharts";
import TablePicker from "../components/TablePicker";
import ErrorBanner from "../components/ErrorBanner";

const COLORS = ["#3b82f6", "#10b981", "#f59e0b", "#ef4444", "#8b5cf6", "#ec4899", "#06b6d4"];

type Tab = "detection" | "evaluation" | "judge";

export default function MetricsPage() {
  const [baseTable, setBaseTable] = useState("");
  const [tab, setTab] = useState<Tab>("detection");
  const [loading, setLoading] = useState(false);
  const [data, setData] = useState<any>(null);
  const [error, setError] = useState("");

  const parts = baseTable.split(".");
  const hasTable = parts.length === 3 && parts[2] !== "";

  const tableMap: Record<Tab, string> = {
    detection: `${baseTable}_detection_results`,
    evaluation: `${baseTable}_evaluation_results`,
    judge: `${baseTable}_judge_results`,
  };

  useEffect(() => {
    if (!hasTable) { setData(null); return; }
    setLoading(true);
    setData(null);
    const enc = encodeURIComponent(tableMap[tab]);
    let req: Promise<any>;

    if (tab === "detection") {
      req = Promise.all([
        fetch(`/api/metrics/summary?output_table=${enc}`).then(r => r.json()),
        fetch(`/api/metrics/by-type?output_table=${enc}`).then(r => r.json()),
        fetch(`/api/metrics/confidence-distribution?output_table=${enc}`).then(r => r.json()),
        fetch(`/api/metrics/examples?output_table=${enc}`).then(r => r.json()),
      ]).then(([summary, byType, confDist, examples]) => ({ summary, byType, confDist, examples }));
    } else if (tab === "evaluation") {
      req = fetch(`/api/metrics/evaluation?eval_table=${enc}`).then(r => r.json()).then(rows => ({ rows }));
    } else if (tab === "judge") {
      req = fetch(`/api/metrics/judge?judge_table=${enc}`).then(r => r.json()).then(rows => ({ rows }));
    } else {
      return;
    }
    req.then(setData).catch((e: any) => { setError(e.message || "Failed to load metrics"); setData(null); }).finally(() => setLoading(false));
  }, [baseTable, tab, hasTable]);

  const tabs: { key: Tab; label: string }[] = [
    { key: "detection", label: "Detection" },
    { key: "evaluation", label: "Evaluation" },
    { key: "judge", label: "Judge" },
  ];

  return (
    <div>
      <h2 className="page-title">Metrics Dashboard</h2>
      <ErrorBanner message={error} onDismiss={() => setError("")} />
      <p className="page-desc">
        Pick the original source table (e.g. <code className="text-xs font-mono">jsl_benchmark</code>).
        All result tables are derived automatically with standard suffixes.
      </p>

      <div className="mb-5 max-w-2xl">
        <TablePicker value={baseTable} onChange={setBaseTable} label="Benchmark Source Table" />
      </div>

      {hasTable && (
        <div className="flex gap-1 mb-6 border-b border-gray-200 dark:border-gray-700">
          {tabs.map(t => (
            <button key={t.key} onClick={() => setTab(t.key)}
              className={`px-4 py-2.5 text-sm font-medium border-b-2 transition-colors -mb-px ${
                tab === t.key
                  ? "border-blue-600 text-blue-600 dark:text-blue-400"
                  : "border-transparent text-gray-500 hover:text-gray-700 dark:text-gray-400 dark:hover:text-gray-200"
              }`}>{t.label}</button>
          ))}
        </div>
      )}

      {loading && <p className="text-sm text-gray-500 dark:text-gray-400 animate-pulse">Loading...</p>}

      {data && tab === "detection" && <DetectionView data={data} />}
      {data && tab === "evaluation" && <EvaluationView rows={data.rows} />}
      {data && tab === "judge" && <JudgeView rows={data.rows} judgeTable={tableMap["judge"]} />}
    </div>
  );
}

function DetectionView({ data }: { data: any }) {
  const { summary, byType, confDist, examples } = data;
  return (
    <>
      {summary && (
        <div className="grid grid-cols-3 gap-4 mb-6">
          {Object.entries(summary).map(([k, v]) => (
            <div key={k} className="stat-card">
              <div className="stat-label">{k.replace(/_/g, " ")}</div>
              <div className="stat-value">{typeof v === "number" ? Number(v).toLocaleString(undefined, { maximumFractionDigits: 1 }) : String(v)}</div>
            </div>
          ))}
        </div>
      )}
      <div className="grid grid-cols-2 gap-6 mb-6">
        {byType?.length > 0 && (
          <div className="card p-5">
            <h3 className="font-semibold mb-4">Entities by Type</h3>
            <ResponsiveContainer width="100%" height={280}>
              <BarChart data={byType} layout="vertical">
                <XAxis type="number" />
                <YAxis dataKey="entity_type" type="category" width={120} tick={{ fontSize: 11 }} />
                <Tooltip />
                <Bar dataKey="count" fill="#3b82f6" radius={[0, 4, 4, 0]} />
              </BarChart>
            </ResponsiveContainer>
          </div>
        )}
        {confDist?.length > 0 && (
          <div className="card p-5">
            <h3 className="font-semibold mb-4">Confidence Distribution</h3>
            <ResponsiveContainer width="100%" height={280}>
              <PieChart>
                <Pie data={confDist} dataKey="count" nameKey="bucket" cx="50%" cy="50%" outerRadius={100} label={({ bucket, percent }) => `${bucket} (${(percent * 100).toFixed(0)}%)`}>
                  {confDist.map((_: any, i: number) => <Cell key={i} fill={COLORS[i % COLORS.length]} />)}
                </Pie>
                <Tooltip />
              </PieChart>
            </ResponsiveContainer>
          </div>
        )}
      </div>
      {examples?.length > 0 && (
        <div className="card p-5">
          <h3 className="font-semibold mb-4">Detected Entity Examples</h3>
          <table className="data-table">
            <thead><tr><th>Entity Text</th><th>Type</th><th>Confidence</th></tr></thead>
            <tbody>
              {examples.map((e: any, i: number) => (
                <tr key={i}>
                  <td className="font-mono text-xs max-w-xs truncate">{e.entity}</td>
                  <td><span className="text-xs px-2 py-0.5 rounded-full bg-blue-100 dark:bg-blue-900/30 text-blue-700 dark:text-blue-300">{e.entity_type}</span></td>
                  <td className="text-xs font-medium">{e.confidence || "---"}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </>
  );
}

function EvaluationView({ rows }: { rows: any[] }) {
  const [mode, setMode] = useState<"strict" | "overlap">("strict");

  if (!rows?.length) return <p className="text-sm text-gray-400">No evaluation data found. Run a benchmark first.</p>;

  const modes = [...new Set(rows.map(r => r.match_mode).filter(Boolean))];
  const activeMode = modes.includes(mode) ? mode : modes[0] || "strict";
  const filtered = rows.filter(r => (r.match_mode || r.method_name?.split("_").pop()) === activeMode);

  const stripSuffix = (name: string) => name.replace(/_strict$|_overlap$/, "");
  const methods = [...new Set(filtered.map(r => stripSuffix(r.method_name)))];
  const coreMetrics = ["precision", "recall", "f1_score"];

  const chartData = coreMetrics.map(m => {
    const point: Record<string, any> = { metric: m.replace("_", " ") };
    methods.forEach(method => {
      const row = filtered.find(r => stripSuffix(r.method_name) === method && r.metric_name === m);
      point[method] = row ? Number(row.metric_value) : 0;
    });
    return point;
  });

  const radarData = methods.map(method => {
    const point: Record<string, any> = { method };
    coreMetrics.forEach(m => {
      const row = filtered.find(r => stripSuffix(r.method_name) === method && r.metric_name === m);
      point[m] = row ? Number(row.metric_value) : 0;
    });
    return point;
  });

  const confusionRows = methods.map(method => {
    const get = (m: string) => {
      const r = filtered.find(r => stripSuffix(r.method_name) === method && r.metric_name === m);
      return r ? Number(r.metric_value) : 0;
    };
    return { method, tp: get("true_positives"), fp: get("false_positives"), fn: get("false_negatives"), tn: get("true_negatives") };
  });

  const bestOf = (metric: string) => methods.reduce((best, m) => {
    const row = filtered.find(r => stripSuffix(r.method_name) === m && r.metric_name === metric);
    const val = row ? Number(row.metric_value) : 0;
    return val > best.val ? { method: m, val } : best;
  }, { method: "", val: 0 });

  const bestF1 = bestOf("f1_score");
  const bestP = bestOf("precision");
  const bestR = bestOf("recall");

  const avgP = methods.reduce((s, m) => { const r = filtered.find(x => stripSuffix(x.method_name) === m && x.metric_name === "precision"); return s + (r ? Number(r.metric_value) : 0); }, 0) / (methods.length || 1);
  const avgR = methods.reduce((s, m) => { const r = filtered.find(x => stripSuffix(x.method_name) === m && x.metric_name === "recall"); return s + (r ? Number(r.metric_value) : 0); }, 0) / (methods.length || 1);

  return (
    <>
      <div className="flex items-center gap-1 mb-5 bg-gray-100 dark:bg-gray-800 rounded-lg p-1 w-fit">
        {(["strict", "overlap"] as const).map(m => (
          <button key={m} onClick={() => setMode(m)}
            className={`px-4 py-1.5 rounded-md text-sm font-medium transition-colors ${
              activeMode === m ? "bg-white dark:bg-gray-700 shadow-sm text-blue-600 dark:text-blue-400" : "text-gray-500 dark:text-gray-400 hover:text-gray-700"
            }`}>{m.charAt(0).toUpperCase() + m.slice(1)} Match</button>
        ))}
      </div>

      <div className="grid grid-cols-3 md:grid-cols-6 gap-4 mb-6">
        <div className="stat-card"><div className="stat-label">methods evaluated</div><div className="stat-value">{methods.length}</div></div>
        <div className="stat-card"><div className="stat-label">best F1</div><div className="stat-value text-sm">{bestF1.method || "---"} ({bestF1.val.toFixed(3)})</div></div>
        <div className="stat-card"><div className="stat-label">best precision</div><div className="stat-value text-sm">{bestP.method || "---"} ({bestP.val.toFixed(3)})</div></div>
        <div className="stat-card"><div className="stat-label">best recall</div><div className="stat-value text-sm">{bestR.method || "---"} ({bestR.val.toFixed(3)})</div></div>
        <div className="stat-card"><div className="stat-label">avg precision</div><div className="stat-value">{avgP.toFixed(3)}</div></div>
        <div className="stat-card"><div className="stat-label">avg recall</div><div className="stat-value">{avgR.toFixed(3)}</div></div>
      </div>

      <div className="card p-5 mb-6">
        <h3 className="font-semibold mb-4">Precision / Recall / F1 by Method</h3>
        <ResponsiveContainer width="100%" height={300}>
          <BarChart data={chartData}>
            <XAxis dataKey="metric" />
            <YAxis domain={[0, 1]} />
            <Tooltip />
            <Legend />
            {methods.map((m, i) => (
              <Bar key={m} dataKey={m} fill={COLORS[i % COLORS.length]} radius={[4, 4, 0, 0]} />
            ))}
          </BarChart>
        </ResponsiveContainer>
      </div>

      <div className="grid grid-cols-2 gap-6 mb-6">
        <div className="card p-5">
          <h3 className="font-semibold mb-4">Method Comparison (Radar)</h3>
          <ResponsiveContainer width="100%" height={300}>
            <RadarChart data={coreMetrics.map(m => ({ metric: m.replace("_", " "), ...Object.fromEntries(radarData.map(r => [r.method, r[m]])) }))}>
              <PolarGrid />
              <PolarAngleAxis dataKey="metric" tick={{ fontSize: 11 }} />
              <PolarRadiusAxis domain={[0, 1]} tick={{ fontSize: 10 }} />
              {methods.map((m, i) => (
                <Radar key={m} name={m} dataKey={m} stroke={COLORS[i % COLORS.length]} fill={COLORS[i % COLORS.length]} fillOpacity={0.15} />
              ))}
              <Legend />
              <Tooltip />
            </RadarChart>
          </ResponsiveContainer>
        </div>

        <div className="card p-5">
          <h3 className="font-semibold mb-4">Confusion Matrix Counts</h3>
          <table className="data-table">
            <thead><tr><th>Method</th><th>TP</th><th>FP</th><th>FN</th><th>TN</th></tr></thead>
            <tbody>
              {confusionRows.map(r => (
                <tr key={r.method}>
                  <td className="font-medium text-xs">{r.method}</td>
                  <td className="text-emerald-600 dark:text-emerald-400 font-mono text-xs">{r.tp}</td>
                  <td className="text-red-600 dark:text-red-400 font-mono text-xs">{r.fp}</td>
                  <td className="text-amber-600 dark:text-amber-400 font-mono text-xs">{r.fn}</td>
                  <td className="font-mono text-xs">{r.tn}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </>
  );
}

function parseFindings(val: unknown): any[] {
  if (!val) return [];
  if (Array.isArray(val)) return val;
  if (typeof val === "string") {
    try { return JSON.parse(val); } catch { return []; }
  }
  return [];
}

function JudgeView({ rows, judgeTable }: { rows: any[]; judgeTable: string }) {
  const [selectedGrade, setSelectedGrade] = useState<"PASS" | "PARTIAL" | "FAIL">("FAIL");
  const [examples, setExamples] = useState<any[] | null>(null);
  const [loadingEx, setLoadingEx] = useState(false);
  const [exampleCount, setExampleCount] = useState(5);

  useEffect(() => {
    if (!judgeTable) return;
    setLoadingEx(true);
    fetch(`/api/metrics/judge-examples?judge_table=${encodeURIComponent(judgeTable)}&grade=${selectedGrade}&limit=${exampleCount}`)
      .then(r => r.json()).then(setExamples)
      .catch((e: any) => setExamples(null))
      .finally(() => setLoadingEx(false));
  }, [judgeTable, selectedGrade, exampleCount]);

  if (!rows?.length) return <p className="text-sm text-gray-400">No judge data found. Run a benchmark first.</p>;

  const gradeColors: Record<string, string> = { PASS: "#10b981", PARTIAL: "#f59e0b", FAIL: "#ef4444" };
  const gradeBg: Record<string, string> = {
    PASS: "bg-emerald-600 text-white",
    PARTIAL: "bg-amber-500 text-white",
    FAIL: "bg-red-600 text-white",
  };
  const methods = [...new Set(rows.map(r => r.method))];
  const chartData = methods.map(method => {
    const point: Record<string, any> = { method };
    rows.filter(r => r.method === method).forEach(r => { point[r.grade] = Number(r.count); });
    return point;
  });

  const total = rows.reduce((s, r) => s + Number(r.count), 0);
  const passCount = rows.filter(r => r.grade === "PASS").reduce((s, r) => s + Number(r.count), 0);
  const partialCount = rows.filter(r => r.grade === "PARTIAL").reduce((s, r) => s + Number(r.count), 0);
  const failCount = rows.filter(r => r.grade === "FAIL").reduce((s, r) => s + Number(r.count), 0);

  return (
    <>
      <div className="grid grid-cols-4 gap-4 mb-6">
        <div className="stat-card"><div className="stat-label">total judged</div><div className="stat-value">{total}</div></div>
        <div className="stat-card"><div className="stat-label">pass rate</div><div className="stat-value text-emerald-600 dark:text-emerald-400">{total ? ((passCount / total) * 100).toFixed(1) : 0}%</div></div>
        <div className="stat-card"><div className="stat-label">partial rate</div><div className="stat-value text-amber-600 dark:text-amber-400">{total ? ((partialCount / total) * 100).toFixed(1) : 0}%</div></div>
        <div className="stat-card"><div className="stat-label">fail rate</div><div className="stat-value text-red-600 dark:text-red-400">{total ? ((failCount / total) * 100).toFixed(1) : 0}%</div></div>
      </div>

      <div className="grid grid-cols-2 gap-6">
        <div className="card p-5">
          <h3 className="font-semibold mb-4">Grade Distribution</h3>
          <ResponsiveContainer width="100%" height={300}>
            <BarChart data={chartData}>
              <XAxis dataKey="method" />
              <YAxis />
              <Tooltip />
              <Legend />
              {["PASS", "PARTIAL", "FAIL"].map(g => (
                <Bar key={g} dataKey={g} stackId="a" fill={gradeColors[g]} radius={g === "FAIL" ? [4, 4, 0, 0] : undefined} />
              ))}
            </BarChart>
          </ResponsiveContainer>
        </div>

        <div className="card p-5">
          <div className="flex items-center justify-between mb-3">
            <h3 className="font-semibold">Example Documents</h3>
            <label className="flex items-center gap-1.5 text-xs text-gray-500">
              Show
              <select className="input-field text-xs py-0.5 px-1.5 w-16" value={exampleCount}
                onChange={e => setExampleCount(Number(e.target.value))}>
                {[5, 10, 15, 20].map(n => <option key={n} value={n}>{n}</option>)}
              </select>
            </label>
          </div>
          <div className="flex gap-1 mb-4 bg-gray-100 dark:bg-gray-800 rounded-lg p-1 w-fit">
            {(["FAIL", "PARTIAL", "PASS"] as const).map(g => (
              <button key={g} onClick={() => setSelectedGrade(g)}
                className={`px-3 py-1.5 rounded-md text-xs font-medium transition-colors ${
                  selectedGrade === g ? `${gradeBg[g]} shadow-sm` : "text-gray-500 dark:text-gray-400 hover:text-gray-700"
                }`}>{g}</button>
            ))}
          </div>
          {loadingEx && <p className="text-xs text-gray-400 animate-pulse">Loading...</p>}
          {examples?.length ? (
            <div className="space-y-3 max-h-[400px] overflow-y-auto">
              {examples.map((ex: any, i: number) => {
                const findings = parseFindings(ex.findings);
                return (
                  <div key={i} className="border border-gray-200 dark:border-gray-700 rounded-lg p-3">
                    <div className="flex items-center gap-2 mb-2">
                      <span className="text-xs font-mono text-gray-500">{ex.doc_id}</span>
                      <span className="text-xs px-1.5 py-0.5 rounded bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-300">{ex.method}</span>
                    </div>
                    {findings.length ? (
                      <ul className="space-y-1">
                        {findings.slice(0, 10).map((f: any, j: number) => (
                          <li key={j} className="text-xs leading-relaxed">
                            <span className="font-medium">{f.entity_type || f.type}</span>
                            {(f.entity || f.value) && <>: <span className="font-mono">{f.entity || f.value}</span></>}
                            {f.status && <span className="ml-1 text-gray-400">({f.status})</span>}
                            {f.explanation && <span className="ml-1 text-gray-500 dark:text-gray-400"> -- {f.explanation}</span>}
                          </li>
                        ))}
                        {findings.length > 10 && <li className="text-xs text-gray-400">...and {findings.length - 10} more</li>}
                      </ul>
                    ) : <p className="text-xs text-gray-400">No findings</p>}
                  </div>
                );
              })}
            </div>
          ) : (!loadingEx && <p className="text-xs text-gray-400">No {selectedGrade} examples found.</p>)}
        </div>
      </div>
    </>
  );
}

