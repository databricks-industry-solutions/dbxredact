import { useState } from "react";
import { BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer, PieChart, Pie, Cell, Legend, RadarChart, Radar, PolarGrid, PolarAngleAxis, PolarRadiusAxis } from "recharts";
import TablePicker, { type TableRef, emptyTableRef, toQualified, isComplete } from "../components/TablePicker";
import ErrorBanner from "../components/ErrorBanner";
import { Skeleton, SkeletonRows } from "../components/Skeleton";
import Tabs from "../components/Tabs";
import { useGet } from "../hooks/useApi";
import type {
  DetectionSummary, EntityByType, ConfidenceBucket, DetectionExample, DetectionData,
  EvalRow, JudgeRow, JudgeExample, Finding,
} from "../types";

const COLORS = ["#3b82f6", "#10b981", "#f59e0b", "#ef4444", "#8b5cf6", "#ec4899", "#06b6d4"];

const METHOD_LABELS: Record<string, string> = {
  gliner: "NER Model",
  presidio: "Presidio",
  ai: "AI Query",
  aligned: "Aligned (ensemble)",
};
const displayMethod = (m: string) => METHOD_LABELS[m] || m;

type Tab = "detection" | "evaluation" | "judge";

export default function MetricsPage() {
  const [baseTable, setBaseTable] = useState<TableRef>(emptyTableRef);
  const [tab, setTab] = useState<Tab>("detection");
  const [showOverrides, setShowOverrides] = useState(false);
  const [detOverride, setDetOverride] = useState("");
  const [evalOverride, setEvalOverride] = useState("");
  const [judgeOverride, setJudgeOverride] = useState("");

  const hasTable = isComplete(baseTable);
  const qualified = toQualified(baseTable);

  const tableMap: Record<Tab, string> = {
    detection: detOverride || `${qualified}_detection_results`,
    evaluation: evalOverride || `${qualified}_evaluation_results`,
    judge: judgeOverride || `${qualified}_judge_results`,
  };

  const detTable = tableMap.detection;
  const evalTable = tableMap.evaluation;
  const judgeTable = tableMap.judge;

  const detEnc = encodeURIComponent(detTable);
  const { data: summary, loading: loadSum, error: summaryError } = useGet<DetectionSummary>(
    `/metrics/summary?output_table=${detEnc}`, { enabled: hasTable && tab === "detection", deps: [detTable] },
  );
  const { data: byType, loading: loadType } = useGet<EntityByType[]>(
    `/metrics/by-type?output_table=${detEnc}`, { enabled: hasTable && tab === "detection", deps: [detTable] },
  );
  const { data: confDist, loading: loadConf } = useGet<ConfidenceBucket[]>(
    `/metrics/confidence-distribution?output_table=${detEnc}`, { enabled: hasTable && tab === "detection", deps: [detTable] },
  );
  const { data: examples, loading: loadEx } = useGet<DetectionExample[]>(
    `/metrics/examples?output_table=${detEnc}`, { enabled: hasTable && tab === "detection", deps: [detTable] },
  );

  const evalEnc = encodeURIComponent(evalTable);
  const { data: evalRows, loading: loadEval, error: evalError } = useGet<EvalRow[]>(
    `/metrics/evaluation?eval_table=${evalEnc}`, { enabled: hasTable && tab === "evaluation", deps: [evalTable] },
  );

  const judgeEnc = encodeURIComponent(judgeTable);
  const { data: judgeRows, loading: loadJudge, error: judgeError } = useGet<JudgeRow[]>(
    `/metrics/judge?judge_table=${judgeEnc}`, { enabled: hasTable && tab === "judge", deps: [judgeTable] },
  );

  const [error, setError] = useState("");
  const displayError = error || summaryError || evalError || judgeError || "";

  const detLoading = loadSum || loadType || loadConf || loadEx;

  const tabs: { key: Tab; label: string }[] = [
    { key: "detection", label: "Detection" },
    { key: "evaluation", label: "Evaluation" },
    { key: "judge", label: "Judge" },
  ];

  return (
    <div>
      <h2 className="page-title">Metrics Dashboard</h2>
      <ErrorBanner message={displayError} onDismiss={() => setError("")} />
      <p className="page-desc">
        Select the benchmark source table. Detection, evaluation, and judge result tables are derived
        automatically using standard suffixes (<code className="text-xs font-mono">_detection_results</code>, etc.).
      </p>

      <div className="mb-5 max-w-2xl">
        <TablePicker value={baseTable} onChange={setBaseTable} label="Benchmark Source Table" />
        <button type="button" className="text-xs text-blue-600 dark:text-blue-400 mt-1.5"
          onClick={() => setShowOverrides(!showOverrides)}>
          {showOverrides ? "Hide table overrides" : "Override derived table names"}
        </button>
        {showOverrides && (
          <div className="grid grid-cols-1 gap-2 mt-2">
            <input className="input-field text-xs" placeholder={`${qualified}_detection_results`}
              value={detOverride} onChange={(e) => setDetOverride(e.target.value)} />
            <input className="input-field text-xs" placeholder={`${qualified}_evaluation_results`}
              value={evalOverride} onChange={(e) => setEvalOverride(e.target.value)} />
            <input className="input-field text-xs" placeholder={`${qualified}_judge_results`}
              value={judgeOverride} onChange={(e) => setJudgeOverride(e.target.value)} />
          </div>
        )}
      </div>

      {hasTable && (
        <Tabs tabs={tabs} active={tab} onChange={(k) => setTab(k as Tab)} />
      )}

      {tab === "detection" && hasTable && (
        detLoading && !summary
          ? <SkeletonRows rows={5} className="max-w-2xl" />
          : summary && <DetectionView summary={summary} byType={byType ?? []} confDist={confDist ?? []} examples={examples ?? []} />
      )}
      {tab === "evaluation" && hasTable && (
        loadEval && !evalRows
          ? <SkeletonRows rows={5} className="max-w-2xl" />
          : evalRows && <EvaluationView rows={evalRows} />
      )}
      {tab === "judge" && hasTable && (
        loadJudge && !judgeRows
          ? <SkeletonRows rows={5} className="max-w-2xl" />
          : judgeRows && <JudgeView rows={judgeRows} judgeTable={tableMap.judge} />
      )}
    </div>
  );
}

function DetectionView({ summary, byType, confDist, examples }: {
  summary: DetectionSummary; byType: EntityByType[]; confDist: ConfidenceBucket[]; examples: DetectionExample[];
}) {
  const isEmpty = !summary.total_entities && !byType.length && !examples.length;
  if (isEmpty) return <p className="text-sm text-gray-400">No detection data found. Run a benchmark first.</p>;

  return (
    <>
      <div className="grid grid-cols-3 gap-4 mb-6">
        {Object.entries(summary).map(([k, v]) => (
          <div key={k} className="stat-card">
            <div className="stat-label">{k.replace(/gliner/gi, "NER").replace(/_/g, " ")}</div>
            <div className="stat-value">{typeof v === "number" ? Number(v).toLocaleString(undefined, { maximumFractionDigits: 1 }) : String(v)}</div>
          </div>
        ))}
      </div>
      <div className="grid grid-cols-2 gap-6 mb-6">
        {byType.length > 0 && (
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
        {confDist.length > 0 && (
          <div className="card p-5">
            <h3 className="font-semibold mb-4">Confidence Distribution</h3>
            <ResponsiveContainer width="100%" height={280}>
              <PieChart>
                <Pie data={confDist} dataKey="count" nameKey="bucket" cx="50%" cy="50%" outerRadius={100} label={({ bucket, percent }: { bucket: string; percent: number }) => `${bucket} (${(percent * 100).toFixed(0)}%)`}>
                  {confDist.map((_, i) => <Cell key={i} fill={COLORS[i % COLORS.length]} />)}
                </Pie>
                <Tooltip />
              </PieChart>
            </ResponsiveContainer>
          </div>
        )}
      </div>
      {examples.length > 0 && (
        <div className="card p-5">
          <h3 className="font-semibold mb-4">Detected Entity Examples</h3>
          <table className="data-table">
            <thead><tr><th>Entity Text</th><th>Type</th><th>Confidence</th></tr></thead>
            <tbody>
              {examples.map((e, i) => (
                <tr key={i}>
                  <td className="font-mono text-xs max-w-xs truncate">{e.entity}</td>
                  <td><span className="text-xs px-2 py-0.5 rounded-full bg-blue-100 dark:bg-blue-900/30 text-blue-700 dark:text-blue-300">{e.entity_type}</span></td>
                  <td className="text-xs font-medium">{e.confidence ?? "---"}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </>
  );
}

function EvaluationView({ rows }: { rows: EvalRow[] }) {
  const [mode, setMode] = useState<"strict" | "overlap">("strict");
  const [metricLevel, setMetricLevel] = useState<"entity" | "token">("entity");
  const [showCorrected, setShowCorrected] = useState(false);
  const [showInfo, setShowInfo] = useState(false);

  if (!rows.length) return <p className="text-sm text-gray-400">No evaluation data found. Run a benchmark first.</p>;

  const modes = [...new Set(rows.map(r => r.match_mode).filter(Boolean))];
  const activeMode = modes.includes(mode) ? mode : modes[0] || "strict";

  const hasCorrected = rows.some(r => r.method_name.includes("_corrected"));
  const hasTokenMetrics = rows.some(r => r.metric_name.startsWith("token_"));

  const filtered = rows.filter(r => {
    const matchesMode = (r.match_mode || r.method_name.split("_").pop()) === activeMode;
    if (!matchesMode) return false;
    const isCorrected = r.method_name.includes("_corrected");
    return showCorrected ? isCorrected : !isCorrected;
  });

  const stripSuffix = (name: string) => name.replace(/_strict$|_overlap$|_corrected/g, "");
  const rawMethods = [...new Set(filtered.map(r => stripSuffix(r.method_name)))];
  const methods = rawMethods.map(displayMethod);

  const prefix = metricLevel === "token" ? "token_" : "";
  const coreMetrics = [`${prefix}precision`, `${prefix}recall`, `${prefix}f1_score`];
  const tpKey = `${prefix}true_positives`;
  const fpKey = `${prefix}false_positives`;
  const fnKey = `${prefix}false_negatives`;
  const tnKey = metricLevel === "entity" ? "true_negatives" : "";

  const findRow = (raw: string, metric: string) =>
    filtered.find(r => stripSuffix(r.method_name) === raw && r.metric_name === metric);

  const chartData = coreMetrics.map(m => {
    const label = m.replace("token_", "").replace("_", " ");
    const point: Record<string, string | number> = { metric: label };
    rawMethods.forEach((raw, i) => {
      const row = findRow(raw, m);
      point[methods[i]] = row ? Number(row.metric_value) : 0;
    });
    return point;
  });

  const radarData = rawMethods.map((raw, i) => {
    const point: Record<string, string | number> = { method: methods[i] };
    coreMetrics.forEach(m => {
      const row = findRow(raw, m);
      point[m.replace("token_", "").replace("_", " ")] = row ? Number(row.metric_value) : 0;
    });
    return point;
  });

  const confusionRows = rawMethods.map((raw, i) => {
    const get = (m: string) => { const r = findRow(raw, m); return r ? Number(r.metric_value) : 0; };
    return { method: methods[i], tp: get(tpKey), fp: get(fpKey), fn: get(fnKey), tn: tnKey ? get(tnKey) : null };
  });

  const bestOf = (metric: string) => rawMethods.reduce((best, raw, i) => {
    const row = findRow(raw, metric);
    const val = row ? Number(row.metric_value) : 0;
    return val > best.val ? { method: methods[i], val } : best;
  }, { method: "", val: 0 });

  const bestF1 = bestOf(coreMetrics[2]);
  const bestP = bestOf(coreMetrics[0]);
  const bestR = bestOf(coreMetrics[1]);

  const avgP = rawMethods.reduce((s, raw) => { const r = findRow(raw, coreMetrics[0]); return s + (r ? Number(r.metric_value) : 0); }, 0) / (methods.length || 1);
  const avgR = rawMethods.reduce((s, raw) => { const r = findRow(raw, coreMetrics[1]); return s + (r ? Number(r.metric_value) : 0); }, 0) / (methods.length || 1);

  const radarMetricLabels = coreMetrics.map(m => m.replace("token_", "").replace("_", " "));

  return (
    <>
      <div className="flex flex-wrap items-center gap-4 mb-4">
        <div className="flex items-center gap-1 bg-gray-100 dark:bg-gray-800 rounded-lg p-1">
          {(["strict", "overlap"] as const).map(m => (
            <button key={m} onClick={() => setMode(m)}
              className={`px-4 py-1.5 rounded-md text-sm font-medium transition-colors ${
                activeMode === m ? "bg-white dark:bg-gray-700 shadow-sm text-blue-600 dark:text-blue-400" : "text-gray-500 dark:text-gray-400 hover:text-gray-700"
              }`}>{m.charAt(0).toUpperCase() + m.slice(1)} Match</button>
          ))}
        </div>

        {hasTokenMetrics && (
          <div className="flex items-center gap-1 bg-gray-100 dark:bg-gray-800 rounded-lg p-1">
            {(["entity", "token"] as const).map(l => (
              <button key={l} onClick={() => setMetricLevel(l)}
                className={`px-4 py-1.5 rounded-md text-sm font-medium transition-colors ${
                  metricLevel === l ? "bg-white dark:bg-gray-700 shadow-sm text-blue-600 dark:text-blue-400" : "text-gray-500 dark:text-gray-400 hover:text-gray-700"
                }`}>{l === "entity" ? "Entity-level" : "Token-level"}</button>
            ))}
          </div>
        )}

        {hasCorrected && (
          <label className="flex items-center gap-2 text-sm text-gray-600 dark:text-gray-300 cursor-pointer select-none">
            <input type="checkbox" checked={showCorrected} onChange={e => setShowCorrected(e.target.checked)}
              className="rounded border-gray-300 dark:border-gray-600 text-blue-600 focus:ring-blue-500" />
            Show Corrected
          </label>
        )}

        <button type="button" onClick={() => setShowInfo(!showInfo)}
          className="text-xs text-gray-400 hover:text-gray-600 dark:hover:text-gray-200 underline underline-offset-2">
          {showInfo ? "Hide explanation" : "What do these mean?"}
        </button>
      </div>

      {showInfo && (
        <div className="mb-5 p-4 rounded-lg bg-blue-50 dark:bg-blue-900/20 border border-blue-200 dark:border-blue-800 text-sm text-gray-700 dark:text-gray-300 space-y-2">
          <p><span className="font-semibold">Entity-level</span> counts each detected PII item equally -- missing a 2-character state code penalizes the same as missing a 50-character address.</p>
          <p><span className="font-semibold">Token-level</span> weights by text length, matching how OpenAI reports Privacy Filter benchmarks (0.98 recall). Better reflects actual data protection coverage.</p>
          {hasCorrected && <p><span className="font-semibold">Corrected</span> excludes ground truth entity types with no detector mapping (e.g. DEMOGRAPHIC), following OpenAI's PII-Masking-300k methodology.</p>}
        </div>
      )}

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
            <RadarChart data={radarMetricLabels.map(label => ({ metric: label, ...Object.fromEntries(radarData.map(r => [r.method, r[label]])) }))}>
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
          <h3 className="font-semibold mb-4">
            {metricLevel === "token" ? "Token Counts (characters)" : "Confusion Matrix Counts"}
          </h3>
          <table className="data-table">
            <thead><tr><th>Method</th><th>TP</th><th>FP</th><th>FN</th>{confusionRows[0]?.tn != null && <th>TN</th>}</tr></thead>
            <tbody>
              {confusionRows.map(r => (
                <tr key={r.method}>
                  <td className="font-medium text-xs">{r.method}</td>
                  <td className="text-emerald-600 dark:text-emerald-400 font-mono text-xs">{r.tp.toLocaleString()}</td>
                  <td className="text-red-600 dark:text-red-400 font-mono text-xs">{r.fp.toLocaleString()}</td>
                  <td className="text-amber-600 dark:text-amber-400 font-mono text-xs">{r.fn.toLocaleString()}</td>
                  {r.tn != null && <td className="font-mono text-xs">{r.tn.toLocaleString()}</td>}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </>
  );
}

function parseFindings(val: unknown): Finding[] {
  if (!val) return [];
  if (Array.isArray(val)) return val as Finding[];
  if (typeof val === "string") {
    try { return JSON.parse(val) as Finding[]; } catch { return []; }
  }
  return [];
}

function JudgeView({ rows, judgeTable }: { rows: JudgeRow[]; judgeTable: string }) {
  const [selectedGrade, setSelectedGrade] = useState<"PASS" | "PARTIAL" | "FAIL">("FAIL");
  const [exampleCount, setExampleCount] = useState(5);

  const { data: examples, loading: loadingEx } = useGet<JudgeExample[]>(
    `/metrics/judge-examples?judge_table=${encodeURIComponent(judgeTable)}&grade=${selectedGrade}&limit=${exampleCount}`,
    { enabled: !!judgeTable, deps: [judgeTable, selectedGrade, exampleCount] },
  );

  if (!rows.length) return <p className="text-sm text-gray-400">No judge data found. Run a benchmark first.</p>;

  const gradeColors: Record<string, string> = { PASS: "#10b981", PARTIAL: "#f59e0b", FAIL: "#ef4444" };
  const gradeBg: Record<string, string> = {
    PASS: "bg-emerald-600 text-white",
    PARTIAL: "bg-amber-500 text-white",
    FAIL: "bg-red-600 text-white",
  };
  const rawJudgeMethods = [...new Set(rows.map(r => r.method))];
  const chartData = rawJudgeMethods.map(method => {
    const point: Record<string, string | number> = { method: displayMethod(method) };
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
          {loadingEx && !examples && <SkeletonRows rows={3} />}
          {examples?.length ? (
            <div className="space-y-3 max-h-[400px] overflow-y-auto">
              {examples.map((ex, i) => {
                const findings = parseFindings(ex.findings);
                return (
                  <div key={i} className="border border-gray-200 dark:border-gray-700 rounded-lg p-3">
                    <div className="flex items-center gap-2 mb-2">
                      <span className="text-xs font-mono text-gray-500">{ex.doc_id}</span>
                      <span className="text-xs px-1.5 py-0.5 rounded bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-300">{ex.method}</span>
                    </div>
                    {findings.length ? (
                      <ul className="space-y-1">
                        {findings.slice(0, 10).map((f, j) => (
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
