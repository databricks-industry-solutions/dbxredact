import { useState } from "react";
import { useGet, apiPost } from "../hooks/useApi";
import { BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer, Legend } from "recharts";
import TablePicker from "../components/TablePicker";
import type { Config, ABTest } from "../types";

export default function ABTestPage() {
  const { data: tests, refetch } = useGet<ABTest[]>("/ab-tests/");
  const { data: configs } = useGet<Config[]>("/config/");
  const [name, setName] = useState("");
  const [configA, setConfigA] = useState("");
  const [configB, setConfigB] = useState("");
  const [sourceTable, setSourceTable] = useState("");
  const [sampleSize, setSampleSize] = useState(100);

  async function create() {
    await apiPost("/ab-tests/", {
      name, config_a_id: configA, config_b_id: configB,
      source_table: sourceTable, sample_size: sampleSize,
    });
    setName("");
    refetch();
  }

  async function runTest(testId: string) {
    await apiPost(`/ab-tests/${testId}/run`, {});
    refetch();
  }

  function metricsChart(test: ABTest) {
    if (!test.metrics_a || !test.metrics_b) return null;
    const data = ["precision", "recall", "f1"].map((k) => ({
      metric: k,
      "Config A": (test.metrics_a as Record<string, number>)[k] || 0,
      "Config B": (test.metrics_b as Record<string, number>)[k] || 0,
    }));
    return (
      <div className="mt-4">
        <ResponsiveContainer width="100%" height={200}>
          <BarChart data={data}>
            <XAxis dataKey="metric" />
            <YAxis domain={[0, 1]} />
            <Tooltip />
            <Legend />
            <Bar dataKey="Config A" fill="#3b82f6" radius={[4, 4, 0, 0]} />
            <Bar dataKey="Config B" fill="#10b981" radius={[4, 4, 0, 0]} />
          </BarChart>
        </ResponsiveContainer>
      </div>
    );
  }

  const configName = (id: string) => configs?.find(c => c.config_id === id)?.name || id.slice(0, 8);

  return (
    <div>
      <h2 className="page-title">A/B Testing</h2>
      <p className="page-desc">
        Compare two detection configurations on the same data to find the best setup.
        Create a test by choosing two configs and a labeled source table, then run it.
        The system will execute both configurations on a random sample and compare precision, recall, and F1.
      </p>

      <div className="card p-4 mb-6">
        <div className="text-xs text-gray-500 dark:text-gray-400 leading-relaxed">
          <b>How to use:</b> 1) Create at least two detection configs on the Config page (e.g. one with Presidio only, one with all methods).
          2) Pick a source table with labeled ground truth. 3) Set a sample size (number of documents to compare).
          4) Click "Create Test", then "Run Test". Results will show which config performs better.
        </div>
      </div>

      <div className="card p-5 mb-8 grid grid-cols-2 gap-4 max-w-2xl">
        <div className="col-span-2">
          <label className="block text-sm font-medium mb-1.5">Test Name</label>
          <input className="input-field" value={name} placeholder="e.g. presidio-only vs all-methods"
            onChange={(e) => setName(e.target.value)} />
        </div>
        <div>
          <label className="block text-sm font-medium mb-1.5">Config A</label>
          <select className="input-field" value={configA}
            onChange={(e) => setConfigA(e.target.value)}>
            <option value="">-- select config --</option>
            {configs?.map((c) => <option key={c.config_id} value={c.config_id}>{c.name}</option>)}
          </select>
        </div>
        <div>
          <label className="block text-sm font-medium mb-1.5">Config B</label>
          <select className="input-field" value={configB}
            onChange={(e) => setConfigB(e.target.value)}>
            <option value="">-- select config --</option>
            {configs?.map((c) => <option key={c.config_id} value={c.config_id}>{c.name}</option>)}
          </select>
        </div>
        <div className="col-span-2">
          <TablePicker value={sourceTable} onChange={setSourceTable} label="Source Table (labeled data)" />
        </div>
        <div>
          <label className="block text-sm font-medium mb-1.5">Sample Size</label>
          <input type="number" className="input-field" value={sampleSize}
            onChange={(e) => setSampleSize(parseInt(e.target.value))} />
        </div>
        <div className="col-span-2 pt-2">
          <button className="btn-primary" disabled={!name || !configA || !configB || !sourceTable.includes(".")} onClick={create}>
            Create Test
          </button>
        </div>
      </div>

      {tests?.length ? (
        <>
          <h3 className="text-lg font-semibold mb-3">Tests</h3>
          <div className="space-y-4">
            {tests.map((t) => (
              <div key={t.test_id} className="card p-5">
                <div className="flex justify-between items-center mb-2">
                  <h3 className="font-semibold">{t.name}</h3>
                  <span className={`text-xs px-2.5 py-1 rounded-full font-medium ${
                    t.status === "completed" ? "bg-emerald-100 dark:bg-emerald-900/30 text-emerald-700 dark:text-emerald-300"
                    : t.status === "running" ? "bg-amber-100 dark:bg-amber-900/30 text-amber-700 dark:text-amber-300"
                    : "bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-300"
                  }`}>{t.status}</span>
                </div>
                <div className="text-xs text-gray-500 dark:text-gray-400 mb-3">
                  A: {configName(t.config_a_id)} | B: {configName(t.config_b_id)} | {t.source_table} | n={t.sample_size}
                </div>
                {t.status === "created" && (
                  <button className="btn-success text-sm" onClick={() => runTest(t.test_id)}>Run Test</button>
                )}
                {metricsChart(t)}
                {t.winner && <p className="text-sm mt-3 font-semibold text-emerald-600 dark:text-emerald-400">Winner: Config {t.winner}</p>}
              </div>
            ))}
          </div>
        </>
      ) : (
        <p className="text-sm text-gray-400">No tests yet. Create one above to get started.</p>
      )}
    </div>
  );
}
