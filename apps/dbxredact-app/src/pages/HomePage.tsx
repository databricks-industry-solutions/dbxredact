import { Link } from "react-router-dom";
import { useGet } from "../hooks/useApi";
import type { Config, JobHistoryItem, ActiveLearnStats } from "../types";

const TERMINAL = ["TERMINATED", "SUCCESS", "SKIPPED", "INTERNAL_ERROR"];

interface AuditSummary {
  total_runs?: number;
  total_docs?: number;
  total_entities?: number;
}

function fmt(n: number | undefined): string {
  if (n == null) return "0";
  return n.toLocaleString();
}

function StatusDot({ status }: { status: string }) {
  if (TERMINAL.includes(status)) {
    const ok = status === "TERMINATED" || status === "SUCCESS";
    return (
      <span className={`inline-block w-2 h-2 rounded-full ${ok ? "bg-emerald-500" : "bg-red-500"}`} />
    );
  }
  return <span className="inline-block w-2 h-2 rounded-full bg-blue-500 animate-pulse" />;
}

export default function HomePage() {
  const { data: configs } = useGet<Config[]>("/config/");
  const { data: history } = useGet<JobHistoryItem[]>("/pipeline/history?limit=5");
  const { data: alStats } = useGet<ActiveLearnStats>("/active-learn/stats");
  const { data: auditSummary } = useGet<AuditSummary>("/admin/audit-summary");

  const configCount = configs?.length ?? 0;
  const recentRuns = history ?? [];
  const hasConfigs = configCount > 0;
  const hasRuns = recentRuns.length > 0;
  const successRun = recentRuns.find((r) => r.status === "TERMINATED" || r.status === "SUCCESS");
  const lastRun = recentRuns[0] ?? null;

  const steps = [
    { label: "Create a configuration", done: hasConfigs, to: "/config" },
    { label: "Run a pipeline", done: hasRuns, to: "/run" },
    { label: "Review results", done: !!successRun, to: "/review" },
  ];

  return (
    <div>
      <div className="mb-8">
        <h1 className="text-2xl font-bold tracking-tight mb-2">dbxredact</h1>
        <p className="text-gray-600 dark:text-gray-400 max-w-2xl leading-relaxed">
          PII and PHI detection and redaction for Databricks. Configure detection methods,
          run pipelines on Unity Catalog tables, benchmark against ground truth, and
          iteratively improve quality through human-in-the-loop workflows.
        </p>
      </div>

      {/* Outcome-oriented stats */}
      <div className="grid grid-cols-2 sm:grid-cols-4 gap-4 mb-8">
        <div className="stat-card">
          <div className="stat-label">PII Items Detected</div>
          <div className="stat-value">{fmt(auditSummary?.total_entities)}</div>
        </div>
        <div className="stat-card">
          <div className="stat-label">Documents Processed</div>
          <div className="stat-value">{fmt(auditSummary?.total_docs)}</div>
        </div>
        <div className="stat-card">
          <div className="stat-label">Detection Runs</div>
          <div className="stat-value">{fmt(auditSummary?.total_runs)}</div>
        </div>
        <div className="stat-card">
          <div className="stat-label">Active Learn Queue</div>
          <div className="stat-value">{alStats?.pending ?? 0}</div>
        </div>
      </div>

      {/* Last Run Summary */}
      {lastRun && (
        <div className="card p-5 mb-8">
          <div className="flex items-center justify-between mb-3">
            <h2 className="text-sm font-semibold">Last Run</h2>
            <Link to="/run" className="text-xs text-blue-600 dark:text-blue-400 hover:underline">
              View all runs
            </Link>
          </div>
          <div className="flex items-start gap-4 flex-wrap">
            <div className="flex items-center gap-2 text-sm">
              <StatusDot status={lastRun.status} />
              <span className="font-medium">{lastRun.status.replace(/_/g, " ")}</span>
            </div>
            <div className="text-sm text-gray-500 dark:text-gray-400">
              <span className="font-medium text-gray-700 dark:text-gray-300">{lastRun.source_table}</span>
            </div>
            {lastRun.started_at && (
              <div className="text-xs text-gray-400">
                {new Date(lastRun.started_at).toLocaleString()}
              </div>
            )}
            {TERMINAL.includes(lastRun.status) && (
              <Link to="/review" className="ml-auto text-xs px-3 py-1.5 rounded-lg bg-blue-600 text-white hover:bg-blue-700 transition-colors">
                Review Results
              </Link>
            )}
            {!TERMINAL.includes(lastRun.status) && (
              <div className="ml-auto text-xs text-blue-500 flex items-center gap-1.5">
                <span className="inline-block w-1.5 h-1.5 rounded-full bg-blue-500 animate-pulse" />
                Running...
              </div>
            )}
          </div>
        </div>
      )}

      {/* Getting Started Checklist */}
      {!steps.every((s) => s.done) && (
        <div className="card p-5 mb-8">
          <h2 className="text-sm font-semibold mb-3">Getting Started</h2>
          <div className="space-y-2">
            {steps.map((s) => (
              <Link key={s.label} to={s.to} className="flex items-center gap-3 text-sm hover:bg-gray-50 dark:hover:bg-gray-800/50 rounded-lg p-2 -mx-2 transition-colors">
                <span className={`w-5 h-5 rounded-full flex items-center justify-center text-xs font-bold shrink-0 ${
                  s.done ? "bg-emerald-100 dark:bg-emerald-900/30 text-emerald-600 dark:text-emerald-400" : "bg-gray-100 dark:bg-gray-700 text-gray-400"
                }`}>
                  {s.done ? "\u2713" : "\u00B7"}
                </span>
                <span className={s.done ? "line-through text-gray-400" : ""}>{s.label}</span>
              </Link>
            ))}
          </div>
        </div>
      )}
    </div>
  );
}
