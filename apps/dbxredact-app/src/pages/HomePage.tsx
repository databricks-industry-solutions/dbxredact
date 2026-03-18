import { Link } from "react-router-dom";
import { useGet } from "../hooks/useApi";
import type { Config, JobHistoryItem, ActiveLearnStats } from "../types";

const sections = [
  {
    title: "Pipeline",
    desc: "Configure detection methods, run the PII redaction pipeline on Unity Catalog tables, and review estimated costs before launching.",
    links: [
      { to: "/config", label: "Configuration", sub: "Set up detection methods and endpoints" },
      { to: "/run", label: "Run Pipeline", sub: "Execute redaction on your data" },
    ],
  },
  {
    title: "Benchmarks + Analysis",
    desc: "Run benchmarks against labeled datasets, review detection results inline, and analyze precision, recall, and quality metrics.",
    links: [
      { to: "/benchmark", label: "Benchmark", sub: "Run detection on labeled data" },
      { to: "/review", label: "Review", sub: "Inspect and correct entity annotations" },
      { to: "/metrics", label: "Metrics", sub: "Precision, recall, and judge grades" },
    ],
  },
  {
    title: "Tuning",
    desc: "Fine-tune detection behavior with block/safe lists, manual labeling, A/B testing, and active learning workflows.",
    links: [
      { to: "/lists", label: "Block / Safe Lists", sub: "Force or suppress specific detections" },
      { to: "/labels", label: "Labeling", sub: "Manually annotate documents" },
      { to: "/ab-tests", label: "A/B Testing", sub: "Compare configuration variants" },
      { to: "/active-learn", label: "Active Learning", sub: "Prioritize uncertain documents for review" },
    ],
    badge: "BETA",
  },
];

interface AuditSummary {
  total_runs?: number;
  total_docs?: number;
  total_entities?: number;
}

export default function HomePage() {
  const { data: configs } = useGet<Config[]>("/config/");
  const { data: history } = useGet<JobHistoryItem[]>("/pipeline/history?limit=3");
  const { data: alStats } = useGet<ActiveLearnStats>("/active-learn/stats");
  const { data: auditSummary } = useGet<AuditSummary>("/admin/audit-summary");

  const configCount = configs?.length ?? 0;
  const recentRuns = history ?? [];
  const hasConfigs = configCount > 0;
  const hasRuns = recentRuns.length > 0;
  const successRun = recentRuns.find((r) => r.status === "TERMINATED" || r.status === "SUCCESS");

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

      {/* Quick Stats */}
      <div className="grid grid-cols-2 sm:grid-cols-4 gap-4 mb-8">
        <div className="stat-card">
          <div className="stat-label">Configs</div>
          <div className="stat-value">{configCount}</div>
        </div>
        <div className="stat-card">
          <div className="stat-label">Recent Runs</div>
          <div className="stat-value">{recentRuns.length}</div>
        </div>
        <div className="stat-card">
          <div className="stat-label">Active Learn Queue</div>
          <div className="stat-value">{alStats?.pending ?? 0}</div>
        </div>
        <div className="stat-card">
          <div className="stat-label">Docs Redacted</div>
          <div className="stat-value">{auditSummary?.total_docs ?? 0}</div>
        </div>
      </div>

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

      <div className="space-y-6">
        {sections.map((sec) => (
          <div key={sec.title} className="card p-5">
            <div className="flex items-center gap-2 mb-1">
              <h2 className="text-lg font-semibold">{sec.title}</h2>
              {sec.badge && (
                <span className="text-[10px] px-2 py-0.5 rounded-full bg-amber-500/20 text-amber-600 dark:text-amber-400 font-medium">
                  {sec.badge}
                </span>
              )}
            </div>
            <p className="text-sm text-gray-500 dark:text-gray-400 mb-4">{sec.desc}</p>
            <div className="grid grid-cols-2 md:grid-cols-3 gap-3">
              {sec.links.map((l) => (
                <Link key={l.to} to={l.to}
                  className="block p-3 rounded-lg border border-gray-200 dark:border-gray-700 hover:border-blue-400 dark:hover:border-blue-500 hover:bg-blue-50/50 dark:hover:bg-blue-900/10 transition-colors">
                  <div className="text-sm font-medium text-blue-600 dark:text-blue-400">{l.label}</div>
                  <div className="text-xs text-gray-500 dark:text-gray-400 mt-0.5">{l.sub}</div>
                </Link>
              ))}
            </div>
          </div>
        ))}
      </div>
    </div>
  );
}
