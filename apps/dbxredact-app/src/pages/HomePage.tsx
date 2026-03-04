import { Link } from "react-router-dom";

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

export default function HomePage() {
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
