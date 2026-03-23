interface Tab {
  key: string;
  label: string | React.ReactNode;
}

interface Props {
  tabs: Tab[];
  active: string;
  onChange: (key: string) => void;
  variant?: "underline" | "pill";
  className?: string;
}

export default function Tabs({ tabs, active, onChange, variant = "underline", className = "" }: Props) {
  const isUnderline = variant === "underline";

  return (
    <div
      role="tablist"
      className={`flex ${isUnderline ? "gap-1 border-b border-gray-200 dark:border-gray-700 mb-6" : "gap-2 mb-5"} ${className}`}
    >
      {tabs.map((t) => {
        const selected = t.key === active;
        const base = isUnderline
          ? `px-4 py-2.5 text-sm font-medium border-b-2 transition-colors -mb-px ${
              selected
                ? "border-blue-600 text-blue-600 dark:text-blue-400"
                : "border-transparent text-gray-500 hover:text-gray-700 dark:text-gray-400 dark:hover:text-gray-200"
            }`
          : `px-4 py-2 rounded-lg text-sm font-medium transition-colors ${
              selected
                ? "bg-blue-600 text-white shadow-sm"
                : "bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-300 hover:bg-gray-200 dark:hover:bg-gray-600"
            }`;

        return (
          <button
            key={t.key}
            role="tab"
            aria-selected={selected}
            aria-controls={`tabpanel-${t.key}`}
            id={`tab-${t.key}`}
            className={base}
            onClick={() => onChange(t.key)}
          >
            {t.label}
          </button>
        );
      })}
    </div>
  );
}

export function TabPanel({ id, children, className = "" }: { id: string; children: React.ReactNode; className?: string }) {
  return (
    <div role="tabpanel" id={`tabpanel-${id}`} aria-labelledby={`tab-${id}`} className={className}>
      {children}
    </div>
  );
}
