import { useState, useEffect, useCallback } from "react";
import { NavLink, Outlet, useLocation } from "react-router-dom";

interface SidebarLink {
  to: string;
  label: string;
  preview?: boolean;
}

interface SidebarSection {
  label: string;
  hint?: string;
  links: SidebarLink[];
}

const sections: SidebarSection[] = [
  {
    label: "Pipeline",
    links: [
      { to: "/", label: "Home" },
      { to: "/config", label: "Config" },
      { to: "/run", label: "Run Pipeline" },
      { to: "/review", label: "Review" },
    ],
  },
  {
    label: "Benchmarks + Analysis",
    hint: "For developers running custom benchmarks",
    links: [
      { to: "/benchmark", label: "Benchmark" },
      { to: "/metrics", label: "Metrics" },
    ],
  },
  {
    label: "Tuning",
    links: [
      { to: "/lists", label: "Block / Safe Lists" },
      { to: "/labels", label: "Labeling" },
      { to: "/ab-tests", label: "A/B Testing", preview: true },
      { to: "/active-learn", label: "Active Learning", preview: true },
    ],
  },
];

function Hamburger({ onClick }: { onClick: () => void }) {
  return (
    <button onClick={onClick} className="lg:hidden p-2 rounded-lg hover:bg-gray-100 dark:hover:bg-gray-800 transition-colors" aria-label="Open menu">
      <svg className="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
        <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M4 6h16M4 12h16M4 18h16" />
      </svg>
    </button>
  );
}

const TERMINAL_STATES = ["TERMINATED", "SUCCESS", "SKIPPED", "INTERNAL_ERROR"];
const POLL_INTERVAL = 30_000;

interface RunStatusInfo {
  status: string;
  source_table?: string;
}

function useRunStatus(): RunStatusInfo | null {
  const [info, setInfo] = useState<RunStatusInfo | null>(null);

  const poll = useCallback(() => {
    fetch("/api/pipeline/history?limit=1")
      .then((r) => (r.ok ? r.json() : null))
      .then((rows) => {
        if (Array.isArray(rows) && rows.length > 0) {
          setInfo({ status: rows[0].status, source_table: rows[0].source_table });
        } else {
          setInfo(null);
        }
      })
      .catch(() => setInfo(null));
  }, []);

  useEffect(() => {
    poll();
    const id = setInterval(poll, POLL_INTERVAL);
    return () => clearInterval(id);
  }, [poll]);

  return info;
}

function RunIndicator({ info }: { info: RunStatusInfo }) {
  const running = !TERMINAL_STATES.includes(info.status);
  if (!running) return null;
  return (
    <NavLink to="/run" className="flex items-center gap-2 px-4 py-2 text-xs text-gray-300 hover:text-white transition-colors">
      <span className="w-2 h-2 rounded-full bg-blue-500 animate-pulse shrink-0" />
      <span className="truncate">Pipeline running...</span>
    </NavLink>
  );
}

function SidebarContent({ dark, setDark, runStatus }: { dark: boolean; setDark: (v: boolean) => void; runStatus: RunStatusInfo | null }) {
  return (
    <>
      <div className="flex items-center justify-between px-5 mb-6">
        <div className="flex items-center gap-2.5">
          <div className="w-8 h-8 rounded-lg bg-gradient-to-br from-blue-500 to-indigo-600 flex items-center justify-center text-white text-xs font-bold">
            R
          </div>
          <span className="text-base font-semibold text-white tracking-tight">dbxredact</span>
        </div>
        <button
          onClick={() => setDark(!dark)}
          className="text-xs px-2 py-1 rounded-md bg-gray-800 hover:bg-gray-700 text-gray-400 hover:text-gray-200 transition-colors"
          title="Toggle dark mode"
        >
          {dark ? "Light" : "Dark"}
        </button>
      </div>
      <div className="flex-1 overflow-y-auto px-3 space-y-5">
        {sections.map((sec) => (
          <div key={sec.label}>
            <div className="px-3 mb-1.5 text-[10px] uppercase tracking-widest text-gray-500 font-semibold">
              {sec.label}
            </div>
            {sec.hint && (
              <div className="px-3 mb-1.5 text-[10px] text-gray-600 leading-tight">{sec.hint}</div>
            )}
            <div className="space-y-0.5">
              {sec.links.map((l) => (
                <NavLink
                  key={l.to}
                  to={l.to}
                  end={l.to === "/"}
                  className={({ isActive }) =>
                    `flex items-center gap-2 px-3 py-2 rounded-lg text-sm transition-colors ${
                      isActive
                        ? "bg-blue-600/90 text-white font-medium shadow-sm"
                        : "hover:bg-white/5 hover:text-white"
                    }`
                  }
                >
                  {l.label}
                  {l.preview && (
                    <span className="text-[9px] px-1.5 py-0.5 rounded bg-gray-700 text-gray-400 font-medium leading-none">
                      Preview
                    </span>
                  )}
                </NavLink>
              ))}
            </div>
          </div>
        ))}
      </div>
      {runStatus && <RunIndicator info={runStatus} />}
    </>
  );
}

export default function Layout() {
  const [dark, setDark] = useState(() => {
    if (typeof window !== "undefined") {
      return localStorage.getItem("theme") === "dark" ||
        (!localStorage.getItem("theme") && window.matchMedia("(prefers-color-scheme: dark)").matches);
    }
    return false;
  });
  const [sidebarOpen, setSidebarOpen] = useState(false);
  const location = useLocation();
  const runStatus = useRunStatus();

  useEffect(() => {
    document.documentElement.classList.toggle("dark", dark);
    localStorage.setItem("theme", dark ? "dark" : "light");
  }, [dark]);

  useEffect(() => { setSidebarOpen(false); }, [location.pathname]);

  return (
    <div className="flex h-screen bg-gray-50 dark:bg-gray-900 text-gray-900 dark:text-gray-100">
      {/* Desktop sidebar */}
      <nav className="hidden lg:flex w-60 bg-gray-900 dark:bg-gray-950 text-gray-300 flex-col py-5 shrink-0 border-r border-gray-800">
        <SidebarContent dark={dark} setDark={setDark} runStatus={runStatus} />
      </nav>

      {/* Mobile sidebar overlay */}
      {sidebarOpen && (
        <div className="fixed inset-0 z-40 lg:hidden" onClick={() => setSidebarOpen(false)}>
          <div className="absolute inset-0 bg-black/50" />
          <nav
            className="relative w-60 h-full bg-gray-900 dark:bg-gray-950 text-gray-300 flex flex-col py-5 shadow-xl"
            onClick={(e) => e.stopPropagation()}
          >
            <SidebarContent dark={dark} setDark={setDark} runStatus={runStatus} />
          </nav>
        </div>
      )}

      <main className="flex-1 overflow-y-auto">
        <div className="max-w-5xl mx-auto p-4 sm:p-6 lg:p-8">
          <div className="lg:hidden mb-4">
            <Hamburger onClick={() => setSidebarOpen(true)} />
          </div>
          <Outlet />
        </div>
      </main>
    </div>
  );
}
