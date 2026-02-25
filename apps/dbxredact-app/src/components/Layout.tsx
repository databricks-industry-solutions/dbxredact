import { useState, useEffect } from "react";
import { NavLink, Outlet } from "react-router-dom";

const sections = [
  {
    label: "Pipeline",
    links: [
      { to: "/", label: "Config" },
      { to: "/run", label: "Run Pipeline" },
      { to: "/benchmark", label: "Benchmark" },
    ],
  },
  {
    label: "Analysis",
    links: [
      { to: "/review", label: "Review" },
      { to: "/metrics", label: "Metrics" },
    ],
  },
  {
    label: "Tuning",
    links: [
      { to: "/lists", label: "Deny/Allow Lists" },
      { to: "/labels", label: "Labeling" },
      { to: "/ab-tests", label: "A/B Testing" },
      { to: "/active-learn", label: "Active Learning" },
    ],
  },
];

export default function Layout() {
  const [dark, setDark] = useState(() => {
    if (typeof window !== "undefined") {
      return localStorage.getItem("theme") === "dark" ||
        (!localStorage.getItem("theme") && window.matchMedia("(prefers-color-scheme: dark)").matches);
    }
    return false;
  });

  useEffect(() => {
    document.documentElement.classList.toggle("dark", dark);
    localStorage.setItem("theme", dark ? "dark" : "light");
  }, [dark]);

  return (
    <div className="flex h-screen bg-gray-50 dark:bg-gray-900 text-gray-900 dark:text-gray-100">
      <nav className="w-60 bg-gray-900 dark:bg-gray-950 text-gray-300 flex flex-col py-5 shrink-0 border-r border-gray-800">
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
              <div className="px-3 mb-1.5 text-[10px] uppercase tracking-widest text-gray-500 font-semibold flex items-center gap-2">
                {sec.label}
                {sec.label === "Tuning" && (
                  <span className="normal-case tracking-normal text-[9px] px-1.5 py-0.5 rounded bg-amber-500/20 text-amber-400 font-medium">
                    Under Active Development
                  </span>
                )}
              </div>
              <div className="space-y-0.5">
                {sec.links.map((l) => (
                  <NavLink
                    key={l.to}
                    to={l.to}
                    end={l.to === "/"}
                    className={({ isActive }) =>
                      `block px-3 py-2 rounded-lg text-sm transition-colors ${
                        isActive
                          ? "bg-blue-600/90 text-white font-medium shadow-sm"
                          : "hover:bg-white/5 hover:text-white"
                      }`
                    }
                  >
                    {l.label}
                  </NavLink>
                ))}
              </div>
            </div>
          ))}
        </div>
      </nav>

      <main className="flex-1 overflow-y-auto">
        <div className="max-w-5xl mx-auto p-8">
          <Outlet />
        </div>
      </main>
    </div>
  );
}
