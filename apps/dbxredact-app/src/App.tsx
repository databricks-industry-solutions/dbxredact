import { Component, type ReactNode } from "react";
import { Routes, Route } from "react-router-dom";
import Layout from "./components/Layout";
import HomePage from "./pages/HomePage";
import ConfigPage from "./pages/ConfigPage";
import RunPage from "./pages/RunPage";
import BenchmarkPage from "./pages/BenchmarkPage";
import ReviewPage from "./pages/ReviewPage";
import MetricsPage from "./pages/MetricsPage";
import ListsPage from "./pages/ListsPage";
import LabelPage from "./pages/LabelPage";
import ABTestPage from "./pages/ABTestPage";
import ActiveLearnPage from "./pages/ActiveLearnPage";

class AppErrorBoundary extends Component<
  { children: ReactNode },
  { error: string | null }
> {
  state = { error: null as string | null };

  static getDerivedStateFromError(err: Error) {
    return { error: err.message || "An unexpected error occurred." };
  }

  render() {
    if (this.state.error) {
      return (
        <div className="min-h-screen flex items-center justify-center bg-gray-50 dark:bg-gray-900 p-8">
          <div className="max-w-lg w-full text-center">
            <h1 className="text-2xl font-bold text-red-700 dark:text-red-400 mb-3">
              Something went wrong
            </h1>
            <p className="text-sm text-gray-600 dark:text-gray-400 mb-6">
              {this.state.error}
            </p>
            <button
              className="px-4 py-2 rounded-lg bg-blue-600 text-white text-sm font-medium hover:bg-blue-700 transition-colors"
              onClick={() => {
                this.setState({ error: null });
                window.location.reload();
              }}
            >
              Reload Page
            </button>
          </div>
        </div>
      );
    }
    return this.props.children;
  }
}

export default function App() {
  return (
    <AppErrorBoundary>
      <Routes>
        <Route element={<Layout />}>
          <Route index element={<HomePage />} />
          <Route path="config" element={<ConfigPage />} />
          <Route path="run" element={<RunPage />} />
          <Route path="benchmark" element={<BenchmarkPage />} />
          <Route path="review" element={<ReviewPage />} />
          <Route path="metrics" element={<MetricsPage />} />
          <Route path="lists" element={<ListsPage />} />
          <Route path="labels" element={<LabelPage />} />
          <Route path="ab-tests" element={<ABTestPage />} />
          <Route path="active-learn" element={<ActiveLearnPage />} />
        </Route>
      </Routes>
    </AppErrorBoundary>
  );
}
