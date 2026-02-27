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

export default function App() {
  return (
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
  );
}
