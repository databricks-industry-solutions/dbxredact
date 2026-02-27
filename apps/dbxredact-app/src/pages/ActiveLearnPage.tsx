import { useState, useEffect } from "react";
import { useGet, apiPost } from "../hooks/useApi";
import TablePicker from "../components/TablePicker";
import ErrorBanner from "../components/ErrorBanner";
import type { ActiveLearnItem, ActiveLearnStats } from "../types";

export default function ActiveLearnPage() {
  const { data: queue, refetch: refetchQueue, error: queueError } = useGet<ActiveLearnItem[]>("/active-learn/queue?status=pending");
  const { data: stats, refetch: refetchStats, error: statsError } = useGet<ActiveLearnStats>("/active-learn/stats");
  const [detectionTable, setDetectionTable] = useState("");
  const [topK, setTopK] = useState(100);
  const [building, setBuilding] = useState(false);
  const [error, setError] = useState("");

  useEffect(() => {
    if (queueError) setError(queueError);
    else if (statsError) setError(statsError);
  }, [queueError, statsError]);

  const parts = detectionTable.split(".");
  const hasTable = parts.length === 3 && parts[2] !== "";

  async function buildQueue() {
    setBuilding(true);
    try {
      await apiPost("/active-learn/build-queue", {
        detection_table: detectionTable,
        top_k: topK,
      });
      refetchQueue();
      refetchStats();
    } catch (e: any) {
      setError(e.message || "Failed to build queue");
    }
    setBuilding(false);
  }

  async function markReviewed(docId: string) {
    try {
      await apiPost(`/active-learn/queue/${docId}/review`, { corrections: [] });
      refetchQueue();
      refetchStats();
    } catch (e: any) {
      setError(e.message || "Failed to mark reviewed");
    }
  }

  return (
    <div>
      <ErrorBanner message={error} onDismiss={() => setError("")} />
      <h2 className="page-title">Active Learning</h2>
      <p className="page-desc">
        Active learning identifies the documents where the detection model is <b>least confident</b> and
        queues them for human review. This focuses labeling effort where it matters most -- on
        ambiguous cases that, once corrected, provide the highest-value training signal.
      </p>

      <div className="card p-4 mb-6">
        <div className="text-xs text-gray-500 dark:text-gray-400 leading-relaxed space-y-1">
          <p><b>How it works:</b> The system explodes the entity arrays from a detection table, computes
            the average and minimum confidence scores per document, and ranks documents by ascending
            confidence. The top-K lowest-confidence documents are added to the review queue.</p>
          <p><b>Workflow:</b> 1) Point to any detection results table. 2) Set how many documents to queue.
            3) Click "Build Queue." 4) Review queued documents in priority order on this page (or on the Review tab for
            richer annotation). 5) Annotations are saved to the unified <code>redact_annotations</code> table and
            can be used for evaluation benchmarks or model fine-tuning.</p>
        </div>
      </div>

      {stats && (
        <div className="grid grid-cols-4 gap-4 mb-6">
          {[
            { label: "Total Queued", val: stats.total_queued },
            { label: "Pending", val: stats.pending },
            { label: "Reviewed", val: stats.reviewed },
            { label: "Avg Priority", val: stats.avg_priority?.toFixed(3) ?? "N/A" },
          ].map((s) => (
            <div key={s.label} className="stat-card">
              <div className="stat-label">{s.label}</div>
              <div className="stat-value">{s.val}</div>
            </div>
          ))}
        </div>
      )}

      <div className="card p-5 mb-6 max-w-2xl space-y-4">
        <TablePicker value={detectionTable} onChange={setDetectionTable} label="Detection Results Table" />
        <div className="flex gap-3 items-end">
          <div className="w-32">
            <label className="block text-sm font-medium mb-1.5">Top K</label>
            <input type="number" className="input-field" value={topK}
              onChange={(e) => setTopK(parseInt(e.target.value))} />
          </div>
          <button className="btn-primary" disabled={building || !hasTable} onClick={buildQueue}>
            {building ? "Building..." : "Build Queue"}
          </button>
        </div>
      </div>

      <h3 className="text-lg font-semibold mb-3">Review Queue</h3>
      {queue?.length ? (
        <table className="data-table">
          <thead>
            <tr>
              <th>Doc ID</th>
              <th>Source Table</th>
              <th>Priority Score</th>
              <th>Status</th>
              <th></th>
            </tr>
          </thead>
          <tbody>
            {queue.map((item) => (
              <tr key={item.doc_id}>
                <td className="font-mono text-xs">{item.doc_id}</td>
                <td className="text-xs">{item.source_table}</td>
                <td>
                  <div className="flex items-center gap-2">
                    <div className="w-16 h-1.5 rounded-full bg-gray-200 dark:bg-gray-700 overflow-hidden">
                      <div className={`h-full rounded-full ${
                        item.priority_score > 0.7 ? "bg-red-500" : item.priority_score > 0.4 ? "bg-amber-500" : "bg-emerald-500"
                      }`} style={{ width: `${item.priority_score * 100}%` }} />
                    </div>
                    <span className="font-mono text-xs">{item.priority_score.toFixed(3)}</span>
                  </div>
                </td>
                <td>{item.status}</td>
                <td>
                  <button className="text-blue-600 dark:text-blue-400 hover:text-blue-800 text-xs font-medium transition-colors"
                    onClick={() => markReviewed(item.doc_id)}>
                    Mark Reviewed
                  </button>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      ) : (
        <p className="text-sm text-gray-400">No items in queue. Build a queue from detection results to get started.</p>
      )}
    </div>
  );
}
