import { useState, useEffect } from "react";
import { useGet, apiPost } from "../hooks/useApi";
import TablePicker, { type TableRef, emptyTableRef, toQualified, isComplete } from "../components/TablePicker";
import ErrorBanner from "../components/ErrorBanner";
import ConfirmDialog from "../components/ConfirmDialog";
import DataTable, { type Column } from "../components/DataTable";
import { useToast } from "../hooks/useToast";
import type { ActiveLearnItem, ActiveLearnStats } from "../types";

export default function ActiveLearnPage() {
  const { data: queue, refetch: refetchQueue, error: queueError } = useGet<ActiveLearnItem[]>("/active-learn/queue?status=pending");
  const { data: stats, refetch: refetchStats, error: statsError } = useGet<ActiveLearnStats>("/active-learn/stats");
  const [detectionTable, setDetectionTable] = useState<TableRef>(emptyTableRef);
  const [topK, setTopK] = useState(100);
  const [building, setBuilding] = useState(false);
  const [error, setError] = useState("");
  const [reviewTarget, setReviewTarget] = useState<string | null>(null);
  const { toast } = useToast();

  useEffect(() => {
    if (queueError) setError(queueError);
    else if (statsError) setError(statsError);
  }, [queueError, statsError]);

  const hasTable = isComplete(detectionTable);

  async function buildQueue() {
    setBuilding(true);
    try {
      await apiPost("/active-learn/build-queue", {
        detection_table: toQualified(detectionTable),
        top_k: topK,
      });
      toast("Queue built");
      refetchQueue();
      refetchStats();
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : "Failed to build queue");
    }
    setBuilding(false);
  }

  async function confirmMarkReviewed() {
    if (!reviewTarget) return;
    try {
      await apiPost(`/active-learn/queue/${reviewTarget}/review`, { corrections: [] });
      toast("Marked as reviewed");
      refetchQueue();
      refetchStats();
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : "Failed to mark reviewed");
    }
    setReviewTarget(null);
  }

  const queueColumns: Column<ActiveLearnItem & Record<string, unknown>>[] = [
    { key: "doc_id", header: "Doc ID", render: (item) => <span className="font-mono text-xs">{item.doc_id}</span> },
    { key: "source_table", header: "Source Table", render: (item) => <span className="text-xs">{item.source_table}</span> },
    { key: "priority_score", header: "Priority Score", render: (item) => (
      <div className="flex items-center gap-2">
        <div className="w-16 h-1.5 rounded-full bg-gray-200 dark:bg-gray-700 overflow-hidden">
          <div className={`h-full rounded-full ${
            item.priority_score > 0.7 ? "bg-red-500" : item.priority_score > 0.4 ? "bg-amber-500" : "bg-emerald-500"
          }`} style={{ width: `${item.priority_score * 100}%` }} />
        </div>
        <span className="font-mono text-xs">{item.priority_score.toFixed(3)}</span>
      </div>
    )},
    { key: "status", header: "Status" },
    { key: "_actions", header: "", sortable: false, searchable: false, render: (item) => (
      <button className="text-blue-600 dark:text-blue-400 hover:text-blue-800 text-xs font-medium transition-colors"
        onClick={() => setReviewTarget(item.doc_id)}>Mark Reviewed</button>
    )},
  ];

  return (
    <div>
      <ConfirmDialog
        open={!!reviewTarget}
        title="Mark as Reviewed"
        message={`Mark document "${reviewTarget}" as reviewed? This will remove it from the pending queue.`}
        confirmLabel="Mark Reviewed"
        onConfirm={confirmMarkReviewed}
        onCancel={() => setReviewTarget(null)}
      />
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
      <DataTable<ActiveLearnItem & Record<string, unknown>>
        data={(queue ?? []) as (ActiveLearnItem & Record<string, unknown>)[]}
        rowKey={(item) => item.doc_id}
        emptyMessage="No items in queue. Build a queue from detection results to get started."
        columns={queueColumns}
      />
    </div>
  );
}
