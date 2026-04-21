import { useState, useEffect } from "react";
import { useGet, apiPost } from "../hooks/useApi";
import TablePicker, { type TableRef, emptyTableRef, toQualified, isComplete } from "../components/TablePicker";
import ErrorBanner from "../components/ErrorBanner";
import { SkeletonRows } from "../components/Skeleton";
import ConfirmDialog from "../components/ConfirmDialog";
import DataTable, { type Column } from "../components/DataTable";
import { useToast } from "../hooks/useToast";
import type { ActiveLearnItem, ActiveLearnStats } from "../types";

export default function ActiveLearnPage() {
  const { data: queue, loading: queueLoading, refetch: refetchQueue, error: queueError } = useGet<ActiveLearnItem[]>("/active-learn/queue?status=pending");
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
      <div className="mb-6 rounded-lg bg-indigo-50 dark:bg-indigo-900/20 border border-indigo-200 dark:border-indigo-700 p-4 flex items-start gap-3">
        <span className="text-xs font-bold px-2 py-0.5 rounded bg-indigo-600 text-white shrink-0 mt-0.5">Preview</span>
        <div className="text-sm text-indigo-800 dark:text-indigo-200">
          Active Learning is in preview. Queue building works; inline entity correction is coming in a future release.
          Use the <a href="/labels" className="underline font-medium">Labeling page</a> for detailed annotation.
        </div>
      </div>
      <h2 className="page-title">Active Learning</h2>
      <p className="page-desc">
        Identifies documents where the detection model is least confident and queues them for human review,
        focusing labeling effort where it matters most.
      </p>

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
            <input type="number" className="input-field" min={1} value={topK}
              onChange={(e) => {
                const v = parseInt(e.target.value);
                if (!isNaN(v) && v > 0) setTopK(v);
              }} />
          </div>
          <button className="btn-primary" disabled={building || !hasTable} onClick={buildQueue}>
            {building ? "Building..." : "Build Queue"}
          </button>
        </div>
      </div>

      <h3 className="text-lg font-semibold mb-3">Review Queue</h3>
      <p className="text-xs text-gray-400 dark:text-gray-500 mb-3">
        "Mark Reviewed" removes a document from the pending queue. Entity-level corrections are not yet captured here -- use the Review page for detailed annotation.
      </p>
      {queueLoading ? <SkeletonRows rows={4} /> : (
        <DataTable<ActiveLearnItem & Record<string, unknown>>
          data={(queue ?? []) as (ActiveLearnItem & Record<string, unknown>)[]}
          rowKey={(item) => item.doc_id}
          emptyMessage="No items in queue. Build a queue from detection results to get started."
          columns={queueColumns}
        />
      )}
    </div>
  );
}
