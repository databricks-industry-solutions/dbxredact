import { useState } from "react";
import { useGet, apiPost } from "../hooks/useApi";
import ErrorBanner from "../components/ErrorBanner";
import { SkeletonRows } from "../components/Skeleton";
import DataTable, { type Column } from "../components/DataTable";
import ConfirmDialog from "../components/ConfirmDialog";
import TablePicker, { type TableRef, emptyTableRef, toQualified, isComplete } from "../components/TablePicker";
import { useToast } from "../hooks/useToast";

interface HealthResult {
  healthy: boolean;
  checks: {
    warehouse: { ok: boolean; warehouse_id?: string; error?: string };
    tables: Record<string, string>;
    job_env: Record<string, string>;
  };
}

interface RetentionRow {
  total_rows: number;
  oldest: string;
  newest: string;
  stale_rows: number;
}

interface RetentionStatus {
  retention_days: number;
  tables: Record<string, RetentionRow>;
}

interface AuditRow {
  run_id?: string;
  doc_id?: string;
  entity_type?: string;
  entity_count?: number;
  created_at?: string;
  [key: string]: unknown;
}

export default function AdminPage() {
  const { data: health, loading: loadingHealth, error: healthError, refetch: refetchHealth } = useGet<HealthResult>("/admin/health/deep");
  const { data: retention, loading: loadingRetention, error: retentionError, refetch: refetchRetention } = useGet<RetentionStatus>("/admin/retention-status");

  const [auditDays, setAuditDays] = useState(30);
  const [auditRunId, setAuditRunId] = useState("");
  const [auditEntityType, setAuditEntityType] = useState("");
  const { data: auditLog, loading: loadingAudit, error: auditError, refetch: refetchAudit } = useGet<{ rows: AuditRow[]; count: number }>(
    `/admin/audit-log?days=${auditDays}&limit=100${auditRunId ? `&run_id=${encodeURIComponent(auditRunId)}` : ""}${auditEntityType ? `&entity_type=${encodeURIComponent(auditEntityType)}` : ""}`,
    { deps: [auditDays, auditRunId, auditEntityType] },
  );

  const [error, setError] = useState("");
  const [purgeConfirm, setPurgeConfirm] = useState<"annotations" | "detection" | null>(null);
  const [purgeRetention, setPurgeRetention] = useState(90);
  const [detectionTable, setDetectionTable] = useState<TableRef>(emptyTableRef);
  const { toast } = useToast();

  const displayError = error || healthError || retentionError || auditError || "";

  async function purgeAnnotations() {
    try {
      const res = await apiPost<{ tables: Record<string, { purged: number }> }>(`/admin/purge-annotations?retention_days=${purgeRetention}`, {});
      const total = Object.values(res.tables).reduce((s, t) => s + t.purged, 0);
      toast(`Purged ${total} rows`);
      refetchRetention();
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : "Purge failed");
    }
    setPurgeConfirm(null);
  }

  async function purgeDetection() {
    const qt = toQualified(detectionTable);
    if (!qt) return;
    try {
      const res = await apiPost<{ purged: number }>(`/admin/purge-detection-results?table_name=${encodeURIComponent(qt)}`, {});
      toast(`Purged ${res.purged} rows from ${qt}`);
      refetchRetention();
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : "Purge failed");
    }
    setPurgeConfirm(null);
  }

  const auditColumns: Column<AuditRow>[] = [
    { key: "created_at", header: "Time", render: (r) => <span className="text-xs text-gray-500">{r.created_at}</span> },
    { key: "run_id", header: "Run ID", render: (r) => <span className="font-mono text-xs">{String(r.run_id ?? "").slice(0, 8)}</span> },
    { key: "doc_id", header: "Doc ID", render: (r) => <span className="font-mono text-xs">{String(r.doc_id ?? "").slice(0, 12)}</span> },
    { key: "entity_type", header: "Entity Type" },
    { key: "entity_count", header: "Count" },
  ];

  return (
    <div>
      <h2 className="page-title">Admin</h2>
      <p className="page-desc">System health, data retention, and audit log.</p>
      <ErrorBanner message={displayError} onDismiss={() => setError("")} />

      <ConfirmDialog
        open={purgeConfirm === "annotations"}
        title="Purge Annotations"
        message={`Delete annotations and ground truths older than ${purgeRetention} days? This cannot be undone.`}
        confirmLabel="Purge"
        variant="danger"
        onConfirm={purgeAnnotations}
        onCancel={() => setPurgeConfirm(null)}
      />
      <ConfirmDialog
        open={purgeConfirm === "detection"}
        title="Purge Detection Results"
        message={`Delete ALL rows from ${toQualified(detectionTable) || "(no table selected)"}? This cannot be undone.`}
        confirmLabel="Purge"
        variant="danger"
        onConfirm={purgeDetection}
        onCancel={() => setPurgeConfirm(null)}
      />

      {/* Health */}
      <section className="mb-8">
        <h3 className="text-lg font-semibold mb-3 flex items-center gap-2">
          System Health
          <button className="btn-secondary text-sm" onClick={refetchHealth}>Refresh</button>
        </h3>
        {loadingHealth && <SkeletonRows rows={3} />}
        {health && (
          <div className="card p-4 space-y-3">
            <div className="flex items-center gap-2">
              <span className={`inline-block w-2.5 h-2.5 rounded-full ${health.healthy ? "bg-emerald-500" : "bg-red-500"}`} />
              <span className="text-sm font-medium">{health.healthy ? "All systems healthy" : "Issues detected"}</span>
            </div>
            <div className="grid grid-cols-1 sm:grid-cols-3 gap-4 text-sm">
              <div>
                <div className="text-xs text-gray-500 mb-1">SQL Warehouse</div>
                <span className={health.checks.warehouse.ok ? "text-emerald-600" : "text-red-600"}>
                  {health.checks.warehouse.ok ? "Connected" : health.checks.warehouse.error || "Failed"}
                </span>
              </div>
              <div>
                <div className="text-xs text-gray-500 mb-1">Tables</div>
                {Object.entries(health.checks.tables).map(([name, status]) => (
                  <div key={name} className="flex items-center gap-1.5">
                    <span className={`w-1.5 h-1.5 rounded-full ${status === "ok" ? "bg-emerald-500" : "bg-red-500"}`} />
                    <span className="font-mono text-xs">{name}</span>
                    {status !== "ok" && <span className="text-xs text-red-500">({status})</span>}
                  </div>
                ))}
              </div>
              <div>
                <div className="text-xs text-gray-500 mb-1">Job Config</div>
                {Object.entries(health.checks.job_env).map(([name, status]) => (
                  <div key={name} className="flex items-center gap-1.5">
                    <span className={`w-1.5 h-1.5 rounded-full ${status === "set" ? "bg-emerald-500" : "bg-amber-500"}`} />
                    <span className="text-xs">{name}</span>
                  </div>
                ))}
              </div>
            </div>
          </div>
        )}
      </section>

      {/* Retention */}
      <section className="mb-8">
        <h3 className="text-lg font-semibold mb-3">Data Retention</h3>
        {loadingRetention && <SkeletonRows rows={2} />}
        {retention && (
          <div className="card p-4 space-y-4">
            <p className="text-xs text-gray-500">
              Retention policy: <b>{retention.retention_days} days</b>. Rows older than this are eligible for purging.
            </p>
            <div className="grid grid-cols-1 sm:grid-cols-2 gap-4">
              {Object.entries(retention.tables).map(([name, info]) => (
                <div key={name} className="border border-gray-200 dark:border-gray-700 rounded-lg p-3">
                  <div className="font-mono text-xs font-medium mb-1">{name}</div>
                  <div className="text-xs text-gray-500 space-y-0.5">
                    <div>Total: {info.total_rows.toLocaleString()} rows</div>
                    <div>Stale: <span className={info.stale_rows > 0 ? "text-amber-600 font-medium" : ""}>{info.stale_rows.toLocaleString()}</span></div>
                    {info.oldest && <div>Range: {info.oldest} - {info.newest}</div>}
                  </div>
                </div>
              ))}
            </div>

            <div className="flex items-end gap-3 flex-wrap pt-2 border-t border-gray-100 dark:border-gray-700">
              <div>
                <label className="block text-xs font-medium mb-1">Retention (days)</label>
                <input type="number" className="input-field w-24" value={purgeRetention}
                  onChange={(e) => setPurgeRetention(Number(e.target.value))} min={1} />
              </div>
              <button className="btn-danger text-sm" onClick={() => setPurgeConfirm("annotations")}>
                Purge Annotations
              </button>
            </div>

            <div className="flex items-end gap-3 flex-wrap pt-2 border-t border-gray-100 dark:border-gray-700">
              <div className="flex-1 min-w-[250px]">
                <TablePicker value={detectionTable} onChange={setDetectionTable} label="Detection Results Table" />
              </div>
              <button className="btn-danger text-sm" disabled={!isComplete(detectionTable)}
                onClick={() => setPurgeConfirm("detection")}>
                Purge Detection Results
              </button>
            </div>
          </div>
        )}
      </section>

      {/* Audit Log */}
      <section>
        <h3 className="text-lg font-semibold mb-3 flex items-center gap-2">
          Audit Log
          <button className="btn-secondary text-sm" onClick={refetchAudit}>Refresh</button>
        </h3>
        <div className="flex items-end gap-3 flex-wrap mb-4">
          <div>
            <label className="block text-xs font-medium mb-1">Days</label>
            <input type="number" className="input-field w-20" value={auditDays}
              onChange={(e) => setAuditDays(Number(e.target.value))} min={1} max={365} />
          </div>
          <div>
            <label className="block text-xs font-medium mb-1">Run ID</label>
            <input className="input-field w-40" value={auditRunId}
              onChange={(e) => setAuditRunId(e.target.value)} placeholder="optional" />
          </div>
          <div>
            <label className="block text-xs font-medium mb-1">Entity Type</label>
            <input className="input-field w-36" value={auditEntityType}
              onChange={(e) => setAuditEntityType(e.target.value)} placeholder="optional" />
          </div>
        </div>
        {loadingAudit && <SkeletonRows rows={4} />}
        <DataTable<AuditRow>
          data={auditLog?.rows ?? []}
          rowKey={(r, i) => `${r.run_id}-${r.doc_id}-${i}`}
          emptyMessage="No audit log entries found for the selected filters."
          columns={auditColumns}
        />
      </section>
    </div>
  );
}
