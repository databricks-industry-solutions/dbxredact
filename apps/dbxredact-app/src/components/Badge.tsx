import type { ReactNode } from "react";

export type BadgeVariant = "running" | "success" | "error" | "neutral" | "info";

const VARIANT_CLASSES: Record<BadgeVariant, string> = {
  running: "bg-amber-100 dark:bg-amber-900/30 text-amber-700 dark:text-amber-300",
  success: "bg-emerald-100 dark:bg-emerald-900/30 text-emerald-700 dark:text-emerald-300",
  error: "bg-red-100 dark:bg-red-900/30 text-red-700 dark:text-red-300",
  neutral: "bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-300",
  info: "bg-blue-100 dark:bg-blue-900/30 text-blue-700 dark:text-blue-300",
};

/**
 * Maps a Databricks job status string to a Badge variant.
 * RUNNING/PENDING/QUEUED -> running, SUCCESS -> success, terminal-but-not-success
 * (FAILED/ERROR/TIMEDOUT/CANCELED) -> error, anything else -> neutral.
 */
export function statusToVariant(status: string | undefined | null): BadgeVariant {
  const s = (status || "").toUpperCase();
  if (s === "SUCCESS" || s === "SUCCEEDED") return "success";
  if (s === "RUNNING" || s === "PENDING" || s === "QUEUED" || s === "TERMINATING") return "running";
  if (s === "FAILED" || s === "ERROR" || s === "TIMEDOUT" || s === "CANCELED" || s === "CANCELLED")
    return "error";
  return "neutral";
}

/** Returns the pill classes for a job status string (variant palette, sans layout). */
export function statusBadgeClass(status: string | undefined | null): string {
  return VARIANT_CLASSES[statusToVariant(status)];
}

interface Props {
  children: ReactNode;
  variant?: BadgeVariant;
  className?: string;
}

/** Small pill/label. Pass `variant` directly, or use `<Badge variant={statusToVariant(s)}>`. */
export default function Badge({ children, variant = "neutral", className = "" }: Props) {
  return (
    <span
      className={`inline-block px-2 py-0.5 text-xs rounded-full font-medium ${VARIANT_CLASSES[variant]} ${className}`}
    >
      {children}
    </span>
  );
}
