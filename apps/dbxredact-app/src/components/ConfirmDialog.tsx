import { useEffect, useRef } from "react";

interface Props {
  open: boolean;
  title: string;
  message: string;
  confirmLabel?: string;
  variant?: "danger" | "default";
  onConfirm: () => void;
  onCancel: () => void;
}

export default function ConfirmDialog({
  open, title, message, confirmLabel = "Confirm", variant = "default", onConfirm, onCancel,
}: Props) {
  const dialogRef = useRef<HTMLDialogElement>(null);

  useEffect(() => {
    const el = dialogRef.current;
    if (!el) return;
    if (open && !el.open) el.showModal();
    else if (!open && el.open) el.close();
  }, [open]);

  if (!open) return null;

  const btnClass = variant === "danger" ? "btn-danger" : "btn-primary";

  return (
    <dialog
      ref={dialogRef}
      className="backdrop:bg-black/40 bg-white dark:bg-gray-800 rounded-xl shadow-xl p-0 max-w-sm w-full border border-gray-200 dark:border-gray-700"
      onClose={onCancel}
    >
      <div className="p-5">
        <h3 className="text-base font-semibold mb-2">{title}</h3>
        <p className="text-sm text-gray-600 dark:text-gray-300">{message}</p>
      </div>
      <div className="flex justify-end gap-2 px-5 pb-5">
        <button className="btn-ghost border border-gray-200 dark:border-gray-600" onClick={onCancel}>Cancel</button>
        <button className={btnClass} onClick={onConfirm}>{confirmLabel}</button>
      </div>
    </dialog>
  );
}
