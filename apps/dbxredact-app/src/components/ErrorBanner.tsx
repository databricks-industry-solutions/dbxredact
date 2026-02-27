interface Props {
  message: string;
  onDismiss?: () => void;
}

export default function ErrorBanner({ message, onDismiss }: Props) {
  if (!message) return null;
  return (
    <div className="rounded-lg border border-red-300 bg-red-50 dark:bg-red-900/20 dark:border-red-700 px-4 py-3 text-sm text-red-800 dark:text-red-300 flex items-center justify-between">
      <span>{message}</span>
      {onDismiss && (
        <button onClick={onDismiss} className="ml-4 text-red-600 dark:text-red-400 hover:opacity-70 font-bold">
          &times;
        </button>
      )}
    </div>
  );
}
