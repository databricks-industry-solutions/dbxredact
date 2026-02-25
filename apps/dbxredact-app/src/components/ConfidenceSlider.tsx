interface Props {
  value: number;
  onChange: (v: number) => void;
  label?: string;
}

export default function ConfidenceSlider({ value, onChange, label }: Props) {
  return (
    <div className="flex items-center gap-3">
      <label className="text-sm text-gray-600 dark:text-gray-400 w-32">{label || "Threshold"}</label>
      <input
        type="range"
        min={0}
        max={1}
        step={0.05}
        value={value}
        onChange={(e) => onChange(parseFloat(e.target.value))}
        className="flex-1"
      />
      <span className="text-sm font-mono w-12 text-right">{value.toFixed(2)}</span>
    </div>
  );
}
