import { useState } from "react";

interface Props {
  onFilter: (table: string) => void;
  placeholder?: string;
}

export default function FilterBar({ onFilter, placeholder }: Props) {
  const [value, setValue] = useState("");

  return (
    <div className="flex gap-2 mb-5">
      <input
        className="input-field flex-1"
        placeholder={placeholder || "Enter table name (catalog.schema.table)"}
        value={value}
        onChange={(e) => setValue(e.target.value)}
        onKeyDown={(e) => e.key === "Enter" && onFilter(value)}
      />
      <button className="btn-primary" onClick={() => onFilter(value)}>
        Load
      </button>
    </div>
  );
}
