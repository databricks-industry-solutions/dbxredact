import { useGet } from "../hooks/useApi";

export interface TableRef {
  catalog: string;
  schema: string;
  table: string;
}

export const emptyTableRef: TableRef = { catalog: "", schema: "", table: "" };

export function toQualified(v: TableRef): string {
  return `${v.catalog}.${v.schema}.${v.table}`;
}

export function isComplete(v: TableRef): boolean {
  return !!(v.catalog && v.schema && v.table);
}

interface Props {
  value: TableRef;
  onChange: (val: TableRef) => void;
  label?: string;
}

export default function TablePicker({ value, onChange, label }: Props) {
  const { data: catalogs } = useGet<string[]>("/catalog/catalogs");
  const { data: schemas } = useGet<string[]>(
    `/catalog/schemas?catalog=${encodeURIComponent(value.catalog)}`,
    { enabled: !!value.catalog, deps: [value.catalog] },
  );
  const { data: tables } = useGet<string[]>(
    `/catalog/tables?catalog=${encodeURIComponent(value.catalog)}&schema=${encodeURIComponent(value.schema)}`,
    { enabled: !!(value.catalog && value.schema), deps: [value.catalog, value.schema] },
  );

  return (
    <div>
      {label && <label className="block text-sm font-medium mb-1.5">{label}</label>}
      <div className="grid grid-cols-3 gap-2">
        <select className="input-field" value={value.catalog}
          onChange={(e) => onChange({ catalog: e.target.value, schema: "", table: "" })}>
          <option value="">Catalog...</option>
          {catalogs?.map((c) => <option key={c} value={c}>{c}</option>)}
        </select>
        <select className="input-field" value={value.schema}
          onChange={(e) => onChange({ ...value, schema: e.target.value, table: "" })}
          disabled={!value.catalog}>
          <option value="">Schema...</option>
          {schemas?.map((s) => <option key={s} value={s}>{s}</option>)}
        </select>
        <select className="input-field" value={value.table}
          onChange={(e) => onChange({ ...value, table: e.target.value })}
          disabled={!value.schema}>
          <option value="">Table...</option>
          {tables?.map((t) => <option key={t} value={t}>{t}</option>)}
        </select>
      </div>
    </div>
  );
}
