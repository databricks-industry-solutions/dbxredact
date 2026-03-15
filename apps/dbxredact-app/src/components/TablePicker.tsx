import { useState, useEffect } from "react";

interface Props {
  value: string;
  onChange: (qualified: string) => void;
  label?: string;
}

export default function TablePicker({ value, onChange, label }: Props) {
  const [catalogs, setCatalogs] = useState<string[]>([]);
  const [schemas, setSchemas] = useState<string[]>([]);
  const [tables, setTables] = useState<string[]>([]);

  const parts = value.split(".");
  const catalog = parts[0] || "";
  const schema = parts[1] || "";
  const table = parts[2] || "";

  useEffect(() => {
    fetch("/api/catalog/catalogs").then(r => r.json()).then(setCatalogs).catch(() => {});
  }, []);

  useEffect(() => {
    if (!catalog) { setSchemas([]); return; }
    fetch(`/api/catalog/schemas?catalog=${encodeURIComponent(catalog)}`)
      .then(r => r.json()).then(setSchemas).catch(() => setSchemas([]));
  }, [catalog]);

  useEffect(() => {
    if (!catalog || !schema) { setTables([]); return; }
    fetch(`/api/catalog/tables?catalog=${encodeURIComponent(catalog)}&schema=${encodeURIComponent(schema)}`)
      .then(r => r.json()).then(setTables).catch(() => setTables([]));
  }, [catalog, schema]);

  function setCatalog(c: string) { onChange(`${c}..`); }
  function setSchema(s: string) { onChange(`${catalog}.${s}.`); }
  function setTable(t: string) { onChange(`${catalog}.${schema}.${t}`); }

  return (
    <div>
      {label && <label className="block text-sm font-medium mb-1.5">{label}</label>}
      <div className="grid grid-cols-3 gap-2">
        <select className="input-field" value={catalog} onChange={e => setCatalog(e.target.value)}>
          <option value="">Catalog...</option>
          {catalogs.map(c => <option key={c} value={c}>{c}</option>)}
        </select>
        <select className="input-field" value={schema} onChange={e => setSchema(e.target.value)} disabled={!catalog}>
          <option value="">Schema...</option>
          {schemas.map(s => <option key={s} value={s}>{s}</option>)}
        </select>
        <select className="input-field" value={table} onChange={e => setTable(e.target.value)} disabled={!schema}>
          <option value="">Table...</option>
          {tables.map(t => <option key={t} value={t}>{t}</option>)}
        </select>
      </div>
    </div>
  );
}
