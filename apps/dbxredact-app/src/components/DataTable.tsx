import { useState, useMemo } from "react";

export interface Column<T> {
  key: string;
  header: string;
  render?: (row: T) => React.ReactNode;
  sortable?: boolean;
  searchable?: boolean;
}

interface Props<T> {
  columns: Column<T>[];
  data: T[];
  pageSize?: number;
  rowKey: (row: T) => string;
  rowClassName?: (row: T) => string;
  emptyMessage?: string;
}

export default function DataTable<T extends Record<string, unknown>>({
  columns, data, pageSize = 10, rowKey, rowClassName, emptyMessage = "No data.",
}: Props<T>) {
  const [search, setSearch] = useState("");
  const [sortKey, setSortKey] = useState<string | null>(null);
  const [sortAsc, setSortAsc] = useState(true);
  const [page, setPage] = useState(0);

  const searchableCols = columns.filter((c) => c.searchable !== false);

  const filtered = useMemo(() => {
    if (!search.trim()) return data;
    const q = search.toLowerCase();
    return data.filter((row) =>
      searchableCols.some((col) => {
        const val = row[col.key];
        return val != null && String(val).toLowerCase().includes(q);
      }),
    );
  }, [data, search, searchableCols]);

  const sorted = useMemo(() => {
    if (!sortKey) return filtered;
    return [...filtered].sort((a, b) => {
      const av = a[sortKey] ?? "";
      const bv = b[sortKey] ?? "";
      const cmp = String(av).localeCompare(String(bv), undefined, { numeric: true });
      return sortAsc ? cmp : -cmp;
    });
  }, [filtered, sortKey, sortAsc]);

  const totalPages = Math.max(1, Math.ceil(sorted.length / pageSize));
  const safePage = Math.min(page, totalPages - 1);
  const pageData = sorted.slice(safePage * pageSize, (safePage + 1) * pageSize);

  function toggleSort(key: string) {
    if (sortKey === key) setSortAsc(!sortAsc);
    else { setSortKey(key); setSortAsc(true); }
  }

  return (
    <div>
      {data.length > 5 && (
        <div className="mb-3">
          <input
            className="input-field max-w-xs"
            placeholder="Search..."
            value={search}
            onChange={(e) => { setSearch(e.target.value); setPage(0); }}
          />
        </div>
      )}

      <div className="table-wrapper">
        <table className="data-table">
          <thead>
            <tr>
              {columns.map((col) => (
                <th
                  key={col.key}
                  className={col.sortable !== false ? "cursor-pointer select-none" : ""}
                  onClick={col.sortable !== false ? () => toggleSort(col.key) : undefined}
                >
                  {col.header}
                  {sortKey === col.key && (
                    <span className="ml-1 text-[10px]">{sortAsc ? "\u25B2" : "\u25BC"}</span>
                  )}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {pageData.length === 0 ? (
              <tr><td colSpan={columns.length} className="text-center text-sm text-gray-400 py-6">{emptyMessage}</td></tr>
            ) : (
              pageData.map((row) => (
                <tr key={rowKey(row)} className={rowClassName?.(row) ?? ""}>
                  {columns.map((col) => (
                    <td key={col.key}>
                      {col.render ? col.render(row) : (row[col.key] as React.ReactNode) ?? ""}
                    </td>
                  ))}
                </tr>
              ))
            )}
          </tbody>
        </table>
      </div>

      {totalPages > 1 && (
        <div className="flex items-center justify-between mt-3 text-sm text-gray-500 dark:text-gray-400">
          <span>{sorted.length} result{sorted.length !== 1 ? "s" : ""}</span>
          <div className="flex gap-1">
            <button className="btn-ghost text-xs" disabled={safePage === 0} onClick={() => setPage(safePage - 1)}>Prev</button>
            <span className="px-2 py-1 text-xs">{safePage + 1} / {totalPages}</span>
            <button className="btn-ghost text-xs" disabled={safePage >= totalPages - 1} onClick={() => setPage(safePage + 1)}>Next</button>
          </div>
        </div>
      )}
    </div>
  );
}
