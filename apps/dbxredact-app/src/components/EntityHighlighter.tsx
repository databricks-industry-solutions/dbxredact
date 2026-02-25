interface Entity {
  entity: string;
  entity_type: string;
  start: number;
  end: number;
  score?: number;
}

const TYPE_COLORS: Record<string, string> = {
  PERSON: "bg-amber-200/70 dark:bg-amber-800/40",
  EMAIL: "bg-blue-200/70 dark:bg-blue-800/40",
  EMAIL_ADDRESS: "bg-blue-200/70 dark:bg-blue-800/40",
  PHONE_NUMBER: "bg-emerald-200/70 dark:bg-emerald-800/40",
  LOCATION: "bg-purple-200/70 dark:bg-purple-800/40",
  DATE_TIME: "bg-pink-200/70 dark:bg-pink-800/40",
  US_SSN: "bg-red-200/70 dark:bg-red-800/40",
  ADDRESS: "bg-violet-200/70 dark:bg-violet-800/40",
};

export default function EntityHighlighter({
  text,
  entities,
}: {
  text: string;
  entities: Entity[];
}) {
  if (!entities?.length) return <p className="whitespace-pre-wrap leading-relaxed">{text}</p>;

  // Merge overlapping spans: keep the wider/higher-score entity when spans overlap
  const sorted = [...entities].sort((a, b) => a.start - b.start || b.end - a.end);
  const merged: Entity[] = [];
  for (const ent of sorted) {
    const prev = merged[merged.length - 1];
    if (prev && ent.start < prev.end) {
      if (ent.end > prev.end) prev.end = ent.end;
    } else {
      merged.push({ ...ent });
    }
  }

  const parts: JSX.Element[] = [];
  let cursor = 0;

  merged.forEach((ent, i) => {
    if (ent.start > cursor) {
      parts.push(<span key={`t${i}`}>{text.slice(cursor, ent.start)}</span>);
    }
    if (ent.start < cursor) return; // safety: skip if still behind
    const color = TYPE_COLORS[ent.entity_type] || "bg-orange-200/70 dark:bg-orange-800/40";
    parts.push(
      <span
        key={`e${i}`}
        className={`${color} rounded px-0.5 cursor-help border-b-2 border-current`}
        title={`${ent.entity_type} (${(ent.score ?? 0).toFixed(2)})`}
      >
        {text.slice(ent.start, ent.end)}
      </span>
    );
    cursor = ent.end;
  });

  if (cursor < text.length) {
    parts.push(<span key="tail">{text.slice(cursor)}</span>);
  }

  return <p className="whitespace-pre-wrap leading-relaxed">{parts}</p>;
}
