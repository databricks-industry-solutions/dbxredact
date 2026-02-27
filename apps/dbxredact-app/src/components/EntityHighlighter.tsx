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
  showIndices = false,
}: {
  text: string;
  entities: Entity[];
  showIndices?: boolean;
}) {
  if (!entities?.length) return <p className="whitespace-pre-wrap leading-relaxed">{text}</p>;

  // Build render list: sort by start, then widest first. Track original index.
  const indexed = entities.map((e, i) => ({ ...e, idx: i }));
  const sorted = [...indexed].sort((a, b) => a.start - b.start || b.end - a.end);

  // Resolve overlaps: skip entities whose start falls inside an already-claimed span.
  // For overlapping entities that share the same start, group their indices together.
  const segments: { start: number; end: number; entity_type: string; score?: number; indices: number[] }[] = [];
  for (const ent of sorted) {
    const last = segments[segments.length - 1];
    if (last && ent.start < last.end) {
      // Overlapping -- add this entity's index to the existing segment
      last.indices.push(ent.idx);
      if (ent.end > last.end) last.end = ent.end;
    } else {
      segments.push({
        start: ent.start,
        end: ent.end,
        entity_type: ent.entity_type,
        score: ent.score,
        indices: [ent.idx],
      });
    }
  }

  const parts: JSX.Element[] = [];
  let cursor = 0;

  segments.forEach((seg, i) => {
    if (seg.start > cursor) {
      parts.push(<span key={`t${i}`}>{text.slice(cursor, seg.start)}</span>);
    }
    if (seg.start < cursor) return;
    const color = TYPE_COLORS[seg.entity_type] || "bg-orange-200/70 dark:bg-orange-800/40";
    parts.push(
      <span
        key={`e${i}`}
        className={`${color} rounded px-0.5 cursor-help border-b-2 border-current`}
        title={`${seg.entity_type} (${(seg.score ?? 0).toFixed(2)})`}
      >
        {text.slice(seg.start, seg.end)}
        {showIndices && (
          <sup className="text-[9px] font-bold ml-0.5 opacity-70">
            {seg.indices.map((idx) => idx + 1).join(",")}
          </sup>
        )}
      </span>
    );
    cursor = seg.end;
  });

  if (cursor < text.length) {
    parts.push(<span key="tail">{text.slice(cursor)}</span>);
  }

  return <p className="whitespace-pre-wrap leading-relaxed">{parts}</p>;
}
