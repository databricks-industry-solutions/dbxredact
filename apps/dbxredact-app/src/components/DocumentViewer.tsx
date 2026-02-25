import EntityHighlighter from "./EntityHighlighter";

interface Props {
  doc: Record<string, unknown>;
  textField?: string;
  entitiesField?: string;
}

export default function DocumentViewer({
  doc,
  textField = "text",
  entitiesField = "aligned_entities",
}: Props) {
  const text = (doc[textField] as string) || "";
  const entities = (doc[entitiesField] as Array<{
    entity: string;
    entity_type: string;
    start: number;
    end: number;
    score?: number;
  }>) || [];

  return (
    <div>
      <div className="text-xs text-gray-500 dark:text-gray-400 mb-2 font-mono">
        doc_id: {String(doc.doc_id)}
      </div>
      <EntityHighlighter text={text} entities={entities} />
    </div>
  );
}
