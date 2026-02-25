"""GLiNER-based PHI/PII detection using HuggingFace transformers."""

import json
import logging
import re
from typing import Iterator, List, Tuple

import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql.functions import pandas_udf, col, from_json

from .config import (
    DEFAULT_GLINER_MODEL,
    DEFAULT_GLINER_LABELS,
    DEFAULT_GLINER_THRESHOLD,
    DEFAULT_GLINER_THRESHOLDS_BY_TYPE,
    GLINER_LABEL_MAP,
    should_ignore_entity,
)

logger = logging.getLogger(__name__)

MAX_WORDS = 256
OVERLAP_WORDS = 50

_WHITESPACE_RE = re.compile(r"\s+")

# Per-worker model cache (never serialized by Spark)
_gliner_model_cache = {}


def _get_cached_gliner_model(model_name: str):
    """Get or create cached GLiNER model instance (singleton per worker)."""
    if model_name not in _gliner_model_cache:
        from gliner import GLiNER
        _gliner_model_cache[model_name] = GLiNER.from_pretrained(model_name)
    return _gliner_model_cache[model_name]


def _find_word_boundaries(text: str) -> List[Tuple[int, int]]:
    """Return (start, end) character positions for each whitespace-delimited word."""
    return [(m.start(), m.end()) for m in re.finditer(r"\S+", text)]


def _build_offset_map(original: str) -> List[int]:
    """Map each character position in normalized text to its position in original.

    Mirrors the behaviour of ``_WHITESPACE_RE.sub(" ", original).strip()``:
    leading/trailing whitespace is dropped and interior runs of whitespace
    collapse to a single space.
    """
    mapping: List[int] = []
    oi = 0
    # skip leading whitespace (mirrors .strip())
    while oi < len(original) and original[oi] in " \t\n\r":
        oi += 1
    in_ws = False
    for oi_scan in range(oi, len(original)):
        ch = original[oi_scan]
        if ch in " \t\n\r":
            if not in_ws:
                mapping.append(oi_scan)  # single space in normalized text
                in_ws = True
        else:
            mapping.append(oi_scan)
            in_ws = False
    # strip trailing (the last entry might be a space from a trailing ws run)
    while mapping and original[mapping[-1]] in " \t\n\r":
        mapping.pop()
    return mapping


_NAME_LABELS = {"first_name", "last_name"}


def _merge_adjacent_names(entities: list) -> list:
    """Merge consecutive first_name + last_name (or vice-versa) into one entity."""
    if len(entities) < 2:
        return entities
    entities = sorted(entities, key=lambda e: e["start"])
    merged: list = []
    i = 0
    while i < len(entities):
        cur = entities[i]
        if (
            i + 1 < len(entities)
            and cur["label"] in _NAME_LABELS
            and entities[i + 1]["label"] in _NAME_LABELS
            and cur["label"] != entities[i + 1]["label"]
            and 0 <= entities[i + 1]["start"] - cur["end"] <= 2
        ):
            nxt = entities[i + 1]
            merged.append({
                "text": cur["text"] + " " + nxt["text"]
                if nxt["start"] > cur["end"] else cur["text"] + nxt["text"],
                "label": "first_name",
                "start": cur["start"],
                "end": nxt["end"],
                "score": min(cur.get("score", 0), nxt.get("score", 0)),
            })
            i += 2
        else:
            merged.append(cur)
            i += 1
    return merged


def _chunk_and_predict(
    model, text: str, labels: List[str], threshold: float
) -> list:
    """Run GLiNER prediction with chunking for long texts.

    Normalizes whitespace once, then uses character-level word boundaries so
    that entity offsets refer to positions in the normalized text.  The UDF
    caller also normalizes the text before passing it here, ensuring offsets
    are consistent with the text stored in the pipeline.
    """
    # Normalize whitespace so offsets are stable
    text = _WHITESPACE_RE.sub(" ", text).strip()

    word_spans = _find_word_boundaries(text)
    if len(word_spans) <= MAX_WORDS:
        return model.predict_entities(text, labels, threshold=threshold)

    all_entities = []
    wi = 0  # word index

    while wi < len(word_spans):
        wi_end = min(wi + MAX_WORDS, len(word_spans))
        char_start = word_spans[wi][0]
        char_end = word_spans[wi_end - 1][1]
        chunk_text = text[char_start:char_end]

        chunk_entities = model.predict_entities(chunk_text, labels, threshold=threshold)

        for ent in chunk_entities:
            all_entities.append({
                "text": ent["text"],
                "label": ent["label"],
                "start": ent["start"] + char_start,
                "end": ent["end"] + char_start,
                "score": ent.get("score", 0.0),
            })

        if wi_end >= len(word_spans):
            break
        wi += MAX_WORDS - OVERLAP_WORDS

    # Deduplicate: for entities with same (start, end, label), keep highest score
    seen = {}
    for ent in all_entities:
        key = (ent["start"], ent["end"], ent["label"])
        if key not in seen or ent["score"] > seen[key]["score"]:
            seen[key] = ent

    return list(seen.values())


def _map_label(raw_label: str) -> str:
    """Map a nemotron-pii training label to a standardized entity type."""
    return GLINER_LABEL_MAP.get(raw_label, raw_label.upper().replace(" ", "_"))


def make_gliner_udf(
    model_name: str = DEFAULT_GLINER_MODEL,
    labels: List[str] = None,
    threshold: float = DEFAULT_GLINER_THRESHOLD,
):
    """Create iterator-based Pandas UDF for GLiNER detection.

    Uses factory pattern to only capture serializable primitives in the closure.
    The model loads lazily via module-level cache on each worker.
    """
    labels_list = list(labels or DEFAULT_GLINER_LABELS)

    @pandas_udf("string")
    def gliner_udf(
        batch_iter: Iterator[Tuple[pd.Series, pd.Series]],
    ) -> Iterator[pd.Series]:
        model = _get_cached_gliner_model(model_name)

        for doc_ids, texts in batch_iter:
            results = []
            for doc_id, text in zip(doc_ids, texts):
                if not text or pd.isna(text):
                    results.append(json.dumps([]))
                    continue

                try:
                    norm_text = _WHITESPACE_RE.sub(" ", text).strip()
                    offset_map = _build_offset_map(text)
                    entities = _chunk_and_predict(model, norm_text, labels_list, threshold)
                    entities = [
                        e for e in entities
                        if e.get("score", 0) >= DEFAULT_GLINER_THRESHOLDS_BY_TYPE.get(
                            e["label"], threshold
                        )
                    ]
                    entities = _merge_adjacent_names(entities)
                    # Remap offsets from normalized text back to original text
                    # and re-derive entity text from original to fix merged-name mismatches
                    for ent in entities:
                        s, e = ent["start"], ent["end"]
                        ent["start"] = offset_map[s] if s < len(offset_map) else s
                        ent["end"] = offset_map[e - 1] + 1 if 0 < e <= len(offset_map) else e
                        ent["text"] = text[ent["start"]:ent["end"]]
                    formatted = [
                        {
                            "entity": ent["text"],
                            "entity_type": _map_label(ent["label"]),
                            "start": ent["start"],
                            "end": ent["end"],
                            "score": ent.get("score", 0.0),
                            "doc_id": str(doc_id),
                        }
                        for ent in entities
                        if not should_ignore_entity(ent["text"], _map_label(ent["label"]))
                    ]
                    results.append(json.dumps(formatted))
                except Exception as e:
                    logger.warning(f"Error processing document {doc_id}: {e}")
                    results.append(json.dumps([]))

            yield pd.Series(results)

    return gliner_udf


def run_gliner_detection(
    df: DataFrame,
    doc_id_column: str,
    text_column: str,
    model_name: str = DEFAULT_GLINER_MODEL,
    num_cores: int = 10,
    labels: List[str] = None,
    threshold: float = DEFAULT_GLINER_THRESHOLD,
    _repartition: bool = True,
) -> DataFrame:
    """Run GLiNER-based PHI detection on a DataFrame.

    Args:
        df: Input DataFrame with text to analyze
        doc_id_column: Name of document ID column
        text_column: Name of text column to analyze
        model_name: HuggingFace model identifier
        num_cores: Number of cores for repartitioning
        labels: Entity labels for detection
        threshold: Minimum confidence threshold
        _repartition: Whether to repartition. Set False when caller already did.
    """
    gliner_udf = make_gliner_udf(model_name=model_name, labels=labels, threshold=threshold)

    base_df = df.repartition(num_cores) if _repartition else df
    result_df = base_df.withColumn(
        "gliner_results", gliner_udf(col(doc_id_column), col(text_column))
    )

    result_df = result_df.withColumn(
        "gliner_results_struct",
        from_json(
            "gliner_results",
            "array<struct<entity:string, entity_type:string, start:integer, end:integer, score:double, doc_id:string>>",
        ),
    )

    return result_df
