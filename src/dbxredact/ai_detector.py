"""AI-based PHI/PII detection functions."""

import json
import logging
import re
from typing import List, Tuple, Union
import pandas as pd
from pyspark.sql.functions import pandas_udf

from .config import PHI_PROMPT_SKELETON, LABEL_ENUMS, DEFAULT_AI_CONFIDENCE_SCORE, should_ignore_entity, _entity_schema
from .utils import build_offset_map

logger = logging.getLogger(__name__)

_WHITESPACE_RE = re.compile(r"\s+")
_FUZZY_MATCH_THRESHOLD = 80


def make_prompt(
    prompt_skeleton: str = PHI_PROMPT_SKELETON, labels: Union[List[str], str] = LABEL_ENUMS
) -> str:
    """Create a PHI detection prompt with specified entity labels.

    Args:
        prompt_skeleton: Template prompt with {label_enums} placeholder
        labels: List of entity type labels or JSON string of labels

    Returns:
        Formatted prompt string ready to use with LLM
    """
    if isinstance(labels, list):
        labels_str = json.dumps(labels)
    else:
        labels_str = labels

    return prompt_skeleton.format(label_enums=labels_str)


def _find_entity_positions(
    entity_text: str, sentence: str
) -> List[Tuple[int, int]]:
    """Locate all occurrences of entity_text in sentence.

    Strategy:
    1. Case-insensitive regex match (handles casing differences).
    2. Whitespace-normalized match (handles newline / multi-space differences).
    3. Fuzzy sliding-window match via rapidfuzz (handles minor LLM rephrasing).
    """
    pattern = re.escape(entity_text)
    positions = [(m.start(), m.end()) for m in re.finditer(pattern, sentence, re.IGNORECASE)]
    if positions:
        return positions

    # Whitespace-normalized: collapse runs of whitespace in both entity and text,
    # then map positions back to original via a character-level offset map.
    norm_entity = _WHITESPACE_RE.sub(" ", entity_text).strip()
    norm_sentence = _WHITESPACE_RE.sub(" ", sentence).strip()
    if norm_entity != entity_text or norm_sentence != sentence:
        norm_positions = [
            (m.start(), m.end())
            for m in re.finditer(re.escape(norm_entity), norm_sentence, re.IGNORECASE)
        ]
        if norm_positions:
            offset_map = build_offset_map(sentence)
            mapped = []
            for ns, ne in norm_positions:
                orig_start = offset_map[ns] if ns < len(offset_map) else ns
                orig_end = (offset_map[ne - 1] + 1) if 0 < ne <= len(offset_map) else ne
                mapped.append((orig_start, orig_end))
            return mapped

    # Fuzzy match: prefer rapidfuzz.partial_ratio_alignment (fast), fall back to
    # brute-force sliding window if the function isn't available in this version.
    from rapidfuzz import fuzz
    try:
        from rapidfuzz.fuzz import partial_ratio_alignment
        result = partial_ratio_alignment(entity_text.lower(), sentence.lower())
        if result is not None and result.score >= _FUZZY_MATCH_THRESHOLD:
            return [(result.dest_start, result.dest_end)]
    except (ImportError, AttributeError):
        # Sliding-window fallback for older rapidfuzz versions
        window_len = len(entity_text)
        if window_len > 0 and window_len <= len(sentence):
            best_score, best_start = 0, 0
            for i in range(len(sentence) - window_len + 1):
                candidate = sentence[i : i + window_len]
                score = fuzz.ratio(entity_text.lower(), candidate.lower())
                if score > best_score:
                    best_score = score
                    best_start = i
            if best_score >= _FUZZY_MATCH_THRESHOLD:
                return [(best_start, best_start + window_len)]

    return []


def _parse_entity_list(raw) -> list:
    """Parse an entity list from either a JSON string or a list of Row/dict objects.

    Deduplicates by (entity, entity_type) to guard against LLM hallucination
    where the model repeats the same entity hundreds of times.
    """
    if raw is None:
        return []
    if isinstance(raw, str):
        try:
            parsed = json.loads(raw)
        except (json.JSONDecodeError, TypeError):
            return []
        if isinstance(parsed, dict) and "result" in parsed:
            parsed = parsed["result"]
        if not isinstance(parsed, list):
            return []
        entities = parsed
    elif isinstance(raw, list):
        entities = [r.asDict() if hasattr(r, "asDict") else dict(r) for r in raw]
    else:
        return []

    seen = set()
    deduped = []
    for e in entities:
        key = (e.get("entity"), e.get("entity_type"))
        if key not in seen:
            seen.add(key)
            deduped.append(e)
    if len(deduped) < len(entities):
        logger.info(
            "Deduplicated AI response from %d to %d entities", len(entities), len(deduped)
        )
    return deduped


def format_entity_response_object_udf(
    identified_entities_series: pd.Series, sentences: pd.Series
) -> pd.Series:
    """Format AI-detected entities with position information.

    Accepts either a JSON string (external model) or a list of Row objects
    (foundation model with returnType) and enhances each entity with precise
    start/end indices by locating all occurrences in the original text.

    Returns a Series of lists-of-dicts matching ``ENTITY_SCHEMA``.
    """
    new_entity_series = []

    for entity_list, sentence in zip(identified_entities_series, sentences):
        entities = _parse_entity_list(entity_list)

        new_entity_list = []
        unlocated_count = 0

        for entity in entities:
            entity_text = entity.get("entity")
            entity_type = entity.get("entity_type")
            if not entity_text or not entity_type:
                continue
            if should_ignore_entity(entity_text, entity_type):
                continue
            positions = _find_entity_positions(entity_text, sentence)

            if not positions:
                logger.warning(
                    "AI detected entity (%s, %d chars) but could not locate in text",
                    entity_type, len(entity_text),
                )
                unlocated_count += 1
                continue

            for start, end in positions:
                new_entity_list.append({
                    "entity": sentence[start:end],
                    "entity_type": entity_type,
                    "score": float(DEFAULT_AI_CONFIDENCE_SCORE),
                    "start": start,
                    "end": end,
                    "doc_id": None,
                })

        if unlocated_count > 0:
            logger.warning(
                "%d AI-detected entities could not be located in text (out of %d)",
                unlocated_count, len(entities),
            )
        new_entity_series.append(new_entity_list)

    return pd.Series(new_entity_series)


_format_entity_udf = None


def _get_format_entity_udf():
    """Lazily create the pandas_udf wrapper for format_entity_response_object_udf."""
    global _format_entity_udf
    if _format_entity_udf is None:
        _format_entity_udf = pandas_udf(format_entity_response_object_udf, _entity_schema())
    return _format_entity_udf


def _offset_entities(entities: list, char_offset: int) -> list:
    """Shift entity start/end positions by char_offset."""
    if not entities or char_offset == 0:
        return entities
    return [
        {**e, "start": e["start"] + char_offset, "end": e["end"] + char_offset}
        for e in entities
    ]


def _deduplicate_chunk_entities(entities: list) -> list:
    """Merge entities from overlapping chunk regions.

    For exact (start, end, entity_type) duplicates, keep one.
    For same-type entities whose spans overlap, keep the longer span.
    """
    if not entities:
        return []
    sorted_ents = sorted(entities, key=lambda e: (e.get("start", 0), -(e.get("end", 0) - e.get("start", 0))))
    result = []
    for ent in sorted_ents:
        merged = False
        for i, existing in enumerate(result):
            if (ent.get("entity_type") == existing.get("entity_type")
                    and ent.get("start", 0) < existing.get("end", 0)
                    and ent.get("end", 0) > existing.get("start", 0)):
                if (ent.get("end", 0) - ent.get("start", 0)) > (existing.get("end", 0) - existing.get("start", 0)):
                    result[i] = ent
                merged = True
                break
        if not merged:
            result.append(ent)
    return result


_offset_correct_udf = None


def _get_offset_correct_udf():
    """Lazily create pandas_udf that offset-corrects entity positions."""
    global _offset_correct_udf
    if _offset_correct_udf is None:
        schema = _entity_schema()

        @pandas_udf(schema)
        def offset_correct_udf(entities_col: pd.Series, offsets: pd.Series) -> pd.Series:
            results = []
            for entities, offset in zip(entities_col, offsets):
                ent_list = []
                if entities is not None:
                    for e in entities:
                        ent_list.append(e.asDict() if hasattr(e, "asDict") else dict(e))
                off = int(offset) if offset is not None and pd.notna(offset) else 0
                results.append(_offset_entities(ent_list, off))
            return pd.Series(results)

        _offset_correct_udf = offset_correct_udf
    return _offset_correct_udf


_merge_chunks_udf = None


def _get_merge_chunks_udf():
    """Lazily create pandas_udf that flattens + deduplicates collected chunk entity lists."""
    global _merge_chunks_udf
    if _merge_chunks_udf is None:
        schema = _entity_schema()

        @pandas_udf(schema)
        def merge_chunks_udf(collected: pd.Series) -> pd.Series:
            results = []
            for entity_lists in collected:
                flat = []
                if entity_lists is not None:
                    for chunk_entities in entity_lists:
                        if chunk_entities is not None:
                            for e in chunk_entities:
                                flat.append(e.asDict() if hasattr(e, "asDict") else dict(e))
                results.append(_deduplicate_chunk_entities(flat))
            return pd.Series(results)

        _merge_chunks_udf = merge_chunks_udf
    return _merge_chunks_udf
