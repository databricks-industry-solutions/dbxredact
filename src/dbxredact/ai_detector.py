"""AI-based PHI/PII detection functions."""

import json
import logging
import re
from typing import List, Tuple, Union
import pandas as pd
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import StringType

from .config import PHI_PROMPT_SKELETON, LABEL_ENUMS, DEFAULT_AI_CONFIDENCE_SCORE, should_ignore_entity

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

    # Whitespace-normalized: collapse runs of whitespace in both entity and text
    norm_entity = _WHITESPACE_RE.sub(" ", entity_text).strip()
    norm_sentence = _WHITESPACE_RE.sub(" ", sentence).strip()
    if norm_entity != entity_text or norm_sentence != sentence:
        norm_positions = [
            (m.start(), m.end())
            for m in re.finditer(re.escape(norm_entity), norm_sentence, re.IGNORECASE)
        ]
        if norm_positions:
            # Map normalized offsets back to original text (approximate: use ratio)
            ratio = len(sentence) / max(len(norm_sentence), 1)
            return [(int(s * ratio), int(e * ratio)) for s, e in norm_positions]

    # Fuzzy sliding-window fallback
    try:
        from rapidfuzz import fuzz
        window_len = len(entity_text)
        best_score, best_start = 0.0, -1
        for i in range(len(sentence) - window_len + 1):
            candidate = sentence[i : i + window_len]
            score = fuzz.ratio(entity_text.lower(), candidate.lower())
            if score > best_score:
                best_score = score
                best_start = i
        if best_score >= _FUZZY_MATCH_THRESHOLD and best_start >= 0:
            return [(best_start, best_start + window_len)]
    except ImportError:
        pass

    return []


@pandas_udf(StringType())
def format_entity_response_object_udf(
    identified_entities_series: pd.Series, sentences: pd.Series
) -> pd.Series:
    """Format AI-detected entities with position information.

    This UDF takes the entity list from AI responses and enhances it with
    precise position information (start/end indices) by finding all occurrences
    in the original text.
    """
    new_entity_series = []

    for entity_list, sentence in zip(identified_entities_series, sentences):
        try:
            entities = json.loads(entity_list)
        except (json.JSONDecodeError, TypeError):
            new_entity_series.append(json.dumps([]))
            continue

        unique_entities_set = set(
            [
                (entity["entity"], entity["entity_type"])
                for entity in entities
                if "entity" in entity and "entity_type" in entity
            ]
        )

        new_entity_list = []

        for entity_text, entity_type in unique_entities_set:
            if should_ignore_entity(entity_text, entity_type):
                continue
            positions = _find_entity_positions(entity_text, sentence)

            for start, end in positions:
                new_entity_list.append(
                    {
                        "entity": sentence[start:end],
                        "entity_type": entity_type,
                        "start": start,
                        "end": end,
                        "score": DEFAULT_AI_CONFIDENCE_SCORE,
                        "analysis_explanation": None,
                        "recognition_metadata": {},
                    }
                )

        new_entity_series.append(json.dumps(new_entity_list))

    return pd.Series(new_entity_series)
