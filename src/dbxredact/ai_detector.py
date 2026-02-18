"""AI-based PHI/PII detection functions."""

import json
import re
from typing import List, Union
import pandas as pd
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import StringType

from .config import PHI_PROMPT_SKELETON, LABEL_ENUMS, DEFAULT_AI_CONFIDENCE_SCORE, should_ignore_entity


def make_prompt(
    prompt_skeleton: str = PHI_PROMPT_SKELETON, labels: Union[List[str], str] = LABEL_ENUMS
) -> str:
    """
    Create a PHI detection prompt with specified entity labels.

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


@pandas_udf(StringType())
def format_entity_response_object_udf(
    identified_entities_series: pd.Series, sentences: pd.Series
) -> pd.Series:
    """
    Format AI-detected entities with position information.

    This UDF takes the entity list from AI responses and enhances it with
    precise position information (start/end indices) by finding all occurrences
    in the original text.

    Args:
        identified_entities_series: Series of JSON strings with entity lists
        sentences: Series of original text strings

    Returns:
        Series of JSON strings with enhanced entity objects including positions
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
            pattern = re.escape(entity_text)
            positions = [
                (m.start(), m.end()) for m in re.finditer(pattern, sentence)
            ]

            for position in positions:
                new_entity_list.append(
                    {
                        "entity": entity_text,
                        "entity_type": entity_type,
                        "start": position[0],
                        "end": position[1],
                        "score": DEFAULT_AI_CONFIDENCE_SCORE,
                        "analysis_explanation": None,
                        "recognition_metadata": {},
                    }
                )

        new_entity_series.append(json.dumps(new_entity_list))

    return pd.Series(new_entity_series)

