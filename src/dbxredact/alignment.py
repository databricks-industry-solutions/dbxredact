"""Entity alignment functions for combining multiple PHI detection methods."""

import logging
from dataclasses import dataclass, field
from typing import List, Dict, Any, Optional, Tuple
from enum import Enum
import pandas as pd
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import (
    ArrayType,
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
)

logger = logging.getLogger(__name__)

from .utils import is_fuzzy_match, is_overlap
from .config import (
    DEFAULT_FUZZY_MATCH_THRESHOLD,
    SOURCE_WEIGHTS,
    EXACT_MATCH_SCORE,
    OVERLAP_MATCH_SCORE,
    CONFIDENCE_THRESHOLDS,
    REQUIRED_ENTITY_FIELDS,
)


class MatchType(Enum):
    """Type of match between two entities."""

    EXACT = "exact"
    OVERLAP_FUZZY = "overlap_fuzzy"
    NO_MATCH = "no_match"


@dataclass
class Entity:
    """Normalized entity representation."""

    entity: str
    start: int
    end: int
    entity_type: Optional[str] = None
    doc_id: Optional[str] = None
    source: Optional[str] = None
    score: Optional[float] = None
    extra_fields: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        """Validate required fields."""
        if self.entity is None or self.start is None or self.end is None:
            raise ValueError("Entity must have entity, start, and end fields")


def normalize_entity(
    entity_dict: Dict[str, Any], source: str, doc_id: Optional[str] = None
) -> Entity:
    """Convert entity dictionary to normalized Entity object."""
    missing_fields = REQUIRED_ENTITY_FIELDS - set(entity_dict.keys())
    if missing_fields:
        raise ValueError(f"Missing required fields: {missing_fields}")

    entity_text = str(entity_dict["entity"])
    start = int(entity_dict["start"])
    end = int(entity_dict["end"])
    entity_type = entity_dict.get("entity_type")
    if entity_type is not None:
        entity_type = str(entity_type)

    entity_doc_id = doc_id if doc_id is not None else entity_dict.get("doc_id")
    if entity_doc_id is not None:
        entity_doc_id = str(entity_doc_id)

    score = entity_dict.get("score")
    if score is not None:
        score = float(score)

    standard_fields = {"entity", "start", "end", "entity_type", "doc_id", "score"}
    extra_fields = {k: v for k, v in entity_dict.items() if k not in standard_fields}

    return Entity(
        entity=entity_text,
        start=start,
        end=end,
        entity_type=entity_type,
        doc_id=entity_doc_id,
        source=source,
        score=score,
        extra_fields=extra_fields,
    )


def calculate_match_score(
    entity1: Entity,
    entity2: Entity,
    fuzzy_threshold: int = DEFAULT_FUZZY_MATCH_THRESHOLD,
) -> Tuple[float, MatchType]:
    """Calculate match quality score between two entities."""
    if (
        entity1.entity == entity2.entity
        and entity1.start == entity2.start
        and entity1.end == entity2.end
    ):
        return EXACT_MATCH_SCORE, MatchType.EXACT

    if is_overlap(entity1.start, entity1.end, entity2.start, entity2.end):
        if is_fuzzy_match(entity1.entity, entity2.entity, threshold=fuzzy_threshold):
            return OVERLAP_MATCH_SCORE, MatchType.OVERLAP_FUZZY

    return 0.0, MatchType.NO_MATCH


def find_best_match(
    entity: Entity,
    candidates: List[Entity],
    fuzzy_threshold: int = DEFAULT_FUZZY_MATCH_THRESHOLD,
) -> Tuple[Optional[Entity], float, MatchType]:
    """Find the best matching entity from a list of candidates."""
    best_match = None
    best_score = 0.0
    best_type = MatchType.NO_MATCH

    for candidate in candidates:
        score, match_type = calculate_match_score(entity, candidate, fuzzy_threshold)

        if match_type == MatchType.EXACT:
            return candidate, score, match_type
        elif score > best_score:
            best_match = candidate
            best_score = score
            best_type = match_type

    return best_match, best_score, best_type


def merge_entities(entities: List[Entity], match_type: MatchType) -> Dict[str, Any]:
    """Merge multiple matched entities from different sources into one result."""
    if not entities:
        raise ValueError("Cannot merge empty list of entities")

    longest_entity = max(entities, key=lambda e: len(e.entity))

    sources = [e.source for e in entities if e.source]
    scores = {"presidio_score": None, "gliner_score": None, "ai_score": None}

    for entity in entities:
        if entity.source and entity.score is not None:
            scores[f"{entity.source}_score"] = entity.score

    entity_type = None
    type_preference = ["ai", "presidio", "gliner"]

    for source_name in type_preference:
        for entity in entities:
            if entity.source == source_name and entity.entity_type:
                entity_type = entity.entity_type
                break
        if entity_type:
            break

    if not entity_type:
        for entity in entities:
            if entity.entity_type:
                entity_type = entity.entity_type
                break

    if not entity_type:
        entity_type = "UNKNOWN"

    doc_id = next((e.doc_id for e in entities if e.doc_id), None)

    return {
        "entity": longest_entity.entity,
        "entity_type": entity_type,
        "start": longest_entity.start,
        "end": longest_entity.end,
        "doc_id": doc_id,
        "presidio_score": scores["presidio_score"],
        "gliner_score": scores["gliner_score"],
        "ai_score": scores["ai_score"],
        "sources": sources,
        "match_type": match_type.value,
    }


def calculate_confidence(merged_entity: Dict[str, Any], match_type: MatchType) -> str:
    """Calculate confidence level using weighted scoring based on source agreement."""
    sources = merged_entity.get("sources", [])

    if not sources:
        return "low"

    total_weight = 0.0
    match_multiplier = 1.0 if match_type == MatchType.EXACT else 0.8

    for source in sources:
        if source in SOURCE_WEIGHTS:
            score_key = f"{source}_score"
            score = merged_entity.get(score_key)

            if score is not None and score >= 0.5:
                contribution = SOURCE_WEIGHTS[source] * match_multiplier
                if score >= 0.7:
                    contribution *= 1.0
                else:
                    contribution *= 0.7
                total_weight += contribution
            else:
                total_weight += SOURCE_WEIGHTS[source] * 0.3 * match_multiplier

    if total_weight >= CONFIDENCE_THRESHOLDS["high"]:
        return "high"
    elif total_weight >= CONFIDENCE_THRESHOLDS["medium"]:
        return "medium"
    else:
        return "low"


CONFIDENCE_RANK = {"high": 3, "medium": 2, "low": 1}


def _merge_overlapping_spans(results: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Merge overlapping entity spans so redaction never produces garbled output.

    Sorts by start position and sweeps left-to-right. When two spans overlap,
    the wider span is kept and the higher-confidence entity type wins.
    """
    if len(results) <= 1:
        return results

    sorted_results = sorted(results, key=lambda r: (r["start"], -r["end"]))
    merged = [sorted_results[0]]

    for current in sorted_results[1:]:
        prev = merged[-1]
        if current["start"] < prev["end"]:
            prev["end"] = max(prev["end"], current["end"])
            if len(current.get("entity", "")) > len(prev.get("entity", "")):
                prev["entity"] = current["entity"]
            cur_rank = CONFIDENCE_RANK.get(current.get("confidence", "low"), 0)
            prev_rank = CONFIDENCE_RANK.get(prev.get("confidence", "low"), 0)
            if cur_rank > prev_rank:
                prev["entity_type"] = current["entity_type"]
                prev["confidence"] = current["confidence"]
            for key in ("presidio_score", "gliner_score", "ai_score"):
                if current.get(key) is not None:
                    if prev.get(key) is None or current[key] > prev[key]:
                        prev[key] = current[key]
        else:
            merged.append(current)

    return merged


class MultiSourceAligner:
    """Orchestrates entity alignment across multiple detection sources."""

    def __init__(self, fuzzy_threshold: int = DEFAULT_FUZZY_MATCH_THRESHOLD):
        self.fuzzy_threshold = fuzzy_threshold

    def align(
        self,
        doc_id: str,
        presidio_entities: Optional[List[Dict[str, Any]]] = None,
        gliner_entities: Optional[List[Dict[str, Any]]] = None,
        ai_entities: Optional[List[Dict[str, Any]]] = None,
        min_sources: int = 1,
    ) -> List[Dict[str, Any]]:
        """Align entities from multiple sources for a single document.
        
        Args:
            min_sources: Minimum number of detection sources that must agree
                for an entity to be included. Use ceil(active_detectors / 2)
                for majority-vote consensus.
        """
        normalized_entities = {
            "presidio": self._normalize_entities(presidio_entities, "presidio", doc_id),
            "gliner": self._normalize_entities(gliner_entities, "gliner", doc_id),
            "ai": self._normalize_entities(ai_entities, "ai", doc_id),
        }

        used_entities = {"presidio": set(), "gliner": set(), "ai": set()}

        results = []

        primary_source = max(
            normalized_entities.keys(), key=lambda s: len(normalized_entities[s])
        )

        for primary_idx, primary_entity in enumerate(
            normalized_entities[primary_source]
        ):
            matches = [primary_entity]
            match_types = [MatchType.EXACT]
            used_entities[primary_source].add(primary_idx)

            for other_source in ["presidio", "gliner", "ai"]:
                if other_source == primary_source:
                    continue

                candidates = [
                    entity
                    for idx, entity in enumerate(normalized_entities[other_source])
                    if idx not in used_entities[other_source]
                ]

                if candidates:
                    best_match, _score, match_type = find_best_match(
                        primary_entity, candidates, self.fuzzy_threshold
                    )

                    if best_match and match_type != MatchType.NO_MATCH:
                        matches.append(best_match)
                        match_types.append(match_type)
                        idx = normalized_entities[other_source].index(best_match)
                        used_entities[other_source].add(idx)

            overall_match_type = (
                MatchType.EXACT
                if all(mt == MatchType.EXACT for mt in match_types)
                else (
                    match_types[0] if len(match_types) == 1 else MatchType.OVERLAP_FUZZY
                )
            )

            merged = merge_entities(matches, overall_match_type)
            merged["confidence"] = calculate_confidence(merged, overall_match_type)
            results.append(merged)

        for source in ["presidio", "gliner", "ai"]:
            if source == primary_source:
                continue

            for idx, entity in enumerate(normalized_entities[source]):
                if idx not in used_entities[source]:
                    merged = merge_entities([entity], MatchType.EXACT)
                    merged["confidence"] = calculate_confidence(merged, MatchType.EXACT)
                    results.append(merged)

        if min_sources > 1:
            results = [r for r in results if len(r.get("sources", [])) >= min_sources]

        results = _merge_overlapping_spans(results)

        cleaned_results = []
        for result in results:
            cleaned = {
                "entity": result["entity"],
                "entity_type": result["entity_type"],
                "start": result["start"],
                "end": result["end"],
                "doc_id": result["doc_id"],
                "presidio_score": result["presidio_score"],
                "gliner_score": result["gliner_score"],
                "ai_score": result["ai_score"],
                "confidence": result["confidence"],
            }
            cleaned_results.append(cleaned)

        return cleaned_results

    def _normalize_entities(
        self, entities: Optional[List[Dict[str, Any]]], source: str, doc_id: str
    ) -> List[Entity]:
        """Normalize a list of entity dictionaries."""
        if entities is None:
            return []

        try:
            if len(entities) == 0:
                return []
        except TypeError:
            logger.debug("Entities not iterable for source=%s doc_id=%s", source, doc_id)
            return []

        normalized = []
        for entity_dict in entities:
            try:
                entity_doc_id = entity_dict.get("doc_id")
                if entity_doc_id is not None and str(entity_doc_id) != str(doc_id):
                    continue

                entity = normalize_entity(entity_dict, source, doc_id)
                normalized.append(entity)
            except (ValueError, KeyError, TypeError) as exc:
                logger.debug("Skipping malformed entity from %s: %s", source, exc)
                continue

        return normalized


def align_entities_multi_source(
    presidio_entities: Optional[List[Dict[str, Any]]],
    gliner_entities: Optional[List[Dict[str, Any]]],
    ai_entities: Optional[List[Dict[str, Any]]],
    doc_id: str,
    fuzzy_threshold: int = DEFAULT_FUZZY_MATCH_THRESHOLD,
    min_sources: int = 1,
) -> List[Dict[str, Any]]:
    """Align entities from multiple sources for a single document."""
    aligner = MultiSourceAligner(fuzzy_threshold=fuzzy_threshold)
    return aligner.align(
        doc_id=doc_id,
        presidio_entities=presidio_entities,
        gliner_entities=gliner_entities,
        ai_entities=ai_entities,
        min_sources=min_sources,
    )


def align_entities_udf(
    fuzzy_threshold: int = DEFAULT_FUZZY_MATCH_THRESHOLD,
    include_presidio: bool = True,
    include_gliner: bool = False,
    include_ai: bool = True,
    min_sources: int = 1,
):
    """Create a Pandas UDF for aligning entities from multiple detection sources."""
    entity_struct = StructType(
        [
            StructField("entity", StringType()),
            StructField("entity_type", StringType()),
            StructField("start", IntegerType()),
            StructField("end", IntegerType()),
            StructField("doc_id", StringType()),
            StructField("presidio_score", DoubleType()),
            StructField("gliner_score", DoubleType()),
            StructField("ai_score", DoubleType()),
            StructField("confidence", StringType()),
        ]
    )
    result_type = ArrayType(entity_struct)

    @pandas_udf(result_type)
    def _align_udf(
        ai_col: pd.Series,
        presidio_col: pd.Series,
        gliner_col: pd.Series,
        doc_id_col: pd.Series,
    ) -> pd.Series:
        """Align entities for each row in the batch."""
        results = []

        for ai_ents, presidio_ents, gliner_ents, doc_id in zip(
            ai_col, presidio_col, gliner_col, doc_id_col
        ):
            aligned = align_entities_multi_source(
                presidio_entities=presidio_ents if include_presidio else None,
                gliner_entities=gliner_ents if include_gliner else None,
                ai_entities=ai_ents if include_ai else None,
                doc_id=doc_id,
                fuzzy_threshold=fuzzy_threshold,
                min_sources=min_sources,
            )
            results.append(aligned)

        return pd.Series(results)

    return _align_udf


def align_entities_row(
    ai_entities: List[Dict[str, Any]],
    presidio_entities: List[Dict[str, Any]],
    doc_id: str,
    fuzzy_threshold: int = DEFAULT_FUZZY_MATCH_THRESHOLD,
) -> List[Dict[str, Any]]:
    """Align entities from AI and Presidio detection for a single document (legacy)."""
    return align_entities_multi_source(
        presidio_entities=presidio_entities,
        gliner_entities=None,
        ai_entities=ai_entities,
        doc_id=doc_id,
        fuzzy_threshold=fuzzy_threshold,
    )

