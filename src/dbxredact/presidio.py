"""Presidio-based PHI/PII detection functions."""

import json
from typing import Iterator, Tuple
import pandas as pd
from pyspark.sql.functions import pandas_udf
from presidio_analyzer import BatchAnalyzerEngine
from presidio_analyzer.dict_analyzer_result import DictAnalyzerResult

from .config import DEFAULT_PRESIDIO_SCORE_THRESHOLD, PRESIDIO_ENTITY_TYPES, should_ignore_entity
from .analyzer import SpacyModelNotFoundError


def format_presidio_batch_results(
    results: Iterator[DictAnalyzerResult],
    score_threshold: float = DEFAULT_PRESIDIO_SCORE_THRESHOLD,
) -> list:
    """
    Format Presidio batch analysis results into a list of JSON strings.

    Args:
        results: Iterator of DictAnalyzerResult from Presidio batch analysis
        score_threshold: Minimum confidence score to include results (0.0-1.0)

    Returns:
        List of JSON-serialized findings for each document
    """
    col1, col2 = tuple(results)
    doc_ids = col1.value
    original_texts = col2.value
    recognizer_results = col2.recognizer_results

    output = []
    for i, res_doc in enumerate(recognizer_results):
        findings = []
        for res_ent in res_doc:
            ans = res_ent.to_dict()
            ans["doc_id"] = doc_ids[i]
            ans["entity"] = original_texts[i][res_ent.start : res_ent.end]

            if ans.get("score", 0) > score_threshold and not should_ignore_entity(
                ans.get("entity", ""), ans.get("entity_type", "")
            ):
                findings.append(ans)

        output.append(json.dumps(findings))

    return output


# Global singleton for analyzer (per-worker)
_analyzer_cache = {}


def _get_cached_analyzer(add_pci: bool, score_threshold: float, model_size: str = None, pattern_only: bool = False):
    """Get or create cached analyzer instance (singleton per worker)."""
    cache_key = (add_pci, score_threshold, model_size, pattern_only)

    if cache_key not in _analyzer_cache:
        if pattern_only:
            from .analyzer import get_pattern_only_analyzer
            _analyzer_cache[cache_key] = get_pattern_only_analyzer(
                default_score_threshold=score_threshold,
            )
        else:
            from .analyzer import get_analyzer_engine
            _analyzer_cache[cache_key] = get_analyzer_engine(
                add_pci=add_pci, default_score_threshold=score_threshold,
                model_size=model_size,
            )

    return _analyzer_cache[cache_key]


def make_presidio_batch_udf(
    score_threshold: float = DEFAULT_PRESIDIO_SCORE_THRESHOLD,
    add_pci: bool = False,
    model_size: str = None,
    entities: list = None,
    pattern_only: bool = False,
):
    """
    Create a Pandas UDF for batch PHI detection using Presidio.

    Args:
        score_threshold: Minimum confidence score to include results (0.0-1.0)
        add_pci: Whether to add PCI (Payment Card Industry) recognizers
        model_size: spaCy model size ('sm', 'md', 'lg', 'trf')
        entities: Entity types to detect. Defaults to PRESIDIO_ENTITY_TYPES.
        pattern_only: If True, use only pattern recognizers (no spaCy/NER).

    Returns:
        A Pandas UDF that takes (doc_ids, texts) and returns JSON-serialized results

    Raises:
        SpacyModelNotFoundError: If required spaCy models are not installed (unless pattern_only)
    """
    entity_list = list(entities or PRESIDIO_ENTITY_TYPES)

    @pandas_udf("string")
    def analyze_udf(
        batch_iter: Iterator[Tuple[pd.Series, pd.Series]],
    ) -> Iterator[pd.Series]:
        analyzer = _get_cached_analyzer(add_pci, score_threshold, model_size, pattern_only=pattern_only)
        batch_analyzer = BatchAnalyzerEngine(analyzer_engine=analyzer)

        for doc_ids, texts in batch_iter:
            text_dict = pd.DataFrame({"doc_id": doc_ids, "text": texts}).to_dict(
                orient="list"
            )

            results = batch_analyzer.analyze_dict(
                text_dict,
                language="en",
                keys_to_skip=["doc_id"],
                score_threshold=score_threshold,
                entities=entity_list,
                batch_size=100,
                n_process=1,
            )

            output = format_presidio_batch_results(
                results, score_threshold=score_threshold
            )
            yield pd.Series(output)

    return analyze_udf
