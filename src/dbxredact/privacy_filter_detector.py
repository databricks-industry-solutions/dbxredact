"""OpenAI Privacy Filter detection using the opf package (Viterbi decoding).

Requires the opf package: pip install git+https://github.com/openai/privacy-filter.git
The model registers a custom HF architecture that only works when opf is installed.
"""

import logging
from typing import Iterator, Tuple

import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql.functions import pandas_udf, col

from .config import _entity_schema, should_ignore_entity

logger = logging.getLogger(__name__)

DEFAULT_PRIVACY_FILTER_MODEL = "openai/privacy-filter"
DEFAULT_PRIVACY_FILTER_THRESHOLD = 0.5

PRIVACY_FILTER_LABEL_MAP = {
    "private_person": "PERSON",
    "private_email": "EMAIL_ADDRESS",
    "private_phone": "PHONE_NUMBER",
    "private_address": "ADDRESS",
    "private_url": "URL",
    "private_date": "DATE",
    "account_number": "ACCOUNT_NUMBER",
    "secret": "PASSWORD",
}

_opf_cache = {}


def _get_cached_opf(model_name: str):
    """Get or create cached OPF redactor with Viterbi decoding (singleton per worker)."""
    if model_name not in _opf_cache:
        import torch

        device = "cuda" if torch.cuda.is_available() else "cpu"

        try:
            from opf._api import OPF
        except ImportError:
            raise ImportError(
                "The opf package is required for openai/privacy-filter. "
                "Install it with: pip install git+https://github.com/openai/privacy-filter.git"
            )

        opf_instance = OPF(
            model=None if model_name == DEFAULT_PRIVACY_FILTER_MODEL else model_name,
            device=device,
            output_mode="typed",
            decode_mode="viterbi",
        )
        logger.info("Privacy Filter loaded via opf (Viterbi) on %s", device)
        _opf_cache[model_name] = opf_instance

    return _opf_cache[model_name]


def _predict_opf(opf_instance, text: str):
    """Run prediction via opf API, returning list of entity dicts."""
    result = opf_instance.redact(text)
    return [
        {"label": span.label, "start": span.start, "end": span.end, "text": span.text}
        for span in result.detected_spans
    ]


def make_privacy_filter_udf(
    model_name: str = DEFAULT_PRIVACY_FILTER_MODEL,
    threshold: float = DEFAULT_PRIVACY_FILTER_THRESHOLD,
):
    """Create iterator-based Pandas UDF for Privacy Filter detection."""
    schema = _entity_schema()

    @pandas_udf(schema)
    def privacy_filter_udf(
        batch_iter: Iterator[Tuple[pd.Series, pd.Series]],
    ) -> Iterator[pd.Series]:
        backend = _get_cached_opf(model_name)

        for doc_ids, texts in batch_iter:
            results = []
            for doc_id, text in zip(doc_ids, texts):
                if not text or pd.isna(text):
                    results.append([])
                    continue
                try:
                    raw_ents = _predict_opf(backend, text)
                    formatted = []
                    for ent in raw_ents:
                        entity_type = PRIVACY_FILTER_LABEL_MAP.get(
                            ent["label"], ent["label"].upper()
                        )
                        if should_ignore_entity(ent["text"], entity_type):
                            continue
                        score = float(ent.get("score", 1.0)) if isinstance(ent, dict) else 1.0
                        if score < threshold:
                            continue
                        formatted.append({
                            "entity": ent["text"],
                            "entity_type": entity_type,
                            "score": score,
                            "start": ent["start"],
                            "end": ent["end"],
                            "doc_id": str(doc_id),
                        })
                    results.append(formatted)
                except Exception as e:
                    logger.warning("Privacy Filter detection failed for doc %s: %s: %s", doc_id, type(e).__name__, e)
                    results.append([])

            yield pd.Series(results)

    return privacy_filter_udf


def run_privacy_filter_detection(
    df: DataFrame,
    doc_id_column: str,
    text_column: str,
    model_name: str = DEFAULT_PRIVACY_FILTER_MODEL,
    num_cores: int = 10,
    threshold: float = DEFAULT_PRIVACY_FILTER_THRESHOLD,
    _repartition: bool = True,
) -> DataFrame:
    """Run OpenAI Privacy Filter detection on a DataFrame.

    Returns DataFrame with ``gliner_results_struct`` column (reuses the GLiNER
    alignment slot since only one NER backend is active at a time).
    """
    udf = make_privacy_filter_udf(model_name=model_name, threshold=threshold)
    base_df = df.repartition(num_cores) if _repartition else df
    return base_df.withColumn(
        "gliner_results_struct", udf(col(doc_id_column), col(text_column))
    )
