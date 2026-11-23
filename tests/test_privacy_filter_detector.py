"""Unit tests for privacy_filter_detector with mocked OPF model."""

from unittest.mock import patch, MagicMock
from types import SimpleNamespace

import pandas as pd
import pytest

from dbxredact.privacy_filter_detector import (
    PRIVACY_FILTER_LABEL_MAP,
    _predict_opf,
    make_privacy_filter_udf,
    run_privacy_filter_detection,
    DEFAULT_PRIVACY_FILTER_MODEL,
)


def _fake_opf_spans(spans):
    """Build a mock opf redact() result from a list of dicts."""
    detected = [SimpleNamespace(**s) for s in spans]
    return SimpleNamespace(detected_spans=detected)


class TestPredictOpf:
    def test_returns_list_of_dicts(self):
        mock_opf = MagicMock()
        mock_opf.redact.return_value = _fake_opf_spans([
            {"label": "private_person", "start": 0, "end": 4, "text": "John"},
        ])
        result = _predict_opf(mock_opf, "John lives here")
        assert len(result) == 1
        assert result[0]["label"] == "private_person"
        assert result[0]["start"] == 0

    def test_empty_text_returns_empty(self):
        mock_opf = MagicMock()
        mock_opf.redact.return_value = _fake_opf_spans([])
        assert _predict_opf(mock_opf, "") == []


class TestLabelMap:
    def test_known_labels_mapped(self):
        assert PRIVACY_FILTER_LABEL_MAP["private_person"] == "PERSON"
        assert PRIVACY_FILTER_LABEL_MAP["private_email"] == "EMAIL_ADDRESS"
        assert PRIVACY_FILTER_LABEL_MAP["private_phone"] == "PHONE_NUMBER"
        assert PRIVACY_FILTER_LABEL_MAP["private_address"] == "ADDRESS"

    def test_unknown_label_uppercased(self):
        assert "some_new_label" not in PRIVACY_FILTER_LABEL_MAP


class TestMakePrivacyFilterUdf:
    @patch("dbxredact.privacy_filter_detector._get_cached_opf")
    def test_udf_processes_batch(self, mock_get_opf):
        mock_opf = MagicMock()
        mock_opf.redact.return_value = _fake_opf_spans([
            {"label": "private_person", "start": 0, "end": 10, "text": "John Smith"},
        ])
        mock_get_opf.return_value = mock_opf

        udf_fn = make_privacy_filter_udf(threshold=0.5)
        inner = udf_fn.func

        doc_ids = pd.Series(["doc1", "doc2"])
        texts = pd.Series(["John Smith is here", None])
        results = list(inner(iter([(doc_ids, texts)])))
        assert len(results) == 1
        batch = results[0]
        assert len(batch) == 2
        assert len(batch.iloc[0]) == 1
        assert batch.iloc[0][0]["entity_type"] == "PERSON"
        assert batch.iloc[1] == []

    @patch("dbxredact.privacy_filter_detector._get_cached_opf")
    def test_threshold_filters_low_score(self, mock_get_opf):
        mock_opf = MagicMock()
        mock_opf.redact.return_value = _fake_opf_spans([
            {"label": "private_person", "start": 0, "end": 4, "text": "John"},
        ])
        mock_get_opf.return_value = mock_opf

        udf_fn = make_privacy_filter_udf(threshold=2.0)
        inner = udf_fn.func

        doc_ids = pd.Series(["doc1"])
        texts = pd.Series(["John is here"])
        results = list(inner(iter([(doc_ids, texts)])))
        assert results[0].iloc[0] == []

    @patch("dbxredact.privacy_filter_detector._get_cached_opf")
    def test_error_handling(self, mock_get_opf):
        mock_opf = MagicMock()
        mock_opf.redact.side_effect = RuntimeError("model failed")
        mock_get_opf.return_value = mock_opf

        udf_fn = make_privacy_filter_udf()
        inner = udf_fn.func

        doc_ids = pd.Series(["doc1"])
        texts = pd.Series(["some text"])
        results = list(inner(iter([(doc_ids, texts)])))
        assert results[0].iloc[0] == []


class TestRunPrivacyFilterDetection:
    def test_adds_gliner_results_column(self, spark):
        from pyspark.sql.functions import lit, array
        from pyspark.sql.types import ArrayType, StructType, StructField, StringType, FloatType, IntegerType

        df = spark.createDataFrame(
            [("doc1", "John Smith lives at 123 Main St")],
            ["doc_id", "text"],
        )
        empty_schema = ArrayType(StructType([
            StructField("entity", StringType()),
            StructField("entity_type", StringType()),
            StructField("score", FloatType()),
            StructField("start", IntegerType()),
            StructField("end", IntegerType()),
            StructField("doc_id", StringType()),
        ]))

        def _fake_udf(*args, **kwargs):
            return array().cast(empty_schema)

        with patch("dbxredact.privacy_filter_detector.make_privacy_filter_udf") as mock_make:
            mock_make.return_value = _fake_udf
            result = run_privacy_filter_detection(
                df=df, doc_id_column="doc_id", text_column="text",
                num_cores=1, _repartition=False,
            )
            assert "gliner_results_struct" in result.columns
