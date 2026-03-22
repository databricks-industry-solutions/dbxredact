"""Tests for active_learning.py -- uncertainty scoring and review queue building."""

import pytest
from pyspark.sql import Row
from pyspark.sql.types import (
    StructType, StructField, StringType, ArrayType, DoubleType, IntegerType,
)

from dbxredact.active_learning import (
    compute_document_uncertainty,
    build_review_queue,
    compute_detector_disagreement,
)

# Matches the real alignment UDF output schema (no 'sources' or 'score' fields).
ENTITY_SCHEMA = ArrayType(StructType([
    StructField("entity", StringType()),
    StructField("entity_type", StringType()),
    StructField("start", IntegerType()),
    StructField("end", IntegerType()),
    StructField("doc_id", StringType()),
    StructField("presidio_score", DoubleType()),
    StructField("gliner_score", DoubleType()),
    StructField("ai_score", DoubleType()),
    StructField("confidence", StringType()),
]))

DOC_SCHEMA = StructType([
    StructField("doc_id", StringType()),
    StructField("aligned_entities", ENTITY_SCHEMA),
])


def _entity(entity, etype, start, end, presidio=None, gliner=None, ai=None):
    return Row(
        entity=entity, entity_type=etype, start=start, end=end,
        doc_id="d1", presidio_score=presidio, gliner_score=gliner,
        ai_score=ai, confidence="high",
    )


class TestComputeDocumentUncertainty:

    def test_high_confidence_doc_low_uncertainty(self, spark):
        df = spark.createDataFrame([
            Row(doc_id="d1", aligned_entities=[
                _entity("John", "PERSON", 0, 4, presidio=0.95, ai=0.90),
                _entity("555-1234", "PHONE", 10, 18, presidio=0.90),
            ]),
        ], DOC_SCHEMA)
        result = compute_document_uncertainty(df).collect()
        assert len(result) == 1
        row = result[0]
        assert row["doc_id"] == "d1"
        assert row["entity_count"] == 2
        assert row["low_confidence_count"] == 0
        assert row["uncertainty_score"] < 0.3

    def test_low_confidence_doc_high_uncertainty(self, spark):
        df = spark.createDataFrame([
            Row(doc_id="d1", aligned_entities=[
                _entity("maybe", "PERSON", 0, 5, presidio=0.3),
                _entity("unsure", "LOCATION", 10, 16, ai=0.2),
            ]),
        ], DOC_SCHEMA)
        result = compute_document_uncertainty(df).collect()
        row = result[0]
        assert row["low_confidence_count"] == 2
        assert row["uncertainty_score"] > 0.7

    def test_no_entities_gets_default_score(self, spark):
        df = spark.createDataFrame([
            Row(doc_id="d1", aligned_entities=[]),
        ], DOC_SCHEMA)
        result = compute_document_uncertainty(df).collect()
        row = result[0]
        assert row["entity_count"] == 0
        assert row["uncertainty_score"] == pytest.approx(0.5)

    def test_output_schema(self, spark):
        df = spark.createDataFrame([
            Row(doc_id="d1", aligned_entities=[
                _entity("John", "PERSON", 0, 4, presidio=0.9),
            ]),
        ], DOC_SCHEMA)
        result = compute_document_uncertainty(df)
        expected_cols = {"doc_id", "avg_score", "min_score", "entity_count",
                         "low_confidence_count", "uncertainty_score"}
        assert set(result.columns) == expected_cols


class TestBuildReviewQueue:

    def test_returns_top_k(self, spark):
        rows = [
            Row(doc_id=f"d{i}", aligned_entities=[
                _entity("ent", "PERSON", 0, 3, presidio=0.1 * i),
            ])
            for i in range(1, 6)
        ]
        df = spark.createDataFrame(rows, DOC_SCHEMA)
        queue = build_review_queue(df, top_k=3)
        result = queue.collect()
        assert len(result) == 3

    def test_most_uncertain_first(self, spark):
        df = spark.createDataFrame([
            Row(doc_id="confident", aligned_entities=[
                _entity("John", "PERSON", 0, 4, presidio=0.99),
            ]),
            Row(doc_id="uncertain", aligned_entities=[
                _entity("maybe", "PERSON", 0, 5, presidio=0.15),
            ]),
        ], DOC_SCHEMA)
        queue = build_review_queue(df, top_k=2)
        result = queue.collect()
        assert result[0]["doc_id"] == "uncertain"


class TestComputeDetectorDisagreement:
    """Derives source count from non-null presidio/gliner/ai score fields."""

    def test_multi_source_low_disagreement(self, spark):
        df = spark.createDataFrame([
            Row(doc_id="d1", aligned_entities=[
                _entity("John", "PERSON", 0, 4, presidio=0.9, ai=0.85),
            ]),
        ], DOC_SCHEMA)
        result = compute_detector_disagreement(df).collect()
        row = result[0]
        assert row["single_source_entities"] == 0
        assert row["disagreement_score"] == pytest.approx(0.0)

    def test_single_source_high_disagreement(self, spark):
        df = spark.createDataFrame([
            Row(doc_id="d1", aligned_entities=[
                _entity("John", "PERSON", 0, 4, presidio=0.9),
                _entity("Acme", "ORG", 10, 14, ai=0.7),
            ]),
        ], DOC_SCHEMA)
        result = compute_detector_disagreement(df).collect()
        row = result[0]
        assert row["single_source_entities"] == 2
        assert row["disagreement_score"] == pytest.approx(1.0)

    def test_mixed_sources(self, spark):
        df = spark.createDataFrame([
            Row(doc_id="d1", aligned_entities=[
                _entity("John", "PERSON", 0, 4, presidio=0.9, ai=0.85),
                _entity("Acme", "ORG", 10, 14, gliner=0.7),
            ]),
        ], DOC_SCHEMA)
        result = compute_detector_disagreement(df).collect()
        row = result[0]
        assert row["single_source_entities"] == 1
        assert row["disagreement_score"] == pytest.approx(0.5)

    def test_missing_column_raises(self, spark):
        df = spark.createDataFrame([Row(doc_id="d1", text="hello")])
        with pytest.raises(ValueError, match="aligned_entities column required"):
            compute_detector_disagreement(df)
