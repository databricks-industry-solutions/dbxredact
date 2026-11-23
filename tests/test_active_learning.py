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

    def test_uncertainty_score_clamped_to_unit_interval(self, spark):
        """Score=0.0 + all low-conf would yield 1.3 unclamped; must be clamped to 1.0."""
        df = spark.createDataFrame([
            Row(doc_id="d1", aligned_entities=[
                _entity("a", "PERSON", 0, 1, presidio=0.0),
                _entity("b", "PERSON", 2, 3, presidio=0.0),
            ]),
        ], DOC_SCHEMA)
        result = compute_document_uncertainty(df).collect()
        assert result[0]["uncertainty_score"] == pytest.approx(1.0)

    def test_uncertainty_score_minimum_bounded(self, spark):
        """Perfect scores should yield uncertainty >= 0.0."""
        df = spark.createDataFrame([
            Row(doc_id="d1", aligned_entities=[
                _entity("John", "PERSON", 0, 4, presidio=1.0),
                _entity("Jane", "PERSON", 5, 9, presidio=1.0),
            ]),
        ], DOC_SCHEMA)
        result = compute_document_uncertainty(df).collect()
        assert result[0]["uncertainty_score"] >= 0.0


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


class TestCalibratedUncertainty:

    def test_calibrated_uncertainty_uses_calibrated_scores(self, spark):
        """With calibration, raw low scores are mapped higher, reducing uncertainty."""
        from dbxredact.calibration import CalibratedScorer

        cal = CalibratedScorer()
        cal.fit("presidio", [0.1, 0.3, 0.5, 0.7, 0.9], [0, 0, 1, 1, 1])

        df = spark.createDataFrame([
            Row(doc_id="d1", aligned_entities=[
                _entity("John", "PERSON", 0, 4, presidio=0.3),
                _entity("Jane", "PERSON", 5, 9, presidio=0.3),
            ]),
        ], DOC_SCHEMA)

        raw_result = compute_document_uncertainty(df).collect()[0]
        cal_result = compute_document_uncertainty(df, calibration=cal).collect()[0]

        assert cal_result["avg_score"] != pytest.approx(raw_result["avg_score"], abs=0.01)
        assert 0.0 <= cal_result["uncertainty_score"] <= 1.0

    def test_calibration_none_unchanged(self, spark):
        """calibration=None should produce the same result as omitting it."""
        df = spark.createDataFrame([
            Row(doc_id="d1", aligned_entities=[
                _entity("John", "PERSON", 0, 4, presidio=0.8),
            ]),
        ], DOC_SCHEMA)

        r1 = compute_document_uncertainty(df).collect()[0]
        r2 = compute_document_uncertainty(df, calibration=None).collect()[0]
        assert r1["uncertainty_score"] == pytest.approx(r2["uncertainty_score"])
