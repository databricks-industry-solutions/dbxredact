"""Spark integration tests for the detection/redaction pipeline."""

import pytest
from unittest.mock import patch, MagicMock
from pyspark.sql import Row
from pyspark.sql.types import (
    StructType, StructField, StringType, ArrayType,
    IntegerType, DoubleType,
)
from dbxredact.redaction import redact_text, create_redaction_udf
from dbxredact.alignment import align_entities_multi_source


@pytest.fixture
def sample_df(spark):
    data = [
        Row(doc_id="d1", text="John Smith lives at 123 Main St."),
        Row(doc_id="d2", text="Call Jane Doe at 555-0100."),
    ]
    return spark.createDataFrame(data)


class TestRedactionUDF:

    def test_redaction_udf_generic(self, spark):
        entity_schema = ArrayType(StructType([
            StructField("entity", StringType()),
            StructField("entity_type", StringType()),
            StructField("start", IntegerType()),
            StructField("end", IntegerType()),
            StructField("score", DoubleType()),
        ]))
        data = [(
            "John Smith is here",
            [Row(entity="John Smith", entity_type="PERSON", start=0, end=10, score=0.9)],
        )]
        schema = StructType([
            StructField("text", StringType()),
            StructField("entities", entity_schema),
        ])
        df = spark.createDataFrame(data, schema)
        udf = create_redaction_udf(strategy="generic")
        result = df.withColumn("redacted", udf("text", "entities")).collect()
        assert "[REDACTED]" in result[0]["redacted"]
        assert "John Smith" not in result[0]["redacted"]

    def test_redaction_udf_typed(self, spark):
        entity_schema = ArrayType(StructType([
            StructField("entity", StringType()),
            StructField("entity_type", StringType()),
            StructField("start", IntegerType()),
            StructField("end", IntegerType()),
            StructField("score", DoubleType()),
        ]))
        data = [(
            "John Smith is here",
            [Row(entity="John Smith", entity_type="PERSON", start=0, end=10, score=0.9)],
        )]
        schema = StructType([
            StructField("text", StringType()),
            StructField("entities", entity_schema),
        ])
        df = spark.createDataFrame(data, schema)
        udf = create_redaction_udf(strategy="typed")
        result = df.withColumn("redacted", udf("text", "entities")).collect()
        assert "[PERSON]" in result[0]["redacted"]


class TestAlignmentUnionConsensus:

    def test_union_keeps_all(self):
        presidio = [{"entity": "John", "start": 0, "end": 4, "entity_type": "PERSON", "score": 0.9}]
        ai = [{"entity": "123 Main", "start": 20, "end": 28, "entity_type": "ADDRESS"}]
        result = align_entities_multi_source(
            presidio_entities=presidio, gliner_entities=None,
            ai_entities=ai, doc_id="d1",
        )
        entities = [r["entity"] for r in result]
        assert "John" in entities
        assert "123 Main" in entities

    def test_consensus_requires_agreement(self):
        presidio = [{"entity": "John", "start": 0, "end": 4, "entity_type": "PERSON", "score": 0.9}]
        ai = [{"entity": "John", "start": 0, "end": 4, "entity_type": "PERSON"}]
        gliner = []
        result = align_entities_multi_source(
            presidio_entities=presidio, gliner_entities=gliner,
            ai_entities=ai, doc_id="d1", mode="consensus",
        )
        assert len(result) >= 1
        assert result[0]["entity"] == "John"
