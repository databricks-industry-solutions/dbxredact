"""Integration tests for redaction pipeline.

Note: TestImportSmoke requires pyspark (runs on cluster).
TestEndToEndWorkflow is self-contained and runs locally.
"""

import sys
from unittest.mock import MagicMock

_pyspark_mods = [
    "pyspark", "pyspark.sql", "pyspark.sql.functions", "pyspark.sql.types",
    "pyspark.sql.streaming",
]
for _mod in _pyspark_mods:
    if _mod not in sys.modules:
        sys.modules[_mod] = MagicMock()

import pytest


class TestImportSmoke:
    """Single test verifying the public API surface is importable."""

    def test_public_api_importable(self):
        import dbxredact

        public_names = [
            "PRESIDIO_ENTITY_TYPES", "LABEL_ENUMS", "PHI_PROMPT_SKELETON",
            "is_fuzzy_match", "is_overlap",
            "run_presidio_detection", "run_ai_query_detection",
            "run_gliner_detection", "run_detection",
            "redact_text", "create_redaction_udf", "create_redacted_table",
            "run_detection_pipeline", "run_redaction_pipeline",
            "run_redaction_pipeline_streaming",
            "evaluate_detection", "calculate_metrics",
            "get_columns_by_tag", "get_protected_columns",
        ]
        missing = [n for n in public_names if not hasattr(dbxredact, n)]
        assert not missing, f"Missing exports: {missing}"


class TestEndToEndWorkflow:
    """Test end-to-end workflow components."""

    def test_redaction_workflow(self):
        from dbxredact.redaction import redact_text

        text = "Patient John Smith (SSN: 123-45-6789) visited on 2024-01-15."
        entities = [
            {"entity": "John Smith", "start": 8, "end": 17, "entity_type": "PERSON"},
            {"entity": "123-45-6789", "start": 25, "end": 35, "entity_type": "US_SSN"},
            {"entity": "2024-01-15", "start": 49, "end": 58, "entity_type": "DATE"},
        ]

        generic_result = redact_text(text, entities, strategy="generic")
        assert "John Smith" not in generic_result
        assert "123-45-6789" not in generic_result
        assert "[REDACTED]" in generic_result

        typed_result = redact_text(text, entities, strategy="typed")
        assert "[PERSON]" in typed_result
        assert "[US_SSN]" in typed_result
        assert "[DATE]" in typed_result
