"""Integration tests for redaction pipeline."""

import pytest


class TestPipelineImports:
    """Test that pipeline modules can be imported."""

    def test_import_pipeline_functions(self):
        """Test that pipeline functions can be imported."""
        from dbxredact.pipeline import (
            run_detection_pipeline,
            run_redaction_pipeline,
            run_redaction_pipeline_by_tag,
            run_redaction_pipeline_streaming,
        )

        assert callable(run_detection_pipeline)
        assert callable(run_redaction_pipeline)
        assert callable(run_redaction_pipeline_by_tag)
        assert callable(run_redaction_pipeline_streaming)

    def test_import_metadata_functions(self):
        """Test that metadata functions can be imported."""
        from dbxredact.metadata import (
            get_columns_by_tag,
            get_protected_columns,
            get_table_metadata,
        )

        assert callable(get_columns_by_tag)
        assert callable(get_protected_columns)
        assert callable(get_table_metadata)

    def test_import_evaluation_functions(self):
        """Test that evaluation functions can be imported."""
        from dbxredact.evaluation import (
            evaluate_detection,
            calculate_metrics,
            format_contingency_table,
            format_metrics_summary,
            save_evaluation_results,
        )

        assert callable(evaluate_detection)
        assert callable(calculate_metrics)
        assert callable(format_contingency_table)
        assert callable(format_metrics_summary)
        assert callable(save_evaluation_results)


class TestModuleExports:
    """Test that main module exports all expected functions."""

    def test_main_module_exports(self):
        """Test main module exports."""
        import dbxredact

        # Config
        assert hasattr(dbxredact, "ELIGIBLE_ENTITY_TYPES")
        assert hasattr(dbxredact, "LABEL_ENUMS")
        assert hasattr(dbxredact, "PHI_PROMPT_SKELETON")

        # Utils
        assert hasattr(dbxredact, "is_fuzzy_match")
        assert hasattr(dbxredact, "is_overlap")

        # Detection
        assert hasattr(dbxredact, "run_presidio_detection")
        assert hasattr(dbxredact, "run_ai_query_detection")
        assert hasattr(dbxredact, "run_gliner_detection")
        assert hasattr(dbxredact, "run_detection")

        # Redaction
        assert hasattr(dbxredact, "redact_text")
        assert hasattr(dbxredact, "create_redaction_udf")
        assert hasattr(dbxredact, "create_redacted_table")

        # Pipeline
        assert hasattr(dbxredact, "run_detection_pipeline")
        assert hasattr(dbxredact, "run_redaction_pipeline")
        assert hasattr(dbxredact, "run_redaction_pipeline_streaming")

        # Evaluation
        assert hasattr(dbxredact, "evaluate_detection")
        assert hasattr(dbxredact, "calculate_metrics")

        # Metadata
        assert hasattr(dbxredact, "get_columns_by_tag")
        assert hasattr(dbxredact, "get_protected_columns")


class TestEndToEndWorkflow:
    """Test end-to-end workflow components."""

    def test_redaction_workflow(self):
        """Test a simple redaction workflow without Spark."""
        from dbxredact import redact_text

        # Sample input
        text = "Patient John Smith (SSN: 123-45-6789) visited on 2024-01-15."
        entities = [
            {"entity": "John Smith", "start": 8, "end": 17, "entity_type": "PERSON"},
            {"entity": "123-45-6789", "start": 25, "end": 35, "entity_type": "US_SSN"},
            {"entity": "2024-01-15", "start": 49, "end": 58, "entity_type": "DATE"},
        ]

        # Generic redaction
        generic_result = redact_text(text, entities, strategy="generic")
        assert "John Smith" not in generic_result
        assert "123-45-6789" not in generic_result
        assert "[REDACTED]" in generic_result

        # Typed redaction
        typed_result = redact_text(text, entities, strategy="typed")
        assert "[PERSON]" in typed_result
        assert "[US_SSN]" in typed_result
        assert "[DATE]" in typed_result

