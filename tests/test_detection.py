"""Unit tests for detection module."""

import pytest

from dbxredact.config import (
    PRESIDIO_ENTITY_TYPES,
    LABEL_ENUMS,
    PHI_PROMPT_SKELETON,
    DEFAULT_PRESIDIO_SCORE_THRESHOLD,
)
from dbxredact.ai_detector import make_prompt
from dbxredact.detection import check_presidio_available


class TestConfig:
    """Tests for configuration constants."""

    def test_presidio_entity_types_defined(self):
        """Test that Presidio entity types are defined."""
        assert len(PRESIDIO_ENTITY_TYPES) > 0
        assert "PERSON" in PRESIDIO_ENTITY_TYPES
        assert "EMAIL_ADDRESS" in PRESIDIO_ENTITY_TYPES

    def test_label_enums_defined(self):
        """Test that label enums are defined."""
        assert len(LABEL_ENUMS) > 0
        assert "PERSON" in LABEL_ENUMS

    def test_prompt_skeleton_defined(self):
        """Test that prompt skeleton is defined."""
        assert len(PHI_PROMPT_SKELETON) > 0
        assert "{label_enums}" in PHI_PROMPT_SKELETON

    def test_default_threshold(self):
        """Test default threshold value."""
        assert 0 <= DEFAULT_PRESIDIO_SCORE_THRESHOLD <= 1


class TestMakePrompt:
    """Tests for prompt creation."""

    def test_make_prompt_with_list(self):
        """Test prompt creation with list of labels."""
        prompt = make_prompt(labels=["PERSON", "EMAIL"])

        assert "PERSON" in prompt
        assert "EMAIL" in prompt
        assert "{label_enums}" not in prompt

    def test_make_prompt_with_default(self):
        """Test prompt creation with default labels."""
        prompt = make_prompt()

        assert "{label_enums}" not in prompt
        assert len(prompt) > 0

    def test_make_prompt_placeholder_replaced(self):
        """Test that placeholder is properly replaced."""
        template = "Labels: {label_enums}"
        prompt = make_prompt(prompt_skeleton=template, labels=["PERSON"])

        assert prompt == 'Labels: ["PERSON"]'


class TestDetectionInterface:
    """Tests for detection interface (without Spark)."""

    def test_import_detection_functions(self):
        """Test that detection functions can be imported."""
        from dbxredact.detection import (
            run_presidio_detection,
            run_ai_query_detection,
            run_gliner_detection,
            run_detection,
        )

        # Just verify imports work
        assert callable(run_presidio_detection)
        assert callable(run_ai_query_detection)
        assert callable(run_gliner_detection)
        assert callable(run_detection)

    def test_import_presidio_functions(self):
        """Test that presidio functions can be imported."""
        from dbxredact.presidio import (
            format_presidio_batch_results,
            make_presidio_batch_udf,
        )

        assert callable(format_presidio_batch_results)
        assert callable(make_presidio_batch_udf)


class TestPresidioAvailability:
    """Tests for Presidio availability checking."""

    def test_check_presidio_available_returns_tuple(self):
        """Test that check_presidio_available returns a tuple."""
        result = check_presidio_available()
        assert isinstance(result, tuple)
        assert len(result) == 2

    def test_check_presidio_available_first_element_is_bool(self):
        """Test that first element is boolean."""
        is_available, _ = check_presidio_available()
        assert isinstance(is_available, bool)

    def test_check_presidio_available_message_when_unavailable(self):
        """Test that error message is provided when unavailable."""
        is_available, error_msg = check_presidio_available()
        if not is_available:
            assert error_msg is not None
            assert len(error_msg) > 0
        else:
            assert error_msg is None


class TestAIQueryPromptBuilding:
    """Tests for AI Query prompt building (used in streaming)."""

    def test_prompt_contains_med_text_placeholder(self):
        """Test that default prompt contains {med_text} placeholder."""
        assert "{med_text}" in PHI_PROMPT_SKELETON

    def test_prompt_can_be_split_for_streaming(self):
        """Test that prompt can be split at {med_text} for streaming concat."""
        prompt = make_prompt()
        parts = prompt.split("{med_text}")
        # Should have exactly 2 parts (before and after placeholder)
        assert len(parts) == 2
        assert len(parts[0]) > 0  # Prefix should not be empty

    def test_prompt_escaping_for_sql(self):
        """Test that prompt can be escaped for SQL expressions."""
        prompt = make_prompt()
        # Single quotes should be escapable
        escaped = prompt.replace("'", "''")
        assert "''" in escaped or "'" not in prompt


class TestDetectionDefaults:
    """Tests for detection module defaults."""

    def test_default_endpoint_is_gpt_oss(self):
        """Test that default endpoint is databricks-gpt-oss-120b."""
        from dbxredact.detection import run_ai_query_detection
        import inspect
        
        sig = inspect.signature(run_ai_query_detection)
        endpoint_default = sig.parameters["endpoint"].default
        assert endpoint_default == "databricks-gpt-oss-120b"

    def test_default_score_threshold_is_valid(self):
        """Test that default score threshold is in valid range."""
        assert 0.0 <= DEFAULT_PRESIDIO_SCORE_THRESHOLD <= 1.0

