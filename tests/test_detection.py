"""Unit tests for detection module."""

import pytest

from dbxredact.config import PHI_PROMPT_SKELETON
from dbxredact.ai_detector import make_prompt
from dbxredact.detection import check_presidio_available


class TestMakePrompt:
    """Tests for prompt creation."""

    def test_make_prompt_with_list(self):
        prompt = make_prompt(labels=["PERSON", "EMAIL"])
        assert "PERSON" in prompt
        assert "EMAIL" in prompt
        assert "{label_enums}" not in prompt

    def test_make_prompt_with_default(self):
        prompt = make_prompt()
        assert "{label_enums}" not in prompt
        assert len(prompt) > 0

    def test_make_prompt_placeholder_replaced(self):
        template = "Labels: {label_enums}"
        prompt = make_prompt(prompt_skeleton=template, labels=["PERSON"])
        assert prompt == 'Labels: ["PERSON"]'


class TestPresidioAvailability:

    def test_returns_bool_and_optional_message(self):
        is_available, error_msg = check_presidio_available()
        assert isinstance(is_available, bool)
        assert (error_msg is None) == is_available


class TestAIQueryPromptBuilding:
    """Tests for AI Query prompt building (used in streaming)."""

    def test_prompt_contains_med_text_placeholder(self):
        assert "{med_text}" in PHI_PROMPT_SKELETON

    def test_prompt_can_be_split_for_streaming(self):
        prompt = make_prompt()
        parts = prompt.split("{med_text}")
        assert len(parts) == 2
        assert len(parts[0]) > 0

    def test_prompt_escaping_for_sql(self):
        prompt = make_prompt()
        escaped = prompt.replace("'", "''")
        assert "''" in escaped or "'" not in prompt
