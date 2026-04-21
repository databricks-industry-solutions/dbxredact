"""Tests for Spanish language modes, translation, and review detection."""

import sys
from unittest.mock import MagicMock

_pyspark_mods = [
    "pyspark", "pyspark.sql", "pyspark.sql.functions", "pyspark.sql.types",
    "pyspark.sql.streaming",
]
for _mod in _pyspark_mods:
    if _mod not in sys.modules:
        sys.modules[_mod] = MagicMock()

import json
import pytest
from dbxredact.config import (
    RedactionConfig,
    PHI_PROMPT_SKELETON,
    PHI_PROMPT_SKELETON_ES,
    TRANSLATION_PROMPT_SKELETON,
    REVIEW_PROMPT_SKELETON,
    PROMPT_SKELETON_BY_LANGUAGE,
    LABEL_ENUMS,
)
from dbxredact.ai_detector import make_prompt


class TestRedactionConfigLanguage:
    """RedactionConfig language and translate_to validation."""

    def test_default_language_is_english(self):
        cfg = RedactionConfig()
        assert cfg.language == "en"
        assert cfg.translate_to is None

    def test_spanish_language_accepted(self):
        cfg = RedactionConfig(language="es", use_presidio=False, use_ai_query=True)
        assert cfg.language == "es"

    def test_unsupported_language_raises(self):
        with pytest.raises(ValueError, match="not supported"):
            RedactionConfig(language="fr")

    def test_translate_to_same_language_raises(self):
        with pytest.raises(ValueError, match="same as language"):
            RedactionConfig(language="es", translate_to="es")

    def test_translate_to_different_language_ok(self):
        cfg = RedactionConfig(language="es", translate_to="en")
        assert cfg.translate_to == "en"


class TestPromptSkeletons:
    """Prompt skeleton formatting and content."""

    def test_spanish_prompt_has_label_placeholder(self):
        assert "{label_enums}" in PHI_PROMPT_SKELETON_ES

    def test_spanish_prompt_has_med_text_placeholder(self):
        formatted = PHI_PROMPT_SKELETON_ES.format(label_enums="[]")
        assert "{med_text}" in formatted

    def test_spanish_prompt_formats_with_make_prompt(self):
        result = make_prompt(PHI_PROMPT_SKELETON_ES, LABEL_ENUMS)
        assert "PERSON" in result
        assert "{med_text}" in result
        assert "{label_enums}" not in result

    def test_english_prompt_formats_with_make_prompt(self):
        result = make_prompt(PHI_PROMPT_SKELETON, LABEL_ENUMS)
        assert "PERSON" in result
        assert "{med_text}" in result

    def test_language_map_has_both_languages(self):
        assert "en" in PROMPT_SKELETON_BY_LANGUAGE
        assert "es" in PROMPT_SKELETON_BY_LANGUAGE
        assert PROMPT_SKELETON_BY_LANGUAGE["en"] is PHI_PROMPT_SKELETON
        assert PROMPT_SKELETON_BY_LANGUAGE["es"] is PHI_PROMPT_SKELETON_ES

    def test_spanish_prompt_contains_spanish_text(self):
        assert "Información de Salud Protegida" in PHI_PROMPT_SKELETON_ES
        assert "TextoMedico" in PHI_PROMPT_SKELETON_ES

    def test_spanish_prompt_has_spanish_examples(self):
        assert "María García López" in PHI_PROMPT_SKELETON_ES
        assert "CURP" in PHI_PROMPT_SKELETON_ES


class TestTranslationPrompt:
    """Translation prompt formatting."""

    def test_translation_prompt_has_placeholders(self):
        assert "{source_lang}" in TRANSLATION_PROMPT_SKELETON
        assert "{target_lang}" in TRANSLATION_PROMPT_SKELETON

    def test_translation_prompt_formats_correctly(self):
        formatted = TRANSLATION_PROMPT_SKELETON.format(
            source_lang="Spanish", target_lang="English"
        )
        assert "Spanish" in formatted
        assert "English" in formatted
        assert "{text}" in formatted

    def test_translation_prompt_mentions_redaction_markers(self):
        assert "[REDACTED]" in TRANSLATION_PROMPT_SKELETON
        assert "[PERSON]" in TRANSLATION_PROMPT_SKELETON

    def test_translation_prompt_preserves_markers_instruction(self):
        lower = TRANSLATION_PROMPT_SKELETON.lower()
        assert "preserve" in lower or "do not translate" in lower


class TestReviewPrompt:
    """Review prompt formatting and marker-awareness."""

    def test_review_prompt_has_label_placeholder(self):
        assert "{label_enums}" in REVIEW_PROMPT_SKELETON

    def test_review_prompt_has_med_text_placeholder(self):
        formatted = REVIEW_PROMPT_SKELETON.format(label_enums="[]")
        assert "{med_text}" in formatted

    def test_review_prompt_mentions_ignore_markers(self):
        lower = REVIEW_PROMPT_SKELETON.lower()
        assert "ignore" in lower
        assert "[redacted]" in lower or "redaction markers" in lower

    def test_review_prompt_formats_with_make_prompt(self):
        result = make_prompt(REVIEW_PROMPT_SKELETON, LABEL_ENUMS)
        assert "PERSON" in result
        assert "{label_enums}" not in result

    def test_review_prompt_handles_empty_results(self):
        assert "empty list" in REVIEW_PROMPT_SKELETON or "[]" in REVIEW_PROMPT_SKELETON
