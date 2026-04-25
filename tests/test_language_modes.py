"""Tests for Spanish language mode, translation prompts, and config validation."""

import pytest
from dbxredact.config import (
    RedactionConfig,
    PHI_PROMPT_SKELETON_ES,
    PROMPT_SKELETON_BY_LANGUAGE,
    TRANSLATION_PROMPT_SKELETON,
    REVIEW_PROMPT_SKELETON,
    PHI_PROMPT_SKELETON,
)
from dbxredact.ai_detector import make_prompt


class TestRedactionConfigLanguage:
    def test_english_default(self):
        cfg = RedactionConfig()
        assert cfg.language == "en"
        assert cfg.translate_to is None

    def test_spanish_accepted(self):
        cfg = RedactionConfig(language="es")
        assert cfg.language == "es"

    def test_unsupported_language_raises(self):
        with pytest.raises(ValueError, match="not supported"):
            RedactionConfig(language="fr")

    def test_translate_to_same_as_language_raises(self):
        with pytest.raises(ValueError, match="must differ"):
            RedactionConfig(language="es", translate_to="es")

    def test_translate_to_different_ok(self):
        cfg = RedactionConfig(language="es", translate_to="en")
        assert cfg.translate_to == "en"

    def test_translate_to_none_ok(self):
        cfg = RedactionConfig(language="es", translate_to=None)
        assert cfg.translate_to is None

    def test_translate_to_unsupported_language_raises(self):
        with pytest.raises(ValueError, match="not supported"):
            RedactionConfig(language="es", translate_to="fr")


class TestPromptSkeletons:
    def test_language_map_has_en_and_es(self):
        assert "en" in PROMPT_SKELETON_BY_LANGUAGE
        assert "es" in PROMPT_SKELETON_BY_LANGUAGE
        assert PROMPT_SKELETON_BY_LANGUAGE["en"] is PHI_PROMPT_SKELETON

    def test_spanish_prompt_has_label_placeholder(self):
        assert "{label_enums}" in PHI_PROMPT_SKELETON_ES

    def test_spanish_prompt_has_med_text_placeholder(self):
        assert "{med_text}" in PHI_PROMPT_SKELETON_ES

    def test_make_prompt_with_spanish_skeleton(self):
        prompt = make_prompt(PHI_PROMPT_SKELETON_ES)
        assert "PERSON" in prompt
        assert "{label_enums}" not in prompt

    def test_translation_prompt_has_placeholders(self):
        assert "{source_lang}" in TRANSLATION_PROMPT_SKELETON
        assert "{target_lang}" in TRANSLATION_PROMPT_SKELETON
        assert "{text}" in TRANSLATION_PROMPT_SKELETON

    def test_review_prompt_has_bracket_instructions(self):
        assert "bracket" in REVIEW_PROMPT_SKELETON.lower() or "BRACKET" in REVIEW_PROMPT_SKELETON
        assert "{label_enums}" in REVIEW_PROMPT_SKELETON

    def test_review_prompt_has_med_text_placeholder(self):
        assert "{med_text}" in REVIEW_PROMPT_SKELETON
