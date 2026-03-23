"""Tests for config.py -- should_ignore_entity patterns."""

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
from dbxredact.config import (
    should_ignore_entity, RedactionConfig,
    DEFAULT_PRESIDIO_SCORE_THRESHOLD, DEFAULT_GLINER_MODEL,
    DEFAULT_GLINER_THRESHOLD, DEFAULT_AI_REASONING_EFFORT,
    MIN_SCORE_THRESHOLD, MIN_GLINER_THRESHOLD,
)


class TestShouldIgnoreEntity:
    """Edge cases for the entity ignore logic."""

    @pytest.mark.parametrize("text", ["1990", "2024", "1955", "2001"])
    def test_bare_years_ignored(self, text):
        assert should_ignore_entity(text, "DATE_TIME") is True

    @pytest.mark.parametrize("text", ["today", "Yesterday", "TOMORROW"])
    def test_relative_days_ignored(self, text):
        assert should_ignore_entity(text, "DATE_TIME") is True

    @pytest.mark.parametrize("text", [
        "3 days", "2 weeks", "10 months", "5 years",
        "two days", "three weeks", "one month",
    ])
    def test_relative_durations_ignored(self, text):
        assert should_ignore_entity(text, "DATE_TIME") is True

    @pytest.mark.parametrize("text", ["approximately one hour ago", "5 minutes ago"])
    def test_ago_phrases_ignored(self, text):
        assert should_ignore_entity(text, "DATE_TIME") is True

    @pytest.mark.parametrize("text", ["daily", "weekly", "monthly", "annually"])
    def test_frequency_words_ignored(self, text):
        assert should_ignore_entity(text, "DATE_TIME") is True

    @pytest.mark.parametrize("text", [
        "Dr.", "mr", "Mrs.", "Ms", "prof", "sir", "madam",
    ])
    def test_titles_ignored(self, text):
        assert should_ignore_entity(text, "PERSON") is True

    @pytest.mark.parametrize("text", ["he", "she", "they", "you", "we", "it", "i"])
    def test_pronouns_ignored(self, text):
        assert should_ignore_entity(text, "PERSON") is True

    @pytest.mark.parametrize("text", ["nurse", "physician", "surgeon"])
    def test_clinical_roles_ignored(self, text):
        assert should_ignore_entity(text, "PERSON") is True

    @pytest.mark.parametrize("text", ["doctor", "patient"])
    def test_doctor_patient_not_ignored(self, text):
        """'doctor' and 'patient' can appear in compound names."""
        assert should_ignore_entity(text, "PERSON") is False

    @pytest.mark.parametrize("text", ["am", "pm"])
    def test_bare_time_fragments_ignored(self, text):
        assert should_ignore_entity(text, "DATE_TIME") is True

    def test_nrp_entity_type_ignored(self):
        assert should_ignore_entity("American", "NRP") is True

    @pytest.mark.parametrize("text", ["CA", "NY", "TX", "OR"])
    def test_short_location_ignored(self, text):
        assert should_ignore_entity(text, "LOCATION") is True

    def test_real_person_name_not_ignored(self):
        assert should_ignore_entity("John Smith", "PERSON") is False

    def test_full_date_not_ignored(self):
        assert should_ignore_entity("January 5, 2024", "DATE_TIME") is False

    def test_real_location_not_ignored(self):
        assert should_ignore_entity("San Francisco", "LOCATION") is False

    def test_email_not_ignored(self):
        assert should_ignore_entity("test@email.com", "EMAIL") is False

    def test_postoperative_day_ignored(self):
        assert should_ignore_entity("postoperative day two", "DATE_TIME") is True

    def test_past_duration_ignored(self):
        assert should_ignore_entity("past 3 months", "DATE_TIME") is True

    # --- I5: refined ignore filter ---

    @pytest.mark.parametrize("text", ["J.", "42", "Ab"])
    def test_two_char_identifiers_not_ignored(self, text):
        """2-char strings that look like identifiers (digits, mixed case, period) pass through."""
        assert should_ignore_entity(text, "PERSON") is False

    @pytest.mark.parametrize("text", ["Dr", "he", "it"])
    def test_two_char_non_identifiers_ignored(self, text):
        """2-char lowercase words or titles should still be ignored."""
        assert should_ignore_entity(text, "PERSON") is True

    def test_single_char_always_ignored(self):
        assert should_ignore_entity("A", "PERSON") is True

    def test_configurable_types_to_ignore(self):
        """Passing custom types_to_ignore overrides the default."""
        assert should_ignore_entity("American", "NRP") is True
        assert should_ignore_entity("American", "NRP", types_to_ignore=set()) is False


class TestRedactionConfig:
    """Verify RedactionConfig defaults match the old function-signature defaults."""

    def test_defaults(self):
        cfg = RedactionConfig()
        assert cfg.use_presidio is True
        assert cfg.use_ai_query is True
        assert cfg.use_gliner is False
        assert cfg.endpoint is None
        assert cfg.score_threshold == DEFAULT_PRESIDIO_SCORE_THRESHOLD
        assert cfg.gliner_model == DEFAULT_GLINER_MODEL
        assert cfg.gliner_threshold == DEFAULT_GLINER_THRESHOLD
        assert cfg.gliner_max_words is None
        assert cfg.num_cores == 10
        assert cfg.fail_on_presidio_error is True
        assert cfg.reasoning_effort == DEFAULT_AI_REASONING_EFFORT
        assert cfg.presidio_model_size is None
        assert cfg.presidio_pattern_only is False
        assert cfg.ai_model_type == "foundation"
        assert cfg.alignment_mode == "union"
        assert cfg.fuzzy_threshold == 50
        assert cfg.allow_consensus_redaction is False
        assert cfg.redaction_strategy == "generic"
        assert cfg.output_strategy == "production"
        assert cfg.output_mode == "separate"
        assert cfg.confirm_destructive is False
        assert cfg.max_rows == 10000
        assert cfg.entity_filter is None

    def test_custom_values(self):
        cfg = RedactionConfig(
            use_presidio=False, use_gliner=True, num_cores=4,
            redaction_strategy="typed", output_mode="in_place",
            confirm_destructive=True, max_rows=None,
        )
        assert cfg.use_presidio is False
        assert cfg.use_gliner is True
        assert cfg.num_cores == 4
        assert cfg.redaction_strategy == "typed"
        assert cfg.output_mode == "in_place"
        assert cfg.confirm_destructive is True
        assert cfg.max_rows is None


class TestGovernanceValidation:
    """RedactionConfig.__post_init__ enforces governance floors on thresholds."""

    def test_score_threshold_below_floor_raises(self):
        with pytest.raises(ValueError, match="governance"):
            RedactionConfig(score_threshold=0.01)

    def test_gliner_threshold_below_floor_raises(self):
        with pytest.raises(ValueError, match="governance"):
            RedactionConfig(gliner_threshold=0.001)

    def test_score_threshold_at_floor_succeeds(self):
        cfg = RedactionConfig(score_threshold=MIN_SCORE_THRESHOLD)
        assert cfg.score_threshold == MIN_SCORE_THRESHOLD

    def test_gliner_threshold_at_floor_succeeds(self):
        cfg = RedactionConfig(gliner_threshold=MIN_GLINER_THRESHOLD)
        assert cfg.gliner_threshold == MIN_GLINER_THRESHOLD

    def test_score_threshold_above_floor_succeeds(self):
        cfg = RedactionConfig(score_threshold=0.5)
        assert cfg.score_threshold == 0.5

