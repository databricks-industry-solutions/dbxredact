"""Tests for config.py -- should_ignore_entity patterns."""

import pytest
from dbxredact.config import should_ignore_entity


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

    @pytest.mark.parametrize("text", ["doctor", "nurse", "patient", "physician", "surgeon"])
    def test_clinical_roles_ignored(self, text):
        assert should_ignore_entity(text, "PERSON") is True

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
