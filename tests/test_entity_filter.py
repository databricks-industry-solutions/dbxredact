"""Tests for entity_filter.py -- EntityFilter, apply_safe_filter, apply_block_filter."""

import tempfile
import os
import pytest

from dbxredact.entity_filter import (
    EntityFilter,
    apply_safe_filter,
    apply_block_filter,
    load_filter_from_yaml,
)


class TestEntityFilter:
    def test_safe_list_lowercases(self):
        ef = EntityFilter(safe_list=["John Doe", "ACME Corp"])
        assert "john doe" in ef._safe_set
        assert "acme corp" in ef._safe_set

    def test_block_list_lowercases(self):
        ef = EntityFilter(block_list=["SSN-123"])
        assert "ssn-123" in ef._block_set

    def test_safe_patterns_compile(self):
        ef = EntityFilter(safe_patterns=[r"\d{3}-\d{2}-\d{4}"])
        assert len(ef._safe_re) == 1

    def test_block_patterns_compile(self):
        ef = EntityFilter(block_patterns=[r"SSN-\d+"])
        assert len(ef._block_re) == 1

    def test_empty_filter(self):
        ef = EntityFilter()
        assert ef._safe_set == set()
        assert ef._block_set == set()
        assert ef._safe_re == []
        assert ef._block_re == []


class TestApplySafeFilter:
    def test_removes_exact_match(self):
        ef = EntityFilter(safe_list=["John Doe"])
        entities = [
            {"entity": "John Doe", "entity_type": "PERSON", "start": 0, "end": 8, "score": 0.9},
            {"entity": "Jane Smith", "entity_type": "PERSON", "start": 10, "end": 20, "score": 0.8},
        ]
        result = apply_safe_filter(entities, ef)
        assert len(result) == 1
        assert result[0]["entity"] == "Jane Smith"

    def test_case_insensitive(self):
        ef = EntityFilter(safe_list=["john doe"])
        entities = [{"entity": "John Doe", "entity_type": "PERSON", "start": 0, "end": 8, "score": 0.9}]
        result = apply_safe_filter(entities, ef)
        assert len(result) == 0

    def test_removes_pattern_match(self):
        ef = EntityFilter(safe_patterns=[r"^test_"])
        entities = [
            {"entity": "test_user", "entity_type": "PERSON", "start": 0, "end": 9, "score": 0.9},
            {"entity": "real_user", "entity_type": "PERSON", "start": 10, "end": 19, "score": 0.8},
        ]
        result = apply_safe_filter(entities, ef)
        assert len(result) == 1
        assert result[0]["entity"] == "real_user"

    def test_empty_entities(self):
        ef = EntityFilter(safe_list=["anything"])
        assert apply_safe_filter([], ef) == []


class TestApplyBlockFilter:
    def test_finds_exact_term(self):
        ef = EntityFilter(block_list=["secret-key"])
        result = apply_block_filter("The secret-key is here", ef)
        assert len(result) == 1
        assert result[0]["entity"] == "secret-key"
        assert result[0]["start"] == 4
        assert result[0]["end"] == 14
        assert result[0]["entity_type"] == "BLOCK_LIST"
        assert result[0]["score"] == 1.0

    def test_finds_multiple_occurrences(self):
        ef = EntityFilter(block_list=["abc"])
        result = apply_block_filter("abc and abc", ef)
        assert len(result) == 2

    def test_finds_pattern_match(self):
        ef = EntityFilter(block_patterns=[r"SSN-\d{3}"])
        result = apply_block_filter("My SSN-123 is here", ef)
        assert len(result) == 1
        assert result[0]["entity"] == "SSN-123"

    def test_empty_text(self):
        ef = EntityFilter(block_list=["something"])
        assert apply_block_filter("", ef) == []

    def test_no_match(self):
        ef = EntityFilter(block_list=["missing"])
        assert apply_block_filter("nothing to find", ef) == []


class TestLoadFilterFromYaml:
    def test_roundtrip(self):
        content = """
safe_list:
  - John Doe
  - ACME Corp
block_list:
  - secret-key
safe_patterns:
  - '^test_'
block_patterns:
  - 'SSN-\\d+'
"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as f:
            f.write(content)
            path = f.name
        try:
            ef = load_filter_from_yaml(path)
            assert "john doe" in ef._safe_set
            assert "acme corp" in ef._safe_set
            assert "secret-key" in ef._block_set
            assert len(ef._safe_re) == 1
            assert len(ef._block_re) == 1
        finally:
            os.unlink(path)

    def test_empty_file(self):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as f:
            f.write("")
            path = f.name
        try:
            ef = load_filter_from_yaml(path)
            assert ef.safe_list == []
            assert ef.block_list == []
        finally:
            os.unlink(path)
