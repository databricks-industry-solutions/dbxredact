"""Tests for metadata.py -- identifier validation, table name parsing, and column discovery."""

from unittest.mock import patch, MagicMock

import pytest
from dbxredact.metadata import _validate_identifier, _parse_table_name, discover_pii_columns


class TestValidateIdentifier:

    def test_accepts_simple(self):
        assert _validate_identifier("users", "table") == "users"

    def test_accepts_underscored(self):
        assert _validate_identifier("my_table_2", "table") == "my_table_2"

    def test_accepts_uppercase(self):
        assert _validate_identifier("MyTable", "table") == "MyTable"

    def test_rejects_dots(self):
        with pytest.raises(ValueError, match="Invalid table"):
            _validate_identifier("schema.table", "table")

    def test_rejects_semicolons(self):
        with pytest.raises(ValueError, match="Invalid table"):
            _validate_identifier("table; DROP", "table")

    def test_rejects_spaces(self):
        with pytest.raises(ValueError, match="Invalid table"):
            _validate_identifier("my table", "table")

    def test_rejects_quotes(self):
        with pytest.raises(ValueError, match="Invalid catalog"):
            _validate_identifier("cat'alog", "catalog")

    def test_rejects_dashes(self):
        with pytest.raises(ValueError):
            _validate_identifier("my-table", "table")

    def test_rejects_empty(self):
        with pytest.raises(ValueError):
            _validate_identifier("", "table")


class TestParseTableName:

    def test_three_part_name(self):
        cat, schema, table = _parse_table_name("main.default.users")
        assert cat == "main"
        assert schema == "default"
        assert table == "users"

    def test_rejects_two_parts(self):
        with pytest.raises(ValueError, match="fully qualified"):
            _parse_table_name("schema.table")

    def test_rejects_four_parts(self):
        with pytest.raises(ValueError, match="fully qualified"):
            _parse_table_name("a.b.c.d")

    def test_rejects_one_part(self):
        with pytest.raises(ValueError, match="fully qualified"):
            _parse_table_name("table")

    def test_rejects_invalid_catalog(self):
        with pytest.raises(ValueError, match="Invalid catalog"):
            _parse_table_name("cat;drop.schema.table")

    def test_rejects_invalid_schema(self):
        with pytest.raises(ValueError, match="Invalid schema"):
            _parse_table_name("catalog.sch ema.table")

    def test_rejects_invalid_table(self):
        with pytest.raises(ValueError, match="Invalid table"):
            _parse_table_name("catalog.schema.ta'ble")


class TestDiscoverPiiColumns:

    def _make_metadata(self):
        return {
            "clinical_notes": {"type": "STRING", "tags": {"data_classification": "protected"}},
            "discharge_summary": {"type": "STRING", "tags": {"data_classification": "protected", "pii_type": "free_text"}},
            "patient_ssn": {"type": "STRING", "tags": {"data_classification": "protected", "pii_type": "ssn"}},
            "phone": {"type": "STRING", "tags": {"data_classification": "protected", "pii_type": "phone"}},
            "date_of_birth": {"type": "DATE", "tags": {"data_classification": "protected", "pii_type": "dob"}},
            "age": {"type": "INT", "tags": {"data_classification": "protected"}},
            "encounter_id": {"type": "STRING", "tags": {}},
            "admission_date": {"type": "DATE", "tags": {}},
            "patient_id": {"type": "STRING", "tags": {}},
        }

    @patch("dbxredact.metadata.get_table_metadata")
    def test_classifies_text_columns(self, mock_meta):
        mock_meta.return_value = self._make_metadata()
        result = discover_pii_columns(MagicMock(), "cat.sch.tbl")
        assert "clinical_notes" in result["text_columns"]
        assert "discharge_summary" in result["text_columns"]

    @patch("dbxredact.metadata.get_table_metadata")
    def test_classifies_structured_columns(self, mock_meta):
        mock_meta.return_value = self._make_metadata()
        result = discover_pii_columns(MagicMock(), "cat.sch.tbl")
        assert result["structured_columns"]["patient_ssn"] == "ssn"
        assert result["structured_columns"]["phone"] == "phone"
        assert result["structured_columns"]["date_of_birth"] == "dob"

    @patch("dbxredact.metadata.get_table_metadata")
    def test_warns_on_untyped_non_string(self, mock_meta):
        mock_meta.return_value = self._make_metadata()
        result = discover_pii_columns(MagicMock(), "cat.sch.tbl")
        assert len(result["warnings"]) == 1
        assert "age" in result["warnings"][0]

    @patch("dbxredact.metadata.get_table_metadata")
    def test_finds_doc_id_candidates(self, mock_meta):
        mock_meta.return_value = self._make_metadata()
        result = discover_pii_columns(MagicMock(), "cat.sch.tbl")
        assert "encounter_id" in result["doc_id_candidates"]
        assert "patient_id" in result["doc_id_candidates"]

    @patch("dbxredact.metadata.get_table_metadata")
    def test_untagged_not_in_text_or_struct(self, mock_meta):
        mock_meta.return_value = self._make_metadata()
        result = discover_pii_columns(MagicMock(), "cat.sch.tbl")
        assert "admission_date" not in result["text_columns"]
        assert "admission_date" not in result["structured_columns"]

    @patch("dbxredact.metadata.get_table_metadata")
    def test_empty_table(self, mock_meta):
        mock_meta.return_value = {}
        result = discover_pii_columns(MagicMock(), "cat.sch.tbl")
        assert result["text_columns"] == []
        assert result["structured_columns"] == {}
        assert result["doc_id_candidates"] == []
        assert result["warnings"] == []

    @patch("dbxredact.metadata.get_table_metadata")
    def test_custom_tags(self, mock_meta):
        mock_meta.return_value = {
            "notes": {"type": "STRING", "tags": {"pii_flag": "yes"}},
        }
        result = discover_pii_columns(
            MagicMock(), "cat.sch.tbl",
            classification_tag="pii_flag", classification_value="yes",
        )
        assert "notes" in result["text_columns"]
