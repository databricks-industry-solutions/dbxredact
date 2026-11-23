"""Tests for metadata.py -- identifier validation, table name parsing, and PII discovery."""

from unittest.mock import patch

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


# ---------------------------------------------------------------------------
# TestDiscoverPiiColumns
# ---------------------------------------------------------------------------

class TestDiscoverPiiColumns:
    """Tests for discover_pii_columns using mocked get_table_metadata."""

    TABLE = "cat.sch.tbl"

    def _call(self, metadata_return, spark=None):
        with patch("dbxredact.metadata.get_table_metadata", return_value=metadata_return):
            return discover_pii_columns(spark, self.TABLE)

    def test_text_column_default(self):
        meta = {
            "notes": {
                "type": "string",
                "tags": {"data_classification": "protected"},
            }
        }
        result = self._call(meta)
        assert "notes" in result["text_columns"]
        assert result["structured_columns"] == {}

    def test_structured_column(self):
        meta = {
            "patient_ssn": {
                "type": "string",
                "tags": {"data_classification": "protected", "pii_type": "ssn"},
            }
        }
        result = self._call(meta)
        assert result["structured_columns"] == {"patient_ssn": "ssn"}
        assert result["text_columns"] == []

    def test_free_text_treated_as_text(self):
        meta = {
            "clinical_note": {
                "type": "string",
                "tags": {"data_classification": "protected", "pii_type": "free_text"},
            }
        }
        result = self._call(meta)
        assert "clinical_note" in result["text_columns"]
        assert "clinical_note" not in result["structured_columns"]

    def test_non_string_no_pii_type_warns(self):
        meta = {
            "birth_year": {
                "type": "int",
                "tags": {"data_classification": "protected"},
            }
        }
        result = self._call(meta)
        assert len(result["warnings"]) == 1
        assert "birth_year" in result["warnings"][0]

    def test_no_protected_columns(self):
        meta = {
            "name": {"type": "string", "tags": {}},
            "age": {"type": "int", "tags": {}},
        }
        result = self._call(meta)
        assert result["text_columns"] == []
        assert result["structured_columns"] == {}

    def test_doc_id_candidates(self):
        meta = {
            "patient_id": {"type": "string", "tags": {}},
            "visit_id": {"type": "int", "tags": {}},
            "name": {"type": "string", "tags": {}},
        }
        result = self._call(meta)
        assert "patient_id" in result["doc_id_candidates"]
        assert "visit_id" in result["doc_id_candidates"]
        assert "name" not in result["doc_id_candidates"]

    def test_mixed_columns(self):
        meta = {
            "ssn": {"type": "string", "tags": {"data_classification": "protected", "pii_type": "ssn"}},
            "notes": {"type": "string", "tags": {"data_classification": "protected"}},
            "age": {"type": "int", "tags": {"data_classification": "protected"}},
            "record_id": {"type": "string", "tags": {}},
            "status": {"type": "string", "tags": {}},
        }
        result = self._call(meta)
        assert result["structured_columns"] == {"ssn": "ssn"}
        assert "notes" in result["text_columns"]
        assert len(result["warnings"]) == 1
        assert "record_id" in result["doc_id_candidates"]
