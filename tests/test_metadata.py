"""Tests for metadata.py -- identifier validation and table name parsing."""

import pytest
from dbxredact.metadata import _validate_identifier, _parse_table_name


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
