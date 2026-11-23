"""Unit tests for sensitivity.py -- table-level PII/PHI classification."""

from unittest.mock import MagicMock, patch

import pytest

from dbxredact.config import RedactionConfig
from dbxredact.sensitivity import (
    ColumnSensitivity,
    TableSensitivity,
    _ENTITY_TO_PII_TYPE,
    _build_payload,
    _rollup_column,
    _rollup_table,
    apply_sensitivity_tags,
    build_sensitivity_tag_statements,
    classify_table,
    get_table_sensitivity_level,
    get_tables_by_sensitivity,
)


def _sample_result():
    return TableSensitivity(
        table_name="cat.sch.tbl",
        data_sensitivity="phi",
        columns=[
            ColumnSensitivity(
                "mrn", "string", "medical_record_number", ["MEDICAL_RECORD_NUMBER"]
            ),
            ColumnSensitivity("notes", "string", "free_text", ["PERSON"]),
            ColumnSensitivity("age", "bigint", "none", []),
        ],
        sampled_rows=200,
    )


class TestBuildPayload:
    def test_dedupes_and_joins(self):
        payload, truncated = _build_payload(["a", "a", "b", None, "  ", "b"])
        assert payload == "a\nb"
        assert truncated is False

    def test_all_null_or_blank_returns_empty(self):
        payload, truncated = _build_payload([None, "", "   "])
        assert payload == ""
        assert truncated is False

    def test_count_cap_binds(self):
        payload, truncated = _build_payload(
            [f"v{i}" for i in range(10)], max_values_per_column=3
        )
        assert truncated is True
        assert payload.startswith("v0\nv1\nv2")
        assert "(truncated, 3 of 10 values)" in payload

    def test_per_value_char_cap_binds(self):
        payload, truncated = _build_payload(["x" * 500], max_chars_per_value=10)
        assert truncated is True
        # value clipped to 10 chars
        assert "xxxxxxxxxx" in payload
        assert "x" * 11 not in payload

    def test_payload_budget_cap_binds(self):
        # Each value ~5 chars; budget stops accumulation early.
        payload, truncated = _build_payload(
            [f"val{i:02d}" for i in range(100)],
            max_chars_per_value=50,
            max_values_per_column=100,
            max_payload_chars=20,
        )
        assert truncated is True
        # Only a handful of values fit under the 20-char budget.
        assert "(truncated," in payload

    def test_no_truncation_when_within_all_caps(self):
        payload, truncated = _build_payload(["a", "b", "c"])
        assert truncated is False
        assert "truncated" not in payload


class TestEntityToPiiType:
    @pytest.mark.parametrize(
        "entity_type,expected",
        [
            ("US_SSN", "ssn"),
            ("EMAIL_ADDRESS", "email"),
            ("PHONE_NUMBER", "phone"),
            ("CREDIT_CARD", "credit_card"),
            ("MEDICAL_RECORD_NUMBER", "medical_record_number"),
            ("PERSON", "free_text"),
            ("LOCATION", "free_text"),
        ],
    )
    def test_representative_mappings(self, entity_type, expected):
        assert _ENTITY_TO_PII_TYPE[entity_type] == expected


class TestRollupColumn:
    def test_structured_type_wins_over_free_text(self):
        pt = _rollup_column(["PERSON", "US_SSN", "US_SSN"], "string", 0.5)
        assert pt == "ssn"

    def test_most_frequent_structured_type_wins(self):
        pt = _rollup_column(["EMAIL_ADDRESS", "US_SSN", "US_SSN"], "string", 0.5)
        assert pt == "ssn"

    def test_free_text_when_only_contextual(self):
        pt = _rollup_column(["PERSON", "LOCATION"], "string", 0.5)
        assert pt == "free_text"

    def test_none_when_no_entities(self):
        assert _rollup_column([], "string", 0.0) == "none"

    def test_ignored_types_do_not_count(self):
        assert _rollup_column(["NRP", "MEDICAL_INFO"], "string", 0.5) == "none"


class TestRollupTable:
    def _col(self, entity_types, pii_type="free_text"):
        return ColumnSensitivity(
            column_name="c",
            data_type="string",
            pii_type=pii_type,
            entity_types=entity_types,
        )

    def test_phi_wins(self):
        cols = [
            self._col(["CREDIT_CARD"], "credit_card"),
            self._col(["MEDICAL_RECORD_NUMBER"], "medical_record_number"),
            self._col(["EMAIL_ADDRESS"], "email"),
        ]
        assert _rollup_table(cols) == "phi"

    def test_pci_wins_over_pii(self):
        cols = [
            self._col(["CREDIT_CARD"], "credit_card"),
            self._col(["EMAIL_ADDRESS"], "email"),
        ]
        assert _rollup_table(cols) == "pci"

    def test_pii_when_only_generic_identifiers(self):
        cols = [self._col(["EMAIL_ADDRESS"], "email")]
        assert _rollup_table(cols) == "pii"

    def test_none_when_no_pii(self):
        cols = [self._col([], "none")]
        assert _rollup_table(cols) == "none"


class TestClassifyTableNoStringColumns:
    def test_returns_none_when_no_string_columns(self, spark):
        meta = {"id": {"type": "bigint", "tags": {}}}
        with patch("dbxredact.sensitivity.get_table_metadata", return_value=meta):
            result = classify_table(spark, "cat.sch.tbl")
        assert isinstance(result, TableSensitivity)
        assert result.data_sensitivity == "none"
        assert result.columns == []
        assert result.warnings


class TestClassifyTableEndToEnd:
    """Pattern-only Presidio, AI Query off -- no spaCy, no network."""

    def test_pivot_and_classify(self, spark, monkeypatch):
        # Email is detected deterministically by pattern-only Presidio (no spaCy,
        # no context needed), so it exercises the full pivot->detect->rollup path.
        df = spark.createDataFrame(
            [
                ("john@test.com", "hello world"),
                ("jane@test.com", "good morning"),
            ],
            ["email_col", "notes_col"],
        )
        # classify_table reads the table by name; patch spark.table + metadata.
        meta = {
            "email_col": {"type": "string", "tags": {}},
            "notes_col": {"type": "string", "tags": {}},
        }
        monkeypatch.setattr(spark, "table", lambda name: df)
        with patch("dbxredact.sensitivity.get_table_metadata", return_value=meta):
            result = classify_table(
                spark,
                "cat.sch.tbl",
                config=RedactionConfig(
                    use_presidio=True,
                    use_ai_query=False,
                    presidio_pattern_only=True,
                ),
            )

        # Pivot produced one ColumnSensitivity per string column.
        assert {c.column_name for c in result.columns} == {
            "email_col",
            "notes_col",
        }
        by_name = {c.column_name: c for c in result.columns}
        assert by_name["email_col"].pii_type == "email"
        assert "EMAIL_ADDRESS" in by_name["email_col"].entity_types
        # A generic direct identifier (email) makes the table PII.
        assert result.data_sensitivity == "pii"
        assert result.sampled_rows == 2


class TestClassifyTableDefaultConfig:
    """The no-config path must build a robust, dependency-light default."""

    def test_default_config_is_pattern_only_and_fault_tolerant(
        self, spark, monkeypatch
    ):
        df = spark.createDataFrame(
            [("john@test.com", "hello")], ["email_col", "notes_col"]
        )
        meta = {
            "email_col": {"type": "string", "tags": {}},
            "notes_col": {"type": "string", "tags": {}},
        }
        monkeypatch.setattr(spark, "table", lambda name: df)

        captured = {}

        def _fake_run_detection(spark_arg, payload_df, **kwargs):
            captured["config"] = kwargs.get("config")
            # Return the payload frame unchanged (no detector struct columns) so
            # _aggregate_entity_types finds nothing -- we only care about config.
            return payload_df

        with (
            patch("dbxredact.sensitivity.get_table_metadata", return_value=meta),
            patch(
                "dbxredact.sensitivity.run_detection", side_effect=_fake_run_detection
            ),
        ):
            classify_table(spark, "cat.sch.tbl")  # no config -> build default

        cfg = captured["config"]
        assert isinstance(cfg, RedactionConfig)
        assert cfg.use_presidio is True
        assert cfg.use_ai_query is True
        assert cfg.presidio_pattern_only is True
        assert cfg.fail_on_presidio_error is False

    def test_caller_config_is_respected_verbatim(self, spark, monkeypatch):
        df = spark.createDataFrame([("x", "y")], ["a", "b"])
        meta = {
            "a": {"type": "string", "tags": {}},
            "b": {"type": "string", "tags": {}},
        }
        monkeypatch.setattr(spark, "table", lambda name: df)

        caller_cfg = RedactionConfig(
            use_presidio=False, use_ai_query=True, presidio_pattern_only=False
        )
        captured = {}

        def _fake_run_detection(spark_arg, payload_df, **kwargs):
            captured["config"] = kwargs.get("config")
            return payload_df

        with (
            patch("dbxredact.sensitivity.get_table_metadata", return_value=meta),
            patch(
                "dbxredact.sensitivity.run_detection", side_effect=_fake_run_detection
            ),
        ):
            classify_table(spark, "cat.sch.tbl", config=caller_cfg)

        # Same object, untouched.
        assert captured["config"] is caller_cfg


class TestBuildSensitivityTagStatements:
    def test_table_and_pii_column_statements(self):
        stmts = build_sensitivity_tag_statements(_sample_result())
        # One table-level + one per PII column (mrn, notes) -- not the "none" col.
        assert len(stmts) == 3
        assert stmts[0] == (
            "ALTER TABLE cat.sch.tbl SET TAGS ('data_sensitivity' = 'phi')"
        )
        assert any(
            "ALTER COLUMN mrn SET TAGS" in s
            and "'data_classification' = 'protected'" in s
            and "'pii_type' = 'medical_record_number'" in s
            for s in stmts
        )
        # The non-PII column gets no statement.
        assert not any("ALTER COLUMN age" in s for s in stmts)

    def test_rejects_injection_in_column_name(self):
        bad = TableSensitivity(
            table_name="cat.sch.tbl",
            data_sensitivity="pii",
            columns=[
                ColumnSensitivity(
                    "col; DROP TABLE x", "string", "email", ["EMAIL_ADDRESS"]
                )
            ],
        )
        with pytest.raises(ValueError):
            build_sensitivity_tag_statements(bad)

    def test_rejects_injection_in_table_name(self):
        bad = TableSensitivity(
            table_name="cat.sch.tbl; DROP",
            data_sensitivity="pii",
            columns=[],
        )
        with pytest.raises(ValueError):
            build_sensitivity_tag_statements(bad)


class TestApplySensitivityTagsGate:
    def test_dry_run_returns_statements_and_executes_nothing(self):
        spark = MagicMock()
        stmts = apply_sensitivity_tags(spark, _sample_result())  # dry_run default
        assert len(stmts) == 3
        spark.sql.assert_not_called()

    def test_real_write_without_confirm_raises(self):
        spark = MagicMock()
        with pytest.raises(ValueError, match="confirm=True"):
            apply_sensitivity_tags(spark, _sample_result(), dry_run=False)
        spark.sql.assert_not_called()

    def test_real_write_with_confirm_executes_all(self):
        spark = MagicMock()
        stmts = apply_sensitivity_tags(
            spark, _sample_result(), dry_run=False, confirm=True
        )
        assert spark.sql.call_count == len(stmts) == 3
        executed = [c.args[0] for c in spark.sql.call_args_list]
        assert executed == stmts


class TestGetTablesBySensitivity:
    def _fetch(self, rows, catalog="cat", schema=None, min_level="pii"):
        spark = MagicMock()
        result_df = MagicMock()
        result_df.collect.return_value = [
            {"catalog_name": c, "schema_name": s, "table_name": t} for (c, s, t) in rows
        ]
        spark.sql.return_value = result_df
        out = get_tables_by_sensitivity(
            spark, catalog, schema=schema, min_level=min_level
        )
        query = spark.sql.call_args.args[0]
        return out, query

    def test_returns_sorted_fqns(self):
        out, _ = self._fetch([("cat", "s", "t2"), ("cat", "s", "t1")])
        assert out == ["cat.s.t1", "cat.s.t2"]

    def test_min_level_pii_includes_all_sensitive(self):
        _, query = self._fetch([], min_level="pii")
        assert "'pii'" in query and "'pci'" in query and "'phi'" in query
        assert "'none'" not in query

    def test_min_level_phi_only(self):
        _, query = self._fetch([], min_level="phi")
        assert "'phi'" in query
        assert "'pii'" not in query and "'pci'" not in query

    def test_uses_table_tags_view_and_correct_columns(self):
        _, query = self._fetch([])
        assert "system.information_schema.table_tags" in query
        assert "catalog_name" in query and "schema_name" in query
        assert "tag_name = 'data_sensitivity'" in query

    def test_schema_filter_applied(self):
        _, query = self._fetch([], schema="sch")
        assert "schema_name = 'sch'" in query

    def test_invalid_min_level_raises(self):
        with pytest.raises(ValueError, match="Invalid min_level"):
            get_tables_by_sensitivity(MagicMock(), "cat", min_level="bogus")

    def test_rejects_injection_in_catalog(self):
        with pytest.raises(ValueError):
            get_tables_by_sensitivity(MagicMock(), "cat; DROP")


class TestGetTableSensitivityLevel:
    def test_returns_tag_value(self):
        spark = MagicMock()
        result_df = MagicMock()
        result_df.collect.return_value = [{"tag_value": "phi"}]
        spark.sql.return_value = result_df
        assert get_table_sensitivity_level(spark, "cat.sch.tbl") == "phi"
        query = spark.sql.call_args.args[0]
        assert "system.information_schema.table_tags" in query
        assert "tag_name = 'data_sensitivity'" in query

    def test_returns_none_when_untagged(self):
        spark = MagicMock()
        result_df = MagicMock()
        result_df.collect.return_value = []
        spark.sql.return_value = result_df
        assert get_table_sensitivity_level(spark, "cat.sch.tbl") is None

    def test_rejects_injection(self):
        with pytest.raises(ValueError):
            get_table_sensitivity_level(MagicMock(), "cat.sch.tbl; DROP")
