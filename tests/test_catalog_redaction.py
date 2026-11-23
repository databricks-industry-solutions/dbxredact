"""Tests for SENSITIVITY_PROFILES and run_catalog_redaction (Step 3)."""

from unittest.mock import MagicMock, patch

import pytest

from dbxredact.config import (
    MIN_SCORE_THRESHOLD,
    RedactionConfig,
    SENSITIVITY_PROFILES,
)
from dbxredact.pipeline import run_catalog_redaction


class TestSensitivityProfiles:
    def test_has_phi_pci_pii(self):
        assert set(SENSITIVITY_PROFILES) == {"phi", "pci", "pii"}

    def test_all_are_redaction_configs(self):
        assert all(
            isinstance(c, RedactionConfig) for c in SENSITIVITY_PROFILES.values()
        )

    def test_all_use_union_alignment(self):
        # Redaction profiles must never use consensus (recall-safe by design).
        assert all(c.alignment_mode == "union" for c in SENSITIVITY_PROFILES.values())
        assert all(
            c.allow_consensus_redaction is False for c in SENSITIVITY_PROFILES.values()
        )

    def test_thresholds_above_governance_floor(self):
        assert all(
            c.score_threshold > MIN_SCORE_THRESHOLD
            for c in SENSITIVITY_PROFILES.values()
        )

    def test_phi_is_most_aggressive(self):
        phi = SENSITIVITY_PROFILES["phi"]
        pii = SENSITIVITY_PROFILES["pii"]
        # PHI recall-biased: lower threshold than PII, and GLiNER enabled.
        assert phi.score_threshold < pii.score_threshold
        assert phi.use_gliner is True

    def test_pci_uses_typed_redaction(self):
        assert SENSITIVITY_PROFILES["pci"].redaction_strategy == "typed"


class TestRunCatalogRedaction:
    def _run(self, tables, levels, **kwargs):
        """Drive run_catalog_redaction with the sensitivity readers + the
        per-table redactor mocked. Returns (results, mock_run_table_redaction)."""
        spark = MagicMock()
        defaults = dict(
            output_catalog="out",
            output_schema="red",
        )
        defaults.update(kwargs)
        with (
            patch(
                "dbxredact.sensitivity.get_tables_by_sensitivity",
                return_value=tables,
            ),
            patch(
                "dbxredact.sensitivity.get_table_sensitivity_level",
                side_effect=lambda _s, t: levels[t],
            ),
            patch("dbxredact.pipeline.run_table_redaction") as mock_rtr,
        ):
            results = run_catalog_redaction(spark, "cat", **defaults)
        return results, mock_rtr

    def test_requires_output_destination(self):
        with pytest.raises(ValueError, match="output_catalog and output_schema"):
            run_catalog_redaction(MagicMock(), "cat")

    def test_selects_profile_per_level(self):
        tables = ["cat.s.a", "cat.s.b"]
        levels = {"cat.s.a": "phi", "cat.s.b": "pii"}
        results, mock_rtr = self._run(tables, levels)

        assert mock_rtr.call_count == 2
        # Each call got the profile matching that table's level.
        by_source = {
            c.kwargs["source_table"]: c.kwargs["config"]
            for c in mock_rtr.call_args_list
        }
        assert by_source["cat.s.a"] is SENSITIVITY_PROFILES["phi"]
        assert by_source["cat.s.b"] is SENSITIVITY_PROFILES["pii"]
        assert all(r["status"] == "ok" for r in results.values())

    def test_output_table_naming(self):
        tables = ["cat.s.patients"]
        levels = {"cat.s.patients": "phi"}
        results, mock_rtr = self._run(tables, levels, output_suffix="_clean")
        assert mock_rtr.call_args.kwargs["output_table"] == "out.red.patients_clean"
        assert results["cat.s.patients"]["output_table"] == "out.red.patients_clean"

    def test_unknown_level_is_skipped_not_run(self):
        tables = ["cat.s.a", "cat.s.b"]
        levels = {"cat.s.a": "phi", "cat.s.b": "weird"}
        results, mock_rtr = self._run(tables, levels)
        # Only the phi table is redacted.
        assert mock_rtr.call_count == 1
        assert results["cat.s.b"]["status"] == "error"
        assert results["cat.s.a"]["status"] == "ok"

    def test_continue_on_error_isolates_failure(self):
        tables = ["cat.s.a", "cat.s.b"]
        levels = {"cat.s.a": "phi", "cat.s.b": "pii"}
        spark = MagicMock()
        with (
            patch(
                "dbxredact.sensitivity.get_tables_by_sensitivity",
                return_value=tables,
            ),
            patch(
                "dbxredact.sensitivity.get_table_sensitivity_level",
                side_effect=lambda _s, t: levels[t],
            ),
            patch(
                "dbxredact.pipeline.run_table_redaction",
                side_effect=[RuntimeError("boom"), MagicMock()],
            ),
        ):
            results = run_catalog_redaction(
                spark, "cat", output_catalog="out", output_schema="red"
            )
        # First failed, second still ran.
        statuses = {t: r["status"] for t, r in results.items()}
        assert statuses == {"cat.s.a": "error", "cat.s.b": "ok"}
        assert "boom" in results["cat.s.a"]["error"]

    def test_fail_fast_raises_on_first_error(self):
        tables = ["cat.s.a"]
        levels = {"cat.s.a": "phi"}
        spark = MagicMock()
        with (
            patch(
                "dbxredact.sensitivity.get_tables_by_sensitivity",
                return_value=tables,
            ),
            patch(
                "dbxredact.sensitivity.get_table_sensitivity_level",
                side_effect=lambda _s, t: levels[t],
            ),
            patch(
                "dbxredact.pipeline.run_table_redaction",
                side_effect=RuntimeError("boom"),
            ),
            pytest.raises(RuntimeError, match="boom"),
        ):
            run_catalog_redaction(
                spark,
                "cat",
                output_catalog="out",
                output_schema="red",
                continue_on_error=False,
            )
