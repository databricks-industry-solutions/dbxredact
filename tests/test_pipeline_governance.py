"""Pipeline governance tests: safety guards, RedactionConfig passthrough, _apply_config."""

import sys
import pytest
from unittest.mock import MagicMock, patch

_pyspark_mods = [
    "pyspark", "pyspark.sql", "pyspark.sql.functions", "pyspark.sql.types",
    "pyspark.sql.streaming",
]
for _mod in _pyspark_mods:
    if _mod not in sys.modules:
        sys.modules[_mod] = MagicMock()

from dbxredact.pipeline import (
    run_redaction_pipeline,
    _check_validation_output,
    _apply_config,
)
from dbxredact.config import RedactionConfig


# ===================================================================
# Safety guards -- run_redaction_pipeline pre-flight checks
# ===================================================================

class TestInPlaceGuard:
    """In-place redaction must require confirm_destructive=True."""

    def test_in_place_without_confirm_raises(self):
        with pytest.raises(ValueError, match="destructive"):
            run_redaction_pipeline(
                spark=MagicMock(),
                source_table="cat.sch.tbl",
                text_column="text",
                output_mode="in_place",
                confirm_destructive=False,
                use_ai_query=False, use_gliner=False,
            )

    def test_in_place_with_confirm_passes_guard(self):
        """confirm_destructive=True should pass the guard and reach spark.table()."""
        mock_spark = MagicMock()
        mock_spark.table.return_value.select.return_value.distinct.return_value = MagicMock()
        with patch("dbxredact.pipeline.run_detection_pipeline") as mock_det:
            mock_det.return_value = MagicMock()
            mock_det.return_value.cache.return_value = mock_det.return_value
            mock_det.return_value.count.return_value = 1
            mock_det.return_value.columns = ["aligned_entities"]
            try:
                run_redaction_pipeline(
                    spark=mock_spark,
                    source_table="cat.sch.tbl",
                    text_column="text",
                    output_mode="in_place",
                    confirm_destructive=True,
                    use_ai_query=False, use_gliner=False,
                )
            except Exception:
                pass
            mock_spark.table.assert_called_once()


class TestSeparateOutputGuard:
    """output_mode='separate' must require output_table."""

    def test_separate_without_output_table_raises(self):
        with pytest.raises(ValueError, match="output_table is required"):
            run_redaction_pipeline(
                spark=MagicMock(),
                source_table="cat.sch.tbl",
                text_column="text",
                output_mode="separate",
                output_table=None,
                use_ai_query=False, use_gliner=False,
            )


class TestValidationOutputGuard:
    """validation output_strategy requires explicit opt-in."""

    def test_validation_without_confirm_raises(self):
        with pytest.raises(ValueError, match="writes raw PII"):
            run_redaction_pipeline(
                spark=MagicMock(),
                source_table="cat.sch.tbl",
                text_column="text",
                output_table="cat.sch.out",
                output_strategy="validation",
                confirm_validation_output=False,
                use_ai_query=False, use_gliner=False,
            )

    def test_check_validation_output_production_passes(self):
        _check_validation_output("production", confirm_validation_output=False)

    def test_check_validation_output_validation_raises(self):
        with pytest.raises(ValueError, match="writes raw PII"):
            _check_validation_output("validation", confirm_validation_output=False)

    def test_check_validation_output_validation_with_confirm(self):
        _check_validation_output("validation", confirm_validation_output=True)


class TestNoDetectorRaises:
    """At least one detector must be enabled."""

    def test_all_detectors_disabled_raises(self):
        with pytest.raises(ValueError, match="At least one detection method"):
            run_redaction_pipeline(
                spark=MagicMock(),
                source_table="cat.sch.tbl",
                text_column="text",
                output_table="cat.sch.out",
                use_presidio=False,
                use_ai_query=False,
                use_gliner=False,
            )

    def test_all_disabled_via_config_raises(self):
        cfg = RedactionConfig(use_presidio=False, use_ai_query=False, use_gliner=False)
        with pytest.raises(ValueError, match="At least one detection method"):
            run_redaction_pipeline(
                spark=MagicMock(),
                source_table="cat.sch.tbl",
                text_column="text",
                output_table="cat.sch.out",
                config=cfg,
            )


# ===================================================================
# _apply_config passthrough
# ===================================================================

class TestApplyConfig:
    """_apply_config overlays RedactionConfig onto local_vars."""

    def test_none_config_returns_unchanged(self):
        local_vars = {"use_presidio": True, "score_threshold": 0.5, "extra_key": "keep"}
        result = _apply_config(None, local_vars)
        assert result is local_vars
        assert result["extra_key"] == "keep"

    def test_config_overwrites_matching_keys(self):
        cfg = RedactionConfig(use_presidio=False, score_threshold=0.8)
        local_vars = {"use_presidio": True, "score_threshold": 0.5}
        result = _apply_config(cfg, local_vars)
        assert result["use_presidio"] is False
        assert result["score_threshold"] == 0.8

    def test_non_matching_keys_preserved(self):
        cfg = RedactionConfig()
        local_vars = {"use_presidio": True, "my_custom_var": 42}
        result = _apply_config(cfg, local_vars)
        assert result["my_custom_var"] == 42

    def test_all_config_fields_overlay(self):
        """Every RedactionConfig field that exists in local_vars gets overwritten."""
        from dataclasses import fields as dc_fields
        cfg = RedactionConfig(
            use_presidio=False, use_ai_query=False, use_gliner=True,
            score_threshold=0.9, redaction_strategy="typed",
            output_mode="in_place", confirm_destructive=True,
        )
        local_vars = {f.name: f.default for f in dc_fields(RedactionConfig)}
        result = _apply_config(cfg, local_vars)
        assert result["use_presidio"] is False
        assert result["use_gliner"] is True
        assert result["score_threshold"] == 0.9
        assert result["redaction_strategy"] == "typed"
        assert result["output_mode"] == "in_place"
        assert result["confirm_destructive"] is True


# ===================================================================
# Config-driven pipeline guards
# ===================================================================

class TestConfigDrivenGuards:
    """Guards should fire even when values come from RedactionConfig."""

    def test_in_place_via_config_requires_confirm(self):
        cfg = RedactionConfig(output_mode="in_place", confirm_destructive=False)
        with pytest.raises(ValueError, match="destructive"):
            run_redaction_pipeline(
                spark=MagicMock(),
                source_table="cat.sch.tbl",
                text_column="text",
                config=cfg,
            )

    def test_validation_via_config_requires_confirm(self):
        cfg = RedactionConfig(
            output_strategy="validation",
            confirm_validation_output=False,
        )
        with pytest.raises(ValueError, match="writes raw PII"):
            run_redaction_pipeline(
                spark=MagicMock(),
                source_table="cat.sch.tbl",
                text_column="text",
                output_table="cat.sch.out",
                config=cfg,
            )

    def test_consensus_via_config_requires_opt_in(self):
        cfg = RedactionConfig(
            alignment_mode="consensus",
            allow_consensus_redaction=False,
        )
        with pytest.raises(ValueError, match="unsafe for redaction"):
            run_redaction_pipeline(
                spark=MagicMock(),
                source_table="cat.sch.tbl",
                text_column="text",
                output_table="cat.sch.out",
                config=cfg,
            )
