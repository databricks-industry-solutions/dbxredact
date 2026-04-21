"""Unit tests for run_ingestion_redaction_pipeline and helpers."""

import pytest
from unittest.mock import MagicMock, patch

from dbxredact.pipeline import (
    _build_autoloader_options,
    _AUTOLOADER_FORMAT_DEFAULTS,
    run_ingestion_redaction_pipeline,
)


class TestBuildAutoloaderOptions:
    """Tests for _build_autoloader_options helper."""

    def test_json_defaults(self):
        opts = _build_autoloader_options("json", "/Volumes/cat/sch/vol/raw")
        assert opts["cloudFiles.format"] == "json"
        assert opts["multiLine"] == "true"
        assert opts["cloudFiles.schemaLocation"] == "/Volumes/cat/sch/vol/raw/_schema"

    def test_csv_defaults(self):
        opts = _build_autoloader_options("csv", "/Volumes/cat/sch/vol/raw")
        assert opts["header"] == "true"
        assert opts["inferSchema"] == "true"

    def test_parquet_no_extra_defaults(self):
        opts = _build_autoloader_options("parquet", "/path")
        assert "multiLine" not in opts
        assert "header" not in opts

    def test_text_defaults(self):
        opts = _build_autoloader_options("text", "/path")
        assert opts["wholetext"] == "true"

    def test_avro_supported(self):
        opts = _build_autoloader_options("avro", "/path")
        assert opts["cloudFiles.format"] == "avro"

    def test_unsupported_format_raises(self):
        with pytest.raises(ValueError, match="Unsupported format 'xml'"):
            _build_autoloader_options("xml", "/path")

    def test_custom_schema_location(self):
        opts = _build_autoloader_options("json", "/path", schema_location="/custom/_schema")
        assert opts["cloudFiles.schemaLocation"] == "/custom/_schema"

    def test_extra_options_override(self):
        opts = _build_autoloader_options(
            "json", "/path",
            extra_options={"multiLine": "false", "cloudFiles.maxBytesPerTrigger": "10g"},
        )
        assert opts["multiLine"] == "false"
        assert opts["cloudFiles.maxBytesPerTrigger"] == "10g"


class TestIngestionPipelineValidation:
    """Pre-flight validation tests for run_ingestion_redaction_pipeline."""

    def test_missing_source_path_raises(self):
        with pytest.raises(ValueError, match="source_path is required"):
            run_ingestion_redaction_pipeline(
                spark=MagicMock(),
                source_path="",
                file_format="json",
                output_table="cat.sch.out",
                text_columns=["text"],
            )

    def test_missing_output_table_raises(self):
        with pytest.raises(ValueError, match="output_table is required"):
            run_ingestion_redaction_pipeline(
                spark=MagicMock(),
                source_path="/Volumes/cat/sch/vol/raw",
                file_format="json",
                output_table="",
                text_columns=["text"],
            )

    def test_empty_text_columns_raises(self):
        with pytest.raises(ValueError, match="text_columns must contain"):
            run_ingestion_redaction_pipeline(
                spark=MagicMock(),
                source_path="/Volumes/cat/sch/vol/raw",
                file_format="json",
                output_table="cat.sch.out",
                text_columns=[],
            )

    def test_consensus_without_flag_raises(self):
        with pytest.raises(ValueError, match="[Cc]onsensus"):
            run_ingestion_redaction_pipeline(
                spark=MagicMock(),
                source_path="/Volumes/cat/sch/vol/raw",
                file_format="json",
                output_table="cat.sch.out",
                text_columns=["text"],
                alignment_mode="consensus",
                allow_consensus_redaction=False,
            )

    def test_unsupported_format_raises(self):
        with patch("dbxredact.detection.check_presidio_available", return_value=(True, None)):
            with pytest.raises(ValueError, match="Unsupported format"):
                run_ingestion_redaction_pipeline(
                    spark=MagicMock(),
                    source_path="/Volumes/cat/sch/vol/raw",
                    file_format="xml",
                    output_table="cat.sch.out",
                    text_columns=["text"],
                )


class TestIngestionPipelineConfigPassthrough:
    """RedactionConfig should override explicit params via _apply_config."""

    def test_config_overrides_use_presidio(self):
        from dbxredact.pipeline import _apply_config
        from dbxredact.config import RedactionConfig

        config = RedactionConfig(use_presidio=False, use_ai_query=True)
        defaults = {
            "use_presidio": True, "use_ai_query": False, "use_gliner": False,
            "endpoint": None, "score_threshold": 0.5, "gliner_model": "x",
            "gliner_threshold": 0.4, "gliner_max_words": None, "num_cores": 10,
            "fail_on_presidio_error": True, "reasoning_effort": "medium",
            "presidio_model_size": None, "presidio_pattern_only": False,
            "ai_model_type": "foundation", "alignment_mode": "union",
            "allow_consensus_redaction": False, "redaction_strategy": "generic",
            "entity_filter": None,
        }
        result = _apply_config(config, defaults)
        assert result["use_presidio"] is False
        assert result["use_ai_query"] is True


class TestIngestionTriggerModes:
    """Trigger mode logic in _run_streaming_query."""

    def test_available_now_blocks(self):
        from dbxredact.pipeline import _run_streaming_query

        mock_spark = MagicMock()
        mock_spark.conf.get.return_value = "200"
        mock_df = MagicMock()
        mock_query = MagicMock()
        mock_df.writeStream.option.return_value = mock_df.writeStream
        mock_df.writeStream.option.return_value.option.return_value = mock_df.writeStream
        ws = mock_df.writeStream.option.return_value.option.return_value
        ws.foreachBatch.return_value = ws
        ws.trigger.return_value = ws
        ws.start.return_value = mock_query

        _run_streaming_query(
            mock_spark, mock_df, "/ckpt", 4, lambda b, i: None, [],
            trigger_once=True, processing_time=None,
        )
        mock_query.awaitTermination.assert_called_once()

    def test_processing_time_does_not_block(self):
        from dbxredact.pipeline import _run_streaming_query

        mock_spark = MagicMock()
        mock_spark.conf.get.return_value = "200"
        mock_df = MagicMock()
        mock_query = MagicMock()
        mock_df.writeStream.option.return_value = mock_df.writeStream
        mock_df.writeStream.option.return_value.option.return_value = mock_df.writeStream
        ws = mock_df.writeStream.option.return_value.option.return_value
        ws.foreachBatch.return_value = ws
        ws.trigger.return_value = ws
        ws.start.return_value = mock_query

        _run_streaming_query(
            mock_spark, mock_df, "/ckpt", 4, lambda b, i: None, [],
            processing_time="30 seconds",
        )
        mock_query.awaitTermination.assert_not_called()
