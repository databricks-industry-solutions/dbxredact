"""Tests for OutputMode (in-place redaction) logic in pipeline.py.

Imports are done from submodules directly to avoid triggering the full
dbxredact.__init__ which requires pyspark/pandas at import time.
"""

import sys
import pytest
from unittest.mock import MagicMock

# Stub pyspark so pipeline.py can be imported without a Spark runtime.
_pyspark_mods = [
    "pyspark", "pyspark.sql", "pyspark.sql.functions", "pyspark.sql.types",
    "pyspark.sql.streaming",
]
_stashed = {}
for _mod in _pyspark_mods:
    if _mod not in sys.modules:
        _stashed[_mod] = None
        sys.modules[_mod] = MagicMock()

from dbxredact.pipeline import _write_in_place, _check_consensus_safety


class TestWriteInPlace:

    def test_validates_source_table(self):
        with pytest.raises(ValueError, match="must be.*catalog\\.schema\\.table"):
            _write_in_place(
                spark=MagicMock(),
                result_df=MagicMock(),
                source_table="invalid",
                doc_id_column="doc_id",
                text_column="text",
            )

    def test_validates_doc_id_column(self):
        with pytest.raises(ValueError, match="Invalid doc_id_column"):
            _write_in_place(
                spark=MagicMock(),
                result_df=MagicMock(),
                source_table="cat.sch.tbl",
                doc_id_column="bad;col",
                text_column="text",
            )

    def test_validates_text_column(self):
        with pytest.raises(ValueError, match="Invalid text_column"):
            _write_in_place(
                spark=MagicMock(),
                result_df=MagicMock(),
                source_table="cat.sch.tbl",
                doc_id_column="doc_id",
                text_column="col; DROP",
            )

    def test_executes_merge_sql(self):
        mock_spark = MagicMock()
        mock_df = MagicMock()
        mock_select = MagicMock()
        mock_df.select.return_value = mock_select

        _write_in_place(
            spark=mock_spark,
            result_df=mock_df,
            source_table="my_cat.my_sch.my_tbl",
            doc_id_column="doc_id",
            text_column="notes",
        )

        mock_select.createOrReplaceTempView.assert_called_once()
        view_name = mock_select.createOrReplaceTempView.call_args[0][0]
        assert view_name.startswith("_dbxredact_inplace_")

        sql_call = mock_spark.sql.call_args[0][0]
        assert "MERGE INTO my_cat.my_sch.my_tbl" in sql_call
        assert "t.`notes` = s.`notes_redacted`" in sql_call
        assert "t.`doc_id` = s.`doc_id`" in sql_call


class TestConsensusGuard:

    def test_union_mode_passes(self):
        _check_consensus_safety("union", allow_consensus_redaction=False)

    def test_consensus_without_opt_in_raises(self):
        with pytest.raises(ValueError, match="unsafe for redaction"):
            _check_consensus_safety("consensus", allow_consensus_redaction=False)

    def test_consensus_with_opt_in_passes(self):
        _check_consensus_safety("consensus", allow_consensus_redaction=True)
