"""Tests for pipeline.py -- detector validation and combination checks."""

import pytest
from unittest.mock import patch, MagicMock
from dbxredact.pipeline import run_detection_pipeline


class TestDetectorValidation:
    """All 7 valid combos + the invalid one (all False)."""

    def test_no_detectors_raises(self):
        spark = MagicMock()
        df = MagicMock()
        with pytest.raises(ValueError, match="At least one detection method"):
            run_detection_pipeline(
                spark=spark, source_df=df,
                doc_id_column="id", text_column="text",
                use_presidio=False, use_ai_query=False, use_gliner=False,
            )

    @pytest.mark.parametrize("presidio,ai,gliner", [
        (True, False, False),
        (False, True, False),
        (False, False, True),
        (True, True, False),
        (True, False, True),
        (False, True, True),
        (True, True, True),
    ])
    @patch("dbxredact.pipeline.run_detection")
    @patch("dbxredact.pipeline._apply_alignment")
    def test_valid_combinations_call_detection(
        self, mock_align, mock_detect, presidio, ai, gliner
    ):
        spark = MagicMock()
        df = MagicMock()
        mock_detect.return_value = MagicMock()
        mock_align.return_value = MagicMock()

        run_detection_pipeline(
            spark=spark, source_df=df,
            doc_id_column="id", text_column="text",
            use_presidio=presidio, use_ai_query=ai, use_gliner=gliner,
            endpoint="test-endpoint" if ai else None,
        )
        mock_detect.assert_called_once()
