"""Tests for calibration.py -- CalibratedScorer."""

import pytest

sklearn = pytest.importorskip("sklearn")

import json
import tempfile
import os
from dbxredact.calibration import CalibratedScorer


class TestCalibratedScorer:
    """Intent: CalibratedScorer maps raw detector confidence scores to
    calibrated probabilities that better reflect true accuracy, using
    isotonic regression per detection source."""

    def test_calibrated_scores_are_bounded_0_1(self):
        """Output probabilities should always be in [0, 1]."""
        scorer = CalibratedScorer()
        scorer.fit("presidio", [0.1, 0.3, 0.5, 0.7, 0.9], [0, 0, 1, 1, 1])
        calibrated = scorer.transform("presidio", [0.0, 0.2, 0.5, 0.8, 1.0])
        assert all(0.0 <= s <= 1.0 for s in calibrated)

    def test_unfitted_source_returns_raw_scores(self):
        """If a source was never fitted, transform should return scores unchanged."""
        scorer = CalibratedScorer()
        scorer.fit("presidio", [0.1, 0.5, 0.9], [0, 1, 1])
        raw = [0.3, 0.6, 0.9]
        assert scorer.transform("gliner", raw) == raw

    def test_transform_single_unfitted_returns_raw(self):
        scorer = CalibratedScorer()
        assert scorer.transform_single("ai", 0.42) == 0.42

    def test_sources_tracks_fitted_sources(self):
        scorer = CalibratedScorer()
        assert scorer.sources == []
        scorer.fit("presidio", [0.1, 0.9], [0, 1])
        scorer.fit("gliner", [0.2, 0.8], [0, 1])
        assert set(scorer.sources) == {"presidio", "gliner"}

    def test_fit_returns_self_for_chaining(self):
        scorer = CalibratedScorer()
        result = scorer.fit("presidio", [0.1, 0.9], [0, 1])
        assert result is scorer

    def test_save_and_load_roundtrip(self):
        """A saved scorer should produce the same calibrated scores after loading."""
        scorer = CalibratedScorer()
        scorer.fit("presidio", [0.1, 0.3, 0.5, 0.7, 0.9], [0, 0, 1, 1, 1])
        test_scores = [0.2, 0.5, 0.8]
        original = scorer.transform("presidio", test_scores)

        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
            path = f.name
        try:
            scorer.save(path)
            loaded = CalibratedScorer.load(path)
            restored = loaded.transform("presidio", test_scores)
            assert len(original) == len(restored)
            for a, b in zip(original, restored):
                assert abs(a - b) < 1e-6
        finally:
            os.unlink(path)

    def test_save_produces_valid_json(self):
        scorer = CalibratedScorer()
        scorer.fit("ai", [0.1, 0.5, 0.9], [0, 1, 1])
        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
            path = f.name
        try:
            scorer.save(path)
            with open(path) as f:
                data = json.load(f)
            assert "ai" in data
            assert "X_thresholds_" in data["ai"]
            assert "y_thresholds_" in data["ai"]
        finally:
            os.unlink(path)

    def test_monotonic_calibration(self):
        """Isotonic regression should produce monotonically non-decreasing output
        when given monotonically increasing input scores."""
        scorer = CalibratedScorer()
        scorer.fit(
            "presidio",
            [0.05, 0.15, 0.25, 0.35, 0.45, 0.55, 0.65, 0.75, 0.85, 0.95],
            [0, 0, 0, 0, 1, 0, 1, 1, 1, 1],
        )
        inputs = [i / 20.0 for i in range(21)]
        calibrated = scorer.transform("presidio", inputs)
        for i in range(1, len(calibrated)):
            assert calibrated[i] >= calibrated[i - 1] - 1e-9
