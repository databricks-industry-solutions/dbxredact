"""Confidence score calibration using isotonic regression."""

import json
from typing import List

import numpy as np


def _get_isotonic_regression():
    from sklearn.isotonic import IsotonicRegression
    return IsotonicRegression


class CalibratedScorer:
    """Calibrate raw detection confidence scores to better-reflect true accuracy.

    Fits one isotonic regression per detection source (presidio / gliner / ai).
    """

    def __init__(self):
        self._models: dict = {}

    def fit(self, source: str, scores: List[float], labels: List[int]) -> "CalibratedScorer":
        """Fit calibration for a single source.

        Args:
            source: Detection source name ("presidio", "gliner", "ai")
            scores: Raw confidence scores
            labels: Binary ground truth (1 = true entity, 0 = not)
        """
        ir = _get_isotonic_regression()(y_min=0.0, y_max=1.0, out_of_bounds="clip")
        ir.fit(np.array(scores), np.array(labels))
        self._models[source] = ir
        return self

    def transform(self, source: str, scores: List[float]) -> List[float]:
        """Calibrate scores for a source. Returns raw scores if source not fitted."""
        if source not in self._models:
            return scores
        return self._models[source].predict(np.array(scores)).tolist()

    def transform_single(self, source: str, score: float) -> float:
        if source not in self._models:
            return score
        return float(self._models[source].predict(np.array([score]))[0])

    @property
    def sources(self) -> list:
        return list(self._models.keys())

    def save(self, path: str) -> None:
        """Save calibration parameters to JSON."""
        data = {}
        for source, ir in self._models.items():
            data[source] = {
                "X_thresholds_": ir.X_thresholds_.tolist(),
                "y_thresholds_": ir.y_thresholds_.tolist(),
            }
        with open(path, "w") as f:
            json.dump(data, f)

    @classmethod
    def load(cls, path: str) -> "CalibratedScorer":
        """Load calibration from JSON."""
        with open(path) as f:
            data = json.load(f)
        scorer = cls()
        for source, params in data.items():
            ir = _get_isotonic_regression()(y_min=0.0, y_max=1.0, out_of_bounds="clip")
            ir.X_thresholds_ = np.array(params["X_thresholds_"])
            ir.y_thresholds_ = np.array(params["y_thresholds_"])
            ir.X_min_ = ir.X_thresholds_[0]
            ir.X_max_ = ir.X_thresholds_[-1]
            ir.f_ = None  # will rebuild on next predict
            scorer._models[source] = ir
        return scorer
