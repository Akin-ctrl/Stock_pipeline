"""Bad-trade risk modeling for advisory validation experiments."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Mapping, Optional, Protocol, Sequence

import numpy as np

from app.services.modeling.dataset_builder import ModelingDatasetRow
from app.services.modeling.feature_engineering import safe_float
from app.services.modeling.feature_extractor import (
    PROBABILITY_FEATURE_NAMES,
    extract_probability_features_from_row,
    extract_probability_features_from_snapshot,
    feature_vector_from_mapping,
)


BAD_TRADE_EXTRA_FEATURE_NAMES = (
    "heuristic_proxy_score",
    "signal_agreement_proxy",
    "score_x_agreement",
    "near_20d_high_flag",
    "extended_20d_runup_flag",
    "high_rebound_20d_flag",
)
BAD_TRADE_FEATURE_NAMES = PROBABILITY_FEATURE_NAMES + BAD_TRADE_EXTRA_FEATURE_NAMES


class BadTradeRiskEstimator(Protocol):
    """Interface for estimating severe-loss risk on recommendation candidates."""

    def estimate_bad_trade_probability(
        self,
        feature_snapshot: Mapping[str, object],
    ) -> Optional[float]:
        """Return the estimated probability that this candidate becomes a bad trade."""


class NullBadTradeRiskEstimator:
    """No-op risk estimator used when bad-trade blocking is not under test."""

    def estimate_bad_trade_probability(
        self,
        feature_snapshot: Mapping[str, object],
    ) -> Optional[float]:
        """Return no estimate while preserving the shared estimator interface."""
        return None


@dataclass(frozen=True)
class BadTradeRiskModelConfig:
    """Training and candidate-filter settings for the bad-trade risk model."""

    bad_loss_threshold_pct: float = -5.0
    min_training_rows: int = 500
    min_bad_count: int = 40
    iterations: int = 300
    learning_rate: float = 0.08
    candidate_min_price: float = 8.0
    candidate_min_score: float = 70.0
    candidate_min_signal_agreement: float = 0.60


@dataclass(frozen=True)
class BadTradeTrainingResult:
    """Result of fitting a bad-trade risk estimator."""

    estimator: Optional["FittedBadTradeRiskEstimator"]
    training_rows: int
    training_bad_count: int
    training_good_count: int
    skipped_reason: Optional[str] = None

    @property
    def trained(self) -> bool:
        """Return True when an estimator was trained successfully."""
        return self.estimator is not None


class FittedBadTradeRiskEstimator:
    """Estimate severe-loss risk with a fitted boosted-stump classifier."""

    def __init__(
        self,
        *,
        model: "GradientBoostedStumpRiskModel",
        config: BadTradeRiskModelConfig,
        trained_as_of_date: Optional[date] = None,
    ):
        self.model = model
        self.config = config
        self.trained_as_of_date = trained_as_of_date

    @classmethod
    def fit_from_rows(
        cls,
        rows: Sequence[ModelingDatasetRow],
        *,
        config: BadTradeRiskModelConfig,
        trained_as_of_date: Optional[date] = None,
    ) -> BadTradeTrainingResult:
        """Fit from historical candidate-like rows without using future data."""
        candidate_rows = filter_bad_trade_candidate_rows(
            rows,
            min_price=config.candidate_min_price,
            min_score=config.candidate_min_score,
            min_signal_agreement=config.candidate_min_signal_agreement,
        )
        labels = np.array(
            [
                int(row.forward_return_10d <= config.bad_loss_threshold_pct)
                for row in candidate_rows
            ],
            dtype=float,
        )
        bad_count = int(labels.sum())
        good_count = int(len(labels) - bad_count)

        if len(candidate_rows) < config.min_training_rows:
            return BadTradeTrainingResult(
                estimator=None,
                training_rows=len(candidate_rows),
                training_bad_count=bad_count,
                training_good_count=good_count,
                skipped_reason="insufficient_training_rows",
            )
        if min(bad_count, good_count) < config.min_bad_count:
            return BadTradeTrainingResult(
                estimator=None,
                training_rows=len(candidate_rows),
                training_bad_count=bad_count,
                training_good_count=good_count,
                skipped_reason="insufficient_class_balance",
            )

        feature_matrix = np.array(
            [build_bad_trade_feature_vector_from_row(row) for row in candidate_rows],
            dtype=float,
        )
        model = GradientBoostedStumpRiskModel.fit(
            feature_matrix,
            labels,
            iterations=config.iterations,
            learning_rate=config.learning_rate,
        )
        return BadTradeTrainingResult(
            estimator=cls(
                model=model,
                config=config,
                trained_as_of_date=trained_as_of_date,
            ),
            training_rows=len(candidate_rows),
            training_bad_count=bad_count,
            training_good_count=good_count,
        )

    def estimate_bad_trade_probability(
        self,
        feature_snapshot: Mapping[str, object],
    ) -> Optional[float]:
        """Estimate bad-trade probability from a live recommendation snapshot."""
        feature_vector = np.array(
            build_bad_trade_feature_vector_from_snapshot(feature_snapshot),
            dtype=float,
        )
        return self.model.predict_probability(feature_vector)


class GradientBoostedStumpRiskModel:
    """Dependency-free logistic gradient boosting with decision stumps."""

    def __init__(
        self,
        *,
        base_logit: float,
        learning_rate: float,
        stumps: list[tuple[int, float, float, float]],
    ):
        self.base_logit = base_logit
        self.learning_rate = learning_rate
        self.stumps = stumps

    @classmethod
    def fit(
        cls,
        feature_matrix: np.ndarray,
        labels: np.ndarray,
        *,
        iterations: int,
        learning_rate: float,
    ) -> "GradientBoostedStumpRiskModel":
        """Fit boosted decision stumps against weighted logistic residuals."""
        positive_rate = float(labels.mean())
        clipped_rate = min(max(positive_rate, 1e-4), 1 - 1e-4)
        base_logit = float(np.log(clipped_rate / (1.0 - clipped_rate)))
        logits = np.full(len(labels), base_logit, dtype=float)
        positive_count = float(labels.sum())
        negative_count = float(len(labels) - positive_count)
        positive_weight = (negative_count / positive_count) if positive_count else 1.0
        sample_weight = np.where(labels == 1.0, positive_weight, 1.0)
        stumps: list[tuple[int, float, float, float]] = []

        for _ in range(iterations):
            probabilities = _sigmoid(logits)
            residuals = labels - probabilities
            stump = cls._fit_stump(feature_matrix, residuals, sample_weight)
            if stump is None:
                break
            feature_index, threshold, left_value, right_value = stump
            updates = np.where(
                feature_matrix[:, feature_index] <= threshold,
                left_value,
                right_value,
            )
            logits += learning_rate * updates
            stumps.append(stump)

        return cls(
            base_logit=base_logit,
            learning_rate=learning_rate,
            stumps=stumps,
        )

    @staticmethod
    def _fit_stump(
        feature_matrix: np.ndarray,
        residuals: np.ndarray,
        sample_weight: np.ndarray,
    ) -> Optional[tuple[int, float, float, float]]:
        best_stump = None
        best_error = float("inf")
        for feature_index in range(feature_matrix.shape[1]):
            values = feature_matrix[:, feature_index]
            thresholds = np.unique(np.quantile(values, np.linspace(0.1, 0.9, 9)))
            for threshold in thresholds:
                left_mask = values <= threshold
                right_mask = ~left_mask
                if left_mask.sum() < 20 or right_mask.sum() < 20:
                    continue
                left_value = _weighted_average(
                    residuals[left_mask],
                    sample_weight[left_mask],
                )
                right_value = _weighted_average(
                    residuals[right_mask],
                    sample_weight[right_mask],
                )
                predictions = np.where(left_mask, left_value, right_value)
                error = float(
                    np.sum(sample_weight * ((residuals - predictions) ** 2))
                )
                if error < best_error:
                    best_error = error
                    best_stump = (
                        int(feature_index),
                        float(threshold),
                        float(left_value),
                        float(right_value),
                    )
        return best_stump

    def predict_probability(self, feature_vector: np.ndarray) -> float:
        """Predict severe-loss probability for one feature vector."""
        logit = self.base_logit
        for feature_index, threshold, left_value, right_value in self.stumps:
            logit += self.learning_rate * (
                left_value if feature_vector[feature_index] <= threshold else right_value
            )
        return float(_sigmoid(logit))


def filter_bad_trade_candidate_rows(
    rows: Sequence[ModelingDatasetRow],
    *,
    min_price: float,
    min_score: float,
    min_signal_agreement: float,
) -> list[ModelingDatasetRow]:
    """Select dataset rows that resemble current advisory candidates."""
    return [
        row
        for row in rows
        if row.close_price >= min_price
        and calculate_heuristic_proxy_score(row) >= min_score
        and calculate_signal_agreement_proxy(row) >= min_signal_agreement
    ]


def build_bad_trade_feature_vector_from_row(row: ModelingDatasetRow) -> list[float]:
    """Build the ordered bad-trade risk feature vector from a dataset row."""
    probability_features = feature_vector_from_mapping(
        extract_probability_features_from_row(row)
    )
    return probability_features + _extra_feature_vector_from_row(row)


def build_bad_trade_feature_vector_from_snapshot(
    feature_snapshot: Mapping[str, object],
) -> list[float]:
    """Build the ordered bad-trade risk feature vector from a recommendation."""
    probability_features = feature_vector_from_mapping(
        extract_probability_features_from_snapshot(feature_snapshot)
    )
    return probability_features + _extra_feature_vector_from_snapshot(feature_snapshot)


def calculate_heuristic_proxy_score(row: ModelingDatasetRow) -> float:
    """Approximate advisory candidate strength from stable dataset features."""
    momentum = _score_band(row.price_change_10d or 0.0, low=-5.0, high=10.0)
    trend = _score_band(row.ma_30_vs_ma_90_pct, low=-10.0, high=15.0)
    volatility = max(0.0, 100.0 - (row.volatility_20d * 10.0))
    technical = _score_rsi(row.rsi_14)
    return (
        technical * 0.25
        + momentum * 0.32
        + volatility * 0.23
        + trend * 0.15
        + 50.0 * 0.05
    )


def calculate_signal_agreement_proxy(row: ModelingDatasetRow) -> float:
    """Approximate advisory signal agreement from model-ready row features."""
    checks = [
        row.price_change_10d is not None and row.price_change_10d > 0.0,
        row.ma_30_vs_ma_90_pct > 0.0,
        row.close_vs_ma_30_pct > 0.0,
        row.rsi_14 is not None and 40.0 <= row.rsi_14 <= 75.0,
        row.macd is not None and row.macd_signal is not None and row.macd > row.macd_signal,
    ]
    return sum(1 for item in checks if item) / len(checks)


def _extra_feature_vector_from_row(row: ModelingDatasetRow) -> list[float]:
    heuristic_score = calculate_heuristic_proxy_score(row)
    signal_agreement = calculate_signal_agreement_proxy(row)
    return [
        heuristic_score,
        signal_agreement,
        heuristic_score * signal_agreement,
        _flag(row.close_vs_20d_high_pct >= -2.0),
        _flag((row.price_change_20d or 0.0) >= 12.0),
        _flag(row.rebound_20d_pct >= 20.0),
    ]


def _extra_feature_vector_from_snapshot(
    feature_snapshot: Mapping[str, object],
) -> list[float]:
    indicators = feature_snapshot.get("indicators", {})
    if not isinstance(indicators, Mapping):
        indicators = {}

    heuristic_score = safe_float(feature_snapshot.get("heuristic_score"))
    signal_agreement = safe_float(feature_snapshot.get("signal_agreement"))
    close_vs_20d_high_pct = safe_float(indicators.get("close_vs_20d_high_pct"))
    price_change_20d = safe_float(indicators.get("price_change_20d"))
    rebound_20d_pct = safe_float(indicators.get("rebound_20d_pct"))
    heuristic_score = 50.0 if heuristic_score is None else heuristic_score
    signal_agreement = 0.0 if signal_agreement is None else signal_agreement
    close_vs_20d_high_pct = (
        -100.0 if close_vs_20d_high_pct is None else close_vs_20d_high_pct
    )
    price_change_20d = 0.0 if price_change_20d is None else price_change_20d
    rebound_20d_pct = 0.0 if rebound_20d_pct is None else rebound_20d_pct
    return [
        heuristic_score,
        signal_agreement,
        heuristic_score * signal_agreement,
        _flag(close_vs_20d_high_pct >= -2.0),
        _flag(price_change_20d >= 12.0),
        _flag(rebound_20d_pct >= 20.0),
    ]


def _score_band(value: float, *, low: float, high: float) -> float:
    if value <= low:
        return 0.0
    if value >= high:
        return 100.0
    return ((value - low) / (high - low)) * 100.0


def _score_rsi(value: Optional[float]) -> float:
    if value is None:
        return 50.0
    if 40.0 <= value <= 60.0:
        return 100.0
    if 30.0 <= value < 40.0 or 60.0 < value <= 70.0:
        return 80.0
    if 20.0 <= value < 30.0 or 70.0 < value <= 80.0:
        return 60.0
    return 30.0


def _flag(value: bool) -> float:
    return 1.0 if value else 0.0


def _weighted_average(values: np.ndarray, weights: np.ndarray) -> float:
    weight_sum = float(weights.sum())
    if weight_sum <= 0:
        return 0.0
    return float(np.sum(values * weights) / weight_sum)


def _sigmoid(value) -> np.ndarray:
    return 1.0 / (1.0 + np.exp(-np.clip(value, -30.0, 30.0)))
