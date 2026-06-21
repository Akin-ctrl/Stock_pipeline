"""Probability-estimation interfaces and baseline models for recommendation confidence."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from typing import Mapping, Optional, Protocol, Sequence

import numpy as np
import os
try:
    import mlflow
    from mlflow.tracking import MlflowClient
except ModuleNotFoundError:
    mlflow = None
    MlflowClient = None

from app.services.modeling.dataset_builder import (
    ModelingDatasetBuilder,
    ModelingDatasetConfig,
    ModelingDatasetRow,
)
from app.services.modeling.feature_extractor import (
    PROBABILITY_FEATURE_NAMES,
    extract_probability_features_from_row,
    extract_probability_features_from_snapshot,
    feature_vector_from_mapping,
)
from app.utils import get_logger


class ProbabilityEstimator(Protocol):
    """Interface for probability estimators used by recommendation services."""

    @property
    def model_version(self) -> Optional[str]:
        """Identifier for the model that produced the last prediction, or None."""

    def estimate_probability_10d_up(
        self,
        feature_snapshot: Mapping[str, object],
    ) -> Optional[float]:
        """Estimate the probability that the stock will be up after 10 trading days."""


class NullProbabilityEstimator:
    """Compatibility estimator used before a trained probability model exists."""

    @property
    def model_version(self) -> Optional[str]:
        return None

    def estimate_probability_10d_up(
        self,
        feature_snapshot: Mapping[str, object],
    ) -> Optional[float]:
        """Return no prediction while preserving the future estimator interface."""
        return None


@dataclass(frozen=True)
class LogisticProbabilityModel:
    """Lightweight logistic-regression model for 10-day direction probability."""

    feature_names: tuple[str, ...]
    weights: np.ndarray
    intercept: float
    means: np.ndarray
    scales: np.ndarray

    @classmethod
    def fit(
        cls,
        feature_matrix: np.ndarray,
        labels: np.ndarray,
        *,
        feature_names: tuple[str, ...] = PROBABILITY_FEATURE_NAMES,
        learning_rate: float = 0.10,
        iterations: int = 500,
        l2_penalty: float = 0.001,
    ) -> "LogisticProbabilityModel":
        """Fit a simple logistic regression model with feature scaling."""
        if feature_matrix.ndim != 2:
            raise ValueError("feature_matrix must be 2-dimensional")
        if labels.ndim != 1:
            raise ValueError("labels must be 1-dimensional")
        if len(feature_matrix) != len(labels):
            raise ValueError("feature_matrix and labels length mismatch")

        means = feature_matrix.mean(axis=0)
        scales = feature_matrix.std(axis=0)
        scales = np.where(scales < 1e-8, 1.0, scales)
        standardized = (feature_matrix - means) / scales

        weights = np.zeros(standardized.shape[1], dtype=float)
        positive_rate = float(labels.mean())
        clipped_positive_rate = min(max(positive_rate, 1e-4), 1 - 1e-4)
        intercept = float(
            np.log(clipped_positive_rate / (1.0 - clipped_positive_rate))
        )

        for _ in range(iterations):
            logits = standardized @ weights + intercept
            predictions = _sigmoid(logits)
            errors = predictions - labels
            grad_weights = (standardized.T @ errors) / len(labels)
            grad_weights += l2_penalty * weights
            grad_intercept = float(errors.mean())

            weights -= learning_rate * grad_weights
            intercept -= learning_rate * grad_intercept

        return cls(
            feature_names=feature_names,
            weights=weights,
            intercept=intercept,
            means=means,
            scales=scales,
        )

    def predict_probability(self, feature_vector: np.ndarray) -> float:
        """Predict the positive-class probability for one numeric feature vector."""
        standardized = (feature_vector - self.means) / self.scales
        probability = float(_sigmoid(standardized @ self.weights + self.intercept))
        return min(max(probability, 0.001), 0.999)


class HistoricalLogisticProbabilityEstimator:
    """Train a rolling historical logistic baseline on the canonical dataset."""

    @property
    def model_version(self) -> Optional[str]:
        return "historical_logistic_v1"

    def __init__(
        self,
        session,
        *,
        training_window_days: int = 540,
        min_training_rows: int = 100,
        min_class_count: int = 20,
        learning_rate: float = 0.10,
        iterations: int = 500,
        l2_penalty: float = 0.001,
        dataset_builder: Optional[ModelingDatasetBuilder] = None,
        preloaded_rows: Optional[Sequence[ModelingDatasetRow]] = None,
    ):
        self.session = session
        self.training_window_days = training_window_days
        self.min_training_rows = min_training_rows
        self.min_class_count = min_class_count
        self.learning_rate = learning_rate
        self.iterations = iterations
        self.l2_penalty = l2_penalty
        self.dataset_builder = dataset_builder or ModelingDatasetBuilder(
            session,
            config=ModelingDatasetConfig(min_confidence_score=60.0),
        )
        self.preloaded_rows = list(preloaded_rows) if preloaded_rows is not None else None
        self.logger = get_logger("historical_probability_estimator")
        self._model_cache: dict[date, Optional[LogisticProbabilityModel]] = {}

    def estimate_probability_10d_up(
        self,
        feature_snapshot: Mapping[str, object],
    ) -> Optional[float]:
        """Estimate the 10-day up probability for the given recommendation snapshot."""
        recommendation_date = feature_snapshot.get("recommendation_date")
        if not isinstance(recommendation_date, date):
            return None

        model = self._get_or_train_model(recommendation_date)
        if model is None:
            return None

        feature_mapping = extract_probability_features_from_snapshot(feature_snapshot)
        feature_vector = np.array(feature_vector_from_mapping(feature_mapping), dtype=float)
        return model.predict_probability(feature_vector)

    def _get_or_train_model(
        self,
        recommendation_date: date,
    ) -> Optional[LogisticProbabilityModel]:
        """Reuse a cached model or train one for the requested as-of date."""
        if recommendation_date in self._model_cache:
            return self._model_cache[recommendation_date]

        model = self._train_model(recommendation_date)
        self._model_cache[recommendation_date] = model
        return model

    def _train_model(
        self,
        recommendation_date: date,
    ) -> Optional[LogisticProbabilityModel]:
        """Train a logistic baseline using only history available before the recommendation date."""
        training_end = recommendation_date - timedelta(days=1)
        training_start = recommendation_date - timedelta(days=self.training_window_days)
        rows = self._training_rows(training_start, training_end)

        if len(rows) < self.min_training_rows:
            self.logger.info(
                "Skipping probability training due to insufficient rows",
                extra={
                    "recommendation_date": recommendation_date.isoformat(),
                    "rows": len(rows),
                    "min_training_rows": self.min_training_rows,
                },
            )
            return None

        feature_matrix = np.array(
            [
                feature_vector_from_mapping(
                    extract_probability_features_from_row(row)
                )
                for row in rows
            ],
            dtype=float,
        )
        labels = np.array([row.target_up_10d for row in rows], dtype=float)

        positives = int(labels.sum())
        negatives = int(len(labels) - positives)
        if min(positives, negatives) < self.min_class_count:
            self.logger.info(
                "Skipping probability training due to weak class balance",
                extra={
                    "recommendation_date": recommendation_date.isoformat(),
                    "rows": len(rows),
                    "positives": positives,
                    "negatives": negatives,
                    "min_class_count": self.min_class_count,
                },
            )
            return None

        model = LogisticProbabilityModel.fit(
            feature_matrix,
            labels,
            learning_rate=self.learning_rate,
            iterations=self.iterations,
            l2_penalty=self.l2_penalty,
        )
        self.logger.info(
            "Trained historical probability model",
            extra={
                "recommendation_date": recommendation_date.isoformat(),
                "rows": len(rows),
                "positives": positives,
                "negatives": negatives,
                "features": len(PROBABILITY_FEATURE_NAMES),
            },
        )
        return model

    def _training_rows(
        self,
        training_start: date,
        training_end: date,
    ) -> list[ModelingDatasetRow]:
        """Return rows matching the normal rolling build window.

        When rows are preloaded, require both the anchor date and horizon date to
        fall inside the training cutoff. That preserves the no-future-leakage
        behavior of ``dataset_builder.build(..., end_date=training_end)``.
        """
        if self.preloaded_rows is None:
            return self.dataset_builder.build(
                start_date=training_start,
                end_date=training_end,
            )
        return [
            row
            for row in self.preloaded_rows
            if training_start <= row.anchor_date <= training_end
            and row.horizon_date <= training_end
        ]


def _sigmoid(values) -> np.ndarray:
    """Numerically stable sigmoid for numpy arrays or scalars."""
    return 1.0 / (1.0 + np.exp(-np.clip(values, -30.0, 30.0)))


class MLflowProbabilityEstimator:
    """Query MLflow for the latest registered production models.

    Requires MLFLOW_TRACKING_URI to be set in the environment when running
    outside Docker Compose (default: http://mlflow:5000, which only resolves
    inside the stock_network bridge network).
    """

    def __init__(
        self,
        horizon_days: int = 10,
        tracking_uri: Optional[str] = None,
    ):
        self.horizon_days = horizon_days
        self.tracking_uri = tracking_uri or os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000")
        self.model_name = f"stock_direction_{horizon_days}d_model"
        self.logger = get_logger(f"mlflow_estimator_{horizon_days}d")
        self._model = None
        self.model_unavailable: bool = False
        self._null_fallback_warned: bool = False

        try:
            if mlflow is None:
                raise ImportError("mlflow is not installed in this environment")
            mlflow.set_tracking_uri(self.tracking_uri)
            self.client = MlflowClient(tracking_uri=self.tracking_uri)
            self._load_latest_model()
        except Exception as e:
            self.logger.error(
                f"Failed to initialize MLflow client for {self.model_name}. "
                f"Ensure mlflow package is installed and server is running. Error: {e}"
            )
            self.client = None
            self._model = None
            self.model_unavailable = True

    @property
    def model_version(self) -> Optional[str]:
        return self.model_name if self._model is not None else None

    def _load_latest_model(self):
        """Attempt to load the latest registered model."""
        try:
            # First try loading "Production" alias/stage, otherwise latest
            model_uri = f"models:/{self.model_name}/latest"
            import xgboost as xgb
            self._model = mlflow.xgboost.load_model(model_uri)
            self.model_unavailable = False
            self.logger.info(f"Successfully loaded MLflow model: {self.model_name}")
        except Exception as e:
            self.logger.error(
                f"Failed to load MLflow model {self.model_name}. "
                f"Ensure it is trained and registered. Error: {e}"
            )
            self._model = None
            self.model_unavailable = True

    def estimate_probability_10d_up(
        self,
        feature_snapshot: Mapping[str, object],
    ) -> Optional[float]:
        """Estimate the positive direction probability using XGBoost from MLflow."""
        if self._model is None:
            if not self._null_fallback_warned:
                self.logger.warning(
                    f"MLflow model '{self.model_name}' is unavailable — all probability "
                    "estimates will be None for this run. Recommendations will be generated "
                    "without ML confidence scores. Train the model first with: "
                    "python -m app.cli models train"
                )
                self._null_fallback_warned = True
            return None

        # Extract stable mapping
        feature_mapping = extract_probability_features_from_snapshot(feature_snapshot)
        
        # XGBoost requires a Pandas DataFrame matching the exact training columns
        import pandas as pd
        df = pd.DataFrame([feature_mapping])
        
        # Detect and log any features the model expects but the snapshot lacks.
        # XGBoost will silently use its internal missing-value handling for absent
        # features, which may produce very different probabilities than intended.
        missing_features = [f for f in PROBABILITY_FEATURE_NAMES if f not in df.columns]
        if missing_features:
            self.logger.warning(
                f"Probability estimate missing {len(missing_features)} feature(s): "
                f"{missing_features}. Prediction may be unreliable."
            )
            if len(missing_features) > len(PROBABILITY_FEATURE_NAMES) // 2:
                self.logger.error(
                    f"More than half the required features are absent "
                    f"({len(missing_features)}/{len(PROBABILITY_FEATURE_NAMES)}); "
                    f"skipping prediction to avoid garbage output."
                )
                return None

        df = df[[f for f in PROBABILITY_FEATURE_NAMES if f in df.columns]]

        try:
            proba = self._model.predict_proba(df)[0][1]
            return float(proba)
        except Exception as e:
            self.logger.error(f"Error predicting with MLflow model: {e}")
            return None
