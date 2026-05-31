"""Walk-forward model validation for the canonical 10-day direction target."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, timedelta
from typing import Dict, Iterable, List, Optional, Sequence

import numpy as np

from app.services.modeling.dataset_builder import (
    ModelingDatasetBuilder,
    ModelingDatasetConfig,
    ModelingDatasetRow,
)
from app.services.modeling.feature_engineering import PROBABILITY_FEATURE_NAMES
from app.services.modeling.feature_extractor import (
    extract_probability_features_from_row,
    feature_vector_from_mapping,
)
from app.services.modeling.probability_estimator import LogisticProbabilityModel
from app.utils import get_logger


@dataclass(frozen=True)
class FoldWindow:
    """One walk-forward training/evaluation window."""

    training_start: date
    training_end: date
    evaluation_start: date
    evaluation_end: date


@dataclass(frozen=True)
class ValidationPrediction:
    """One evaluation-row prediction and its realized outcome."""

    stock_code: str
    anchor_date: date
    predicted_probability_10d_up: float
    target_up_10d: int
    forward_return_10d: float
    baseline_score: float


@dataclass(frozen=True)
class ProbabilityBucketStat:
    """Aggregate statistics for one probability bucket."""

    label: str
    lower_bound: float
    upper_bound: float
    row_count: int
    positive_rate: float
    average_forward_return_10d: float


@dataclass(frozen=True)
class BaselineComparison:
    """Comparison metrics for a simple baseline ranking/classifier."""

    name: str
    hit_rate: float
    top_k_hit_rate: float
    top_k_average_forward_return_10d: float


@dataclass(frozen=True)
class WalkForwardFoldResult:
    """Metrics for one walk-forward validation fold."""

    window: FoldWindow
    training_row_count: int
    evaluation_row_count: int
    positive_rate: float
    hit_rate: float
    brier_score: float
    average_forward_return_10d: float
    top_k_hit_rate: float
    top_k_average_forward_return_10d: float
    bucket_stats: tuple[ProbabilityBucketStat, ...]
    baseline_comparison: BaselineComparison
    predictions: tuple[ValidationPrediction, ...] = field(default_factory=tuple)


@dataclass(frozen=True)
class ModelValidationReport:
    """Aggregate report across walk-forward folds."""

    folds: tuple[WalkForwardFoldResult, ...]
    skipped_folds: int
    total_evaluated_rows: int
    overall_hit_rate: float
    overall_brier_score: float
    overall_average_forward_return_10d: float
    overall_top_k_hit_rate: float
    overall_top_k_average_forward_return_10d: float
    overall_bucket_stats: tuple[ProbabilityBucketStat, ...]
    overall_baseline_comparison: BaselineComparison

    @property
    def fold_count(self) -> int:
        """Return the number of successful evaluation folds."""
        return len(self.folds)


def generate_walk_forward_windows(
    *,
    start_date: date,
    end_date: date,
    training_window_days: int,
    evaluation_window_days: int,
    step_days: Optional[int] = None,
) -> list[FoldWindow]:
    """Generate chronological walk-forward fold windows."""
    if evaluation_window_days <= 0:
        raise ValueError("evaluation_window_days must be positive")
    if training_window_days <= 0:
        raise ValueError("training_window_days must be positive")

    actual_step_days = step_days or evaluation_window_days
    if actual_step_days <= 0:
        raise ValueError("step_days must be positive")

    windows: list[FoldWindow] = []
    evaluation_start = start_date

    while evaluation_start <= end_date:
        evaluation_end = min(
            end_date,
            evaluation_start + timedelta(days=evaluation_window_days - 1),
        )
        training_end = evaluation_start - timedelta(days=1)
        training_start = training_end - timedelta(days=training_window_days - 1)
        windows.append(
            FoldWindow(
                training_start=training_start,
                training_end=training_end,
                evaluation_start=evaluation_start,
                evaluation_end=evaluation_end,
            )
        )
        evaluation_start = evaluation_start + timedelta(days=actual_step_days)

    return windows


class WalkForwardModelValidator:
    """Evaluate the probability model with walk-forward, no-leakage splits."""

    def __init__(
        self,
        session,
        *,
        training_window_days: int = 365,
        evaluation_window_days: int = 30,
        step_days: Optional[int] = None,
        min_training_rows: int = 100,
        min_class_count: int = 20,
        learning_rate: float = 0.10,
        iterations: int = 500,
        l2_penalty: float = 0.001,
        top_k: int = 5,
        bucket_edges: Sequence[float] = (0.0, 0.4, 0.5, 0.6, 0.7, 1.0),
        dataset_builder: Optional[ModelingDatasetBuilder] = None,
    ):
        self.session = session
        self.training_window_days = training_window_days
        self.evaluation_window_days = evaluation_window_days
        self.step_days = step_days
        self.min_training_rows = min_training_rows
        self.min_class_count = min_class_count
        self.learning_rate = learning_rate
        self.iterations = iterations
        self.l2_penalty = l2_penalty
        self.top_k = top_k
        self.bucket_edges = tuple(bucket_edges)
        self.dataset_builder = dataset_builder or ModelingDatasetBuilder(
            session,
            config=ModelingDatasetConfig(min_confidence_score=60.0),
        )
        self.logger = get_logger("walk_forward_model_validator")

    def run(
        self,
        *,
        start_date: date,
        end_date: date,
        stock_codes: Optional[Sequence[str]] = None,
    ) -> ModelValidationReport:
        """Run walk-forward validation across the requested date range."""
        folds: list[WalkForwardFoldResult] = []
        skipped_folds = 0

        for window in generate_walk_forward_windows(
            start_date=start_date,
            end_date=end_date,
            training_window_days=self.training_window_days,
            evaluation_window_days=self.evaluation_window_days,
            step_days=self.step_days,
        ):
            training_rows = self._build_rows_for_anchor_window(
                anchor_start=window.training_start,
                anchor_end=window.training_end,
                stock_codes=stock_codes,
            )
            evaluation_rows = self._build_rows_for_anchor_window(
                anchor_start=window.evaluation_start,
                anchor_end=window.evaluation_end,
                stock_codes=stock_codes,
            )

            if not evaluation_rows:
                skipped_folds += 1
                continue

            model = self._train_model(training_rows)
            if model is None:
                skipped_folds += 1
                continue

            predictions = self._predict_rows(model, evaluation_rows)
            folds.append(
                self._build_fold_result(
                    window=window,
                    training_rows=training_rows,
                    predictions=predictions,
                )
            )

        return self._build_report(folds=folds, skipped_folds=skipped_folds)

    def _build_rows_for_anchor_window(
        self,
        *,
        anchor_start: date,
        anchor_end: date,
        stock_codes: Optional[Sequence[str]],
    ) -> list[ModelingDatasetRow]:
        """Build rows whose anchors fall within the requested inclusive window."""
        horizon_days = self.dataset_builder.config.target_definition.horizon_trading_days
        rows = self.dataset_builder.build(
            start_date=anchor_start,
            end_date=anchor_end + timedelta(days=horizon_days),
            stock_codes=stock_codes,
        )
        return [
            row
            for row in rows
            if anchor_start <= row.anchor_date <= anchor_end
        ]

    def _train_model(
        self,
        rows: Sequence[ModelingDatasetRow],
    ) -> Optional[LogisticProbabilityModel]:
        """Train a logistic model when the fold has enough data and class balance."""
        if len(rows) < self.min_training_rows:
            return None

        feature_matrix = np.array(
            [
                feature_vector_from_mapping(extract_probability_features_from_row(row))
                for row in rows
            ],
            dtype=float,
        )
        labels = np.array([row.target_up_10d for row in rows], dtype=float)
        positives = int(labels.sum())
        negatives = int(len(labels) - positives)
        if min(positives, negatives) < self.min_class_count:
            return None

        return LogisticProbabilityModel.fit(
            feature_matrix,
            labels,
            feature_names=PROBABILITY_FEATURE_NAMES,
            learning_rate=self.learning_rate,
            iterations=self.iterations,
            l2_penalty=self.l2_penalty,
        )

    def _predict_rows(
        self,
        model: LogisticProbabilityModel,
        rows: Sequence[ModelingDatasetRow],
    ) -> list[ValidationPrediction]:
        """Predict evaluation rows with the trained fold model."""
        predictions: list[ValidationPrediction] = []
        for row in rows:
            feature_vector = np.array(
                feature_vector_from_mapping(extract_probability_features_from_row(row)),
                dtype=float,
            )
            predictions.append(
                ValidationPrediction(
                    stock_code=row.stock_code,
                    anchor_date=row.anchor_date,
                    predicted_probability_10d_up=model.predict_probability(feature_vector),
                    target_up_10d=row.target_up_10d,
                    forward_return_10d=row.forward_return_10d,
                    baseline_score=float(row.price_change_10d or 0.0),
                )
            )
        return predictions

    def _build_fold_result(
        self,
        *,
        window: FoldWindow,
        training_rows: Sequence[ModelingDatasetRow],
        predictions: Sequence[ValidationPrediction],
    ) -> WalkForwardFoldResult:
        """Summarize one walk-forward fold."""
        hit_rate = classification_hit_rate(predictions)
        brier_score = brier_score_from_predictions(predictions)
        average_forward_return_value = average_forward_return(predictions)
        top_predictions = top_k_predictions(predictions, self.top_k)
        bucket_stats = build_probability_bucket_stats(
            predictions,
            bucket_edges=self.bucket_edges,
        )
        baseline = compare_momentum_baseline(predictions, top_k=self.top_k)

        return WalkForwardFoldResult(
            window=window,
            training_row_count=len(training_rows),
            evaluation_row_count=len(predictions),
            positive_rate=sum(item.target_up_10d for item in predictions) / len(predictions),
            hit_rate=hit_rate,
            brier_score=brier_score,
            average_forward_return_10d=average_forward_return_value,
            top_k_hit_rate=classification_hit_rate(top_predictions),
            top_k_average_forward_return_10d=average_forward_return(top_predictions),
            bucket_stats=tuple(bucket_stats),
            baseline_comparison=baseline,
            predictions=tuple(predictions),
        )

    def _build_report(
        self,
        *,
        folds: Sequence[WalkForwardFoldResult],
        skipped_folds: int,
    ) -> ModelValidationReport:
        """Build the aggregate validation report from successful folds."""
        all_predictions = [
            prediction
            for fold in folds
            for prediction in fold.predictions
        ]
        aggregate_top_predictions = top_k_predictions(all_predictions, self.top_k)

        return ModelValidationReport(
            folds=tuple(folds),
            skipped_folds=skipped_folds,
            total_evaluated_rows=len(all_predictions),
            overall_hit_rate=classification_hit_rate(all_predictions),
            overall_brier_score=brier_score_from_predictions(all_predictions),
            overall_average_forward_return_10d=average_forward_return(all_predictions),
            overall_top_k_hit_rate=classification_hit_rate(aggregate_top_predictions),
            overall_top_k_average_forward_return_10d=average_forward_return(
                aggregate_top_predictions
            ),
            overall_bucket_stats=tuple(
                build_probability_bucket_stats(
                    all_predictions,
                    bucket_edges=self.bucket_edges,
                )
            ),
            overall_baseline_comparison=compare_momentum_baseline(
                all_predictions,
                top_k=self.top_k,
            ),
        )


def classification_hit_rate(predictions: Sequence[ValidationPrediction]) -> float:
    """Return the positive-class hit rate using a 0.5 probability threshold."""
    if not predictions:
        return 0.0
    correct = sum(
        1
        for item in predictions
        if (item.predicted_probability_10d_up >= 0.5) == bool(item.target_up_10d)
    )
    return correct / len(predictions)


def brier_score_from_predictions(predictions: Sequence[ValidationPrediction]) -> float:
    """Return the mean Brier score across validation predictions."""
    if not predictions:
        return 0.0
    return sum(
        (item.predicted_probability_10d_up - item.target_up_10d) ** 2
        for item in predictions
    ) / len(predictions)


def average_forward_return(predictions: Sequence[ValidationPrediction]) -> float:
    """Return the average forward 10-day return across predictions."""
    if not predictions:
        return 0.0
    return sum(item.forward_return_10d for item in predictions) / len(predictions)


def top_k_predictions(
    predictions: Sequence[ValidationPrediction],
    top_k: int,
) -> list[ValidationPrediction]:
    """Return the top-k predictions sorted by model probability."""
    if top_k <= 0:
        return []
    return sorted(
        predictions,
        key=lambda item: (
            item.predicted_probability_10d_up,
            item.forward_return_10d,
            item.stock_code,
        ),
        reverse=True,
    )[:top_k]


def build_probability_bucket_stats(
    predictions: Sequence[ValidationPrediction],
    *,
    bucket_edges: Sequence[float],
) -> list[ProbabilityBucketStat]:
    """Bucket predictions by probability and summarize outcomes."""
    if len(bucket_edges) < 2:
        raise ValueError("bucket_edges must contain at least two boundaries")

    bucket_stats: list[ProbabilityBucketStat] = []
    for lower_bound, upper_bound in zip(bucket_edges[:-1], bucket_edges[1:]):
        bucket_predictions = [
            item
            for item in predictions
            if lower_bound <= item.predicted_probability_10d_up < upper_bound
            or (
                upper_bound == bucket_edges[-1]
                and lower_bound <= item.predicted_probability_10d_up <= upper_bound
            )
        ]
        row_count = len(bucket_predictions)
        positive_rate = (
            sum(item.target_up_10d for item in bucket_predictions) / row_count
            if row_count
            else 0.0
        )
        avg_forward_return = (
            sum(item.forward_return_10d for item in bucket_predictions) / row_count
            if row_count
            else 0.0
        )
        bucket_stats.append(
            ProbabilityBucketStat(
                label=f"{lower_bound:.2f}-{upper_bound:.2f}",
                lower_bound=lower_bound,
                upper_bound=upper_bound,
                row_count=row_count,
                positive_rate=positive_rate,
                average_forward_return_10d=avg_forward_return,
            )
        )
    return bucket_stats


def compare_momentum_baseline(
    predictions: Sequence[ValidationPrediction],
    *,
    top_k: int,
) -> BaselineComparison:
    """Compare the model against a naive 10-day momentum baseline."""
    if not predictions:
        return BaselineComparison(
            name="price_change_10d_momentum",
            hit_rate=0.0,
            top_k_hit_rate=0.0,
            top_k_average_forward_return_10d=0.0,
        )

    baseline_hit_rate = sum(
        1
        for item in predictions
        if (item.baseline_score >= 0.0) == bool(item.target_up_10d)
    ) / len(predictions)

    baseline_top_predictions = sorted(
        predictions,
        key=lambda item: (item.baseline_score, item.forward_return_10d, item.stock_code),
        reverse=True,
    )[:top_k]
    return BaselineComparison(
        name="price_change_10d_momentum",
        hit_rate=baseline_hit_rate,
        top_k_hit_rate=sum(
            item.target_up_10d for item in baseline_top_predictions
        ) / len(baseline_top_predictions)
        if baseline_top_predictions
        else 0.0,
        top_k_average_forward_return_10d=average_forward_return(
            baseline_top_predictions
        ),
    )
