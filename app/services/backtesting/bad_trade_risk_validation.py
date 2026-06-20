#!/usr/bin/env python3
"""Validate a walk-forward bad-trade risk blocker on market data.

This script is read-only. It trains severe-loss classifiers on historical
model rows, then evaluates whether blocking high-risk candidate-like rows
would reduce losses without over-blocking winners.
"""

from __future__ import annotations

import argparse
import json
import logging
from datetime import date, timedelta
from pathlib import Path
from typing import Iterable, Optional, Protocol, Sequence

import numpy as np
from sqlalchemy import func

from app.config.database import get_db
from app.models import FactDailyPrice
from app.services.modeling import (
    BAD_TRADE_FEATURE_NAMES,
    DirectionTargetDefinition,
    GradientBoostedStumpRiskModel,
    LogisticProbabilityModel,
    ModelingDatasetBuilder,
    ModelingDatasetConfig,
    build_bad_trade_feature_vector_from_row,
    filter_bad_trade_candidate_rows,
    generate_walk_forward_windows,
)
from app.services.modeling.dataset_builder import ModelingDatasetRow


DEFAULT_BLOCK_THRESHOLDS = (0.20, 0.30, 0.40, 0.50, 0.60)


class RiskProbabilityModel(Protocol):
    """Shared prediction protocol for bad-trade validation models."""

    def predict_probability(self, feature_vector: np.ndarray) -> float:
        """Return the positive-class probability for one feature vector."""


class SklearnHistGradientBoostingRiskModel:
    """Small adapter around sklearn's histogram gradient boosting classifier."""

    def __init__(self, model):
        self.model = model

    @classmethod
    def fit(
        cls,
        feature_matrix: np.ndarray,
        labels: np.ndarray,
        *,
        iterations: int,
        learning_rate: float,
        l2_penalty: float,
    ) -> "SklearnHistGradientBoostingRiskModel":
        try:
            from sklearn.ensemble import HistGradientBoostingClassifier
        except ModuleNotFoundError as exc:
            raise SystemExit(
                "scikit-learn is required for --model hist_gradient_boosting. "
                "Install app/requirements.txt or rebuild the container."
            ) from exc

        positive_count = float(labels.sum())
        negative_count = float(len(labels) - positive_count)
        positive_weight = (negative_count / positive_count) if positive_count else 1.0
        sample_weight = np.where(labels == 1.0, positive_weight, 1.0)
        model = HistGradientBoostingClassifier(
            max_iter=iterations,
            learning_rate=learning_rate,
            l2_regularization=l2_penalty,
            max_leaf_nodes=15,
            min_samples_leaf=25,
            random_state=42,
        )
        model.fit(feature_matrix, labels, sample_weight=sample_weight)
        return cls(model)

    def predict_probability(self, feature_vector: np.ndarray) -> float:
        probabilities = self.model.predict_proba(feature_vector.reshape(1, -1))[0]
        return float(probabilities[1])


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Walk-forward validation for a bad-trade risk blocker."
    )
    parser.add_argument("--start-date", type=_parse_date, required=True)
    parser.add_argument("--end-date", type=_parse_date, required=True)
    parser.add_argument("--stocks", default="")
    parser.add_argument("--horizon-days", type=int, default=10)
    parser.add_argument(
        "--bad-loss-threshold-pct",
        type=float,
        default=-5.0,
        help="A row is bad if forward return is <= this percent.",
    )
    parser.add_argument("--training-window-days", type=int, default=720)
    parser.add_argument("--evaluation-window-days", type=int, default=90)
    parser.add_argument("--step-days", type=int, default=90)
    parser.add_argument("--min-training-rows", type=int, default=500)
    parser.add_argument("--min-bad-count", type=int, default=40)
    parser.add_argument("--iterations", type=int, default=300)
    parser.add_argument("--learning-rate", type=float, default=0.08)
    parser.add_argument("--l2-penalty", type=float, default=0.005)
    parser.add_argument(
        "--model",
        choices=("logistic", "stump_boosting", "hist_gradient_boosting"),
        default="logistic",
        help="Bad-trade classifier to validate.",
    )
    parser.add_argument("--candidate-min-price", type=float, default=8.0)
    parser.add_argument("--candidate-min-score", type=float, default=70.0)
    parser.add_argument("--candidate-min-signal-agreement", type=float, default=0.60)
    parser.add_argument(
        "--block-thresholds",
        default=",".join(str(item) for item in DEFAULT_BLOCK_THRESHOLDS),
        help="Comma-separated bad-risk probability thresholds to evaluate.",
    )
    parser.add_argument("--output")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    _quiet_runtime_logs()

    stock_codes = _parse_stock_codes(args.stocks)
    block_thresholds = _parse_thresholds(args.block_thresholds)

    db = get_db()
    db.engine.echo = False
    with db.get_session() as session:
        data_start, data_end = session.query(
            func.min(FactDailyPrice.price_date),
            func.max(FactDailyPrice.price_date),
        ).one()
        dataset_builder = ModelingDatasetBuilder(
            session,
            config=ModelingDatasetConfig(
                target_definition=DirectionTargetDefinition(
                    horizon_trading_days=args.horizon_days,
                ),
                min_confidence_score=60.0,
                max_abs_anchor_return_pct=50.0,
                max_abs_forward_return_pct=50.0,
            ),
        )
        rows = dataset_builder.build(
            start_date=args.start_date
            - timedelta(days=args.training_window_days + 180),
            end_date=args.end_date + timedelta(days=_label_buffer_days(args.horizon_days)),
            stock_codes=stock_codes,
        )

    output = _validate_rows(
        rows=rows,
        args=args,
        block_thresholds=block_thresholds,
        data_start=data_start,
        data_end=data_end,
        stock_codes=stock_codes,
    )
    rendered = json.dumps(output, indent=2, sort_keys=True)
    if args.output:
        Path(args.output).write_text(rendered + "\n", encoding="utf-8")
    else:
        print(rendered)


def _validate_rows(
    *,
    rows: Sequence[ModelingDatasetRow],
    args: argparse.Namespace,
    block_thresholds: tuple[float, ...],
    data_start: Optional[date],
    data_end: Optional[date],
    stock_codes: Optional[list[str]],
) -> dict:
    windows = generate_walk_forward_windows(
        start_date=args.start_date,
        end_date=args.end_date,
        training_window_days=args.training_window_days,
        evaluation_window_days=args.evaluation_window_days,
        step_days=args.step_days,
    )
    fold_reports = []
    predictions = []
    skipped_folds = 0

    for window in windows:
        training_rows = _filter_rows(rows, window.training_start, window.training_end)
        evaluation_rows = _filter_rows(rows, window.evaluation_start, window.evaluation_end)
        training_candidates = filter_bad_trade_candidate_rows(
            training_rows,
            min_price=args.candidate_min_price,
            min_score=args.candidate_min_score,
            min_signal_agreement=args.candidate_min_signal_agreement,
        )
        evaluation_candidates = filter_bad_trade_candidate_rows(
            evaluation_rows,
            min_price=args.candidate_min_price,
            min_score=args.candidate_min_score,
            min_signal_agreement=args.candidate_min_signal_agreement,
        )
        model = _train_bad_trade_model(
            training_candidates,
            bad_loss_threshold_pct=args.bad_loss_threshold_pct,
            min_training_rows=args.min_training_rows,
            min_bad_count=args.min_bad_count,
            learning_rate=args.learning_rate,
            iterations=args.iterations,
            l2_penalty=args.l2_penalty,
            model_name=args.model,
        )
        if model is None or not evaluation_candidates:
            skipped_folds += 1
            continue

        fold_predictions = _predict_rows(
            model,
            evaluation_candidates,
            bad_loss_threshold_pct=args.bad_loss_threshold_pct,
        )
        predictions.extend(fold_predictions)
        fold_reports.append(
            {
                "window": {
                    "training_start": window.training_start.isoformat(),
                    "training_end": window.training_end.isoformat(),
                    "evaluation_start": window.evaluation_start.isoformat(),
                    "evaluation_end": window.evaluation_end.isoformat(),
                },
                "training_candidates": len(training_candidates),
                "evaluation_candidates": len(evaluation_candidates),
                "training_bad_rate": _bad_rate(
                    training_candidates,
                    bad_loss_threshold_pct=args.bad_loss_threshold_pct,
                ),
                "evaluation_bad_rate": _bad_rate(
                    evaluation_candidates,
                    bad_loss_threshold_pct=args.bad_loss_threshold_pct,
                ),
                "thresholds": _summarize_block_thresholds(
                    fold_predictions,
                    thresholds=block_thresholds,
                ),
            }
        )

    return {
        "validation_window": {
            "data_start": data_start.isoformat() if data_start else None,
            "data_end": data_end.isoformat() if data_end else None,
            "evaluation_start": args.start_date.isoformat(),
            "evaluation_end": args.end_date.isoformat(),
            "stock_codes": stock_codes or "ALL",
        },
        "config": {
            "horizon_days": args.horizon_days,
            "bad_loss_threshold_pct": args.bad_loss_threshold_pct,
            "training_window_days": args.training_window_days,
            "evaluation_window_days": args.evaluation_window_days,
            "step_days": args.step_days,
            "candidate_min_price": args.candidate_min_price,
            "candidate_min_score": args.candidate_min_score,
            "candidate_min_signal_agreement": args.candidate_min_signal_agreement,
            "model": args.model,
        },
        "fold_count": len(fold_reports),
        "skipped_folds": skipped_folds,
        "total_predictions": len(predictions),
        "overall_bad_rate": (
            sum(item["bad_trade"] for item in predictions) / len(predictions)
            if predictions
            else 0.0
        ),
        "overall_average_forward_return_pct": (
            sum(item["forward_return_pct"] for item in predictions) / len(predictions)
            if predictions
            else 0.0
        ),
        "overall_thresholds": _summarize_block_thresholds(
            predictions,
            thresholds=block_thresholds,
        ),
        "folds": fold_reports,
    }


def _train_bad_trade_model(
    rows: Sequence[ModelingDatasetRow],
    *,
    bad_loss_threshold_pct: float,
    min_training_rows: int,
    min_bad_count: int,
    learning_rate: float,
    iterations: int,
    l2_penalty: float,
    model_name: str,
) -> Optional[RiskProbabilityModel]:
    if len(rows) < min_training_rows:
        return None
    labels = np.array(
        [int(row.forward_return_10d <= bad_loss_threshold_pct) for row in rows],
        dtype=float,
    )
    bad_count = int(labels.sum())
    good_count = int(len(labels) - bad_count)
    if min(bad_count, good_count) < min_bad_count:
        return None

    feature_matrix = np.array(
        [build_bad_trade_feature_vector_from_row(row) for row in rows],
        dtype=float,
    )
    if model_name == "logistic":
        return LogisticProbabilityModel.fit(
            feature_matrix,
            labels,
            feature_names=BAD_TRADE_FEATURE_NAMES,
            learning_rate=learning_rate,
            iterations=iterations,
            l2_penalty=l2_penalty,
        )
    if model_name == "stump_boosting":
        return GradientBoostedStumpRiskModel.fit(
            feature_matrix,
            labels,
            iterations=iterations,
            learning_rate=learning_rate,
        )
    if model_name == "hist_gradient_boosting":
        return SklearnHistGradientBoostingRiskModel.fit(
            feature_matrix,
            labels,
            iterations=iterations,
            learning_rate=learning_rate,
            l2_penalty=l2_penalty,
        )
    raise ValueError(f"Unsupported model {model_name!r}")


def _predict_rows(
    model: RiskProbabilityModel,
    rows: Sequence[ModelingDatasetRow],
    *,
    bad_loss_threshold_pct: float,
) -> list[dict]:
    predictions = []
    for row in rows:
        feature_vector = np.array(
            build_bad_trade_feature_vector_from_row(row),
            dtype=float,
        )
        predictions.append(
            {
                "stock_code": row.stock_code,
                "anchor_date": row.anchor_date.isoformat(),
                "predicted_bad_trade_probability": model.predict_probability(feature_vector),
                "bad_trade": int(row.forward_return_10d <= bad_loss_threshold_pct),
                "forward_return_pct": row.forward_return_10d,
            }
        )
    return predictions


def _summarize_block_thresholds(
    predictions: Sequence[dict],
    *,
    thresholds: Iterable[float],
) -> list[dict]:
    total = len(predictions)
    bad_total = sum(item["bad_trade"] for item in predictions)
    summaries = []
    for threshold in thresholds:
        blocked = [
            item
            for item in predictions
            if item["predicted_bad_trade_probability"] >= threshold
        ]
        kept = [
            item
            for item in predictions
            if item["predicted_bad_trade_probability"] < threshold
        ]
        blocked_bad = sum(item["bad_trade"] for item in blocked)
        summaries.append(
            {
                "block_threshold": threshold,
                "blocked_count": len(blocked),
                "blocked_pct": (len(blocked) / total * 100.0) if total else 0.0,
                "bad_trade_recall": (blocked_bad / bad_total) if bad_total else 0.0,
                "blocked_bad_rate": (blocked_bad / len(blocked)) if blocked else 0.0,
                "kept_count": len(kept),
                "kept_bad_rate": (
                    sum(item["bad_trade"] for item in kept) / len(kept)
                    if kept
                    else 0.0
                ),
                "kept_average_return_pct": (
                    sum(item["forward_return_pct"] for item in kept) / len(kept)
                    if kept
                    else 0.0
                ),
                "blocked_average_return_pct": (
                    sum(item["forward_return_pct"] for item in blocked) / len(blocked)
                    if blocked
                    else 0.0
                ),
            }
        )
    return summaries


def _filter_rows(
    rows: Sequence[ModelingDatasetRow],
    start_date: date,
    end_date: date,
) -> list[ModelingDatasetRow]:
    return [row for row in rows if start_date <= row.anchor_date <= end_date]


def _bad_rate(
    rows: Sequence[ModelingDatasetRow],
    *,
    bad_loss_threshold_pct: float,
) -> float:
    if not rows:
        return 0.0
    return sum(row.forward_return_10d <= bad_loss_threshold_pct for row in rows) / len(rows)


def _parse_stock_codes(raw_value: str) -> Optional[list[str]]:
    stock_codes = [item.strip().upper() for item in raw_value.split(",") if item.strip()]
    return stock_codes or None


def _parse_thresholds(raw_value: str) -> tuple[float, ...]:
    return tuple(float(item.strip()) for item in raw_value.split(",") if item.strip())


def _parse_date(raw_value: str) -> date:
    return date.fromisoformat(raw_value)


def _label_buffer_days(horizon_days: int) -> int:
    return max(horizon_days * 3, horizon_days + 14)


def _quiet_runtime_logs() -> None:
    logging.getLogger("sqlalchemy.engine").setLevel(logging.WARNING)
    logging.getLogger("database").setLevel(logging.WARNING)
    logging.getLogger("modeling_dataset_builder").setLevel(logging.WARNING)


if __name__ == "__main__":
    main()
