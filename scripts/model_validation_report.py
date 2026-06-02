#!/usr/bin/env python3
"""Run model and trust validation against the current database.

This script is intentionally read-only. It builds validation datasets from
promoted price facts and indicators, runs walk-forward probability validation,
summarizes trust cohorts, and prints a compact JSON report for model iteration.
"""

from __future__ import annotations

import argparse
import json
import logging
from dataclasses import asdict
from datetime import date, timedelta
from typing import Iterable, Optional, Sequence

from sqlalchemy import func

from app.config.database import get_db
from app.models import FactDailyPrice
from app.services.modeling import (
    DirectionTargetDefinition,
    ModelingDatasetBuilder,
    ModelingDatasetConfig,
    TrustValidator,
    WalkForwardModelValidator,
)
from app.services.modeling.model_validation import ValidationPrediction
from app.services.modeling.trust_validation import (
    ALL_BAR_STATUSES,
    ALL_QUALITY_FLAGS,
)


DEFAULT_PROBABILITY_THRESHOLDS = (0.50, 0.55, 0.60, 0.65, 0.70, 0.75)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run walk-forward and trust validation for model iteration."
    )
    parser.add_argument("--start-date", type=_parse_date, help="Evaluation start date.")
    parser.add_argument("--end-date", type=_parse_date, help="Evaluation end date.")
    parser.add_argument(
        "--stocks",
        default="",
        help="Comma-separated stock codes. Blank means all active stocks.",
    )
    parser.add_argument("--training-window-days", type=int, default=365)
    parser.add_argument("--evaluation-window-days", type=int, default=60)
    parser.add_argument("--step-days", type=int, default=60)
    parser.add_argument(
        "--horizon-days",
        type=int,
        default=10,
        help="Forward trading-day horizon for the direction target.",
    )
    parser.add_argument(
        "--positive-threshold-pct",
        type=float,
        default=0.0,
        help="Minimum forward return percent required for a positive target label.",
    )
    parser.add_argument("--min-training-rows", type=int, default=250)
    parser.add_argument("--min-class-count", type=int, default=40)
    parser.add_argument("--iterations", type=int, default=500)
    parser.add_argument("--top-k", type=int, default=10)
    parser.add_argument(
        "--round-trip-cost-pct",
        type=float,
        default=0.20,
        help="Estimated transaction cost deducted from threshold return summaries.",
    )
    parser.add_argument(
        "--min-confidence-score",
        type=float,
        default=60.0,
        help="Minimum price confidence score for walk-forward dataset rows.",
    )
    parser.add_argument(
        "--max-abs-return-pct",
        type=float,
        default=50.0,
        help="Exclude anchor/forward returns above this absolute percent; negative disables.",
    )
    parser.add_argument(
        "--probability-thresholds",
        default=",".join(str(item) for item in DEFAULT_PROBABILITY_THRESHOLDS),
        help="Comma-separated probability thresholds to summarize.",
    )
    parser.add_argument(
        "--output",
        help="Optional path to write the JSON report. Prints to stdout when omitted.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    logging.getLogger("sqlalchemy.engine").setLevel(logging.WARNING)
    logging.getLogger("database").setLevel(logging.WARNING)
    logging.getLogger("walk_forward_model_validator").setLevel(logging.INFO)

    stock_codes = _parse_stock_codes(args.stocks)
    probability_thresholds = _parse_probability_thresholds(args.probability_thresholds)
    max_abs_return_pct = (
        None if args.max_abs_return_pct < 0 else args.max_abs_return_pct
    )

    db = get_db()
    db.engine.echo = False
    with db.get_session() as session:
        data_start, data_end = _price_date_bounds(session)
        if data_start is None or data_end is None:
            raise SystemExit("No fact_daily_prices rows found.")

        start_date = args.start_date or data_start + timedelta(
            days=args.training_window_days
        )
        end_date = args.end_date or data_end - timedelta(
            days=_default_label_buffer_days(args.horizon_days)
        )
        if start_date > end_date:
            raise SystemExit(
                f"Invalid validation window: {start_date} is after {end_date}."
            )

        dataset_config = ModelingDatasetConfig(
            target_definition=DirectionTargetDefinition(
                horizon_trading_days=args.horizon_days,
                positive_threshold_pct=args.positive_threshold_pct,
            ),
            min_confidence_score=args.min_confidence_score,
            max_abs_anchor_return_pct=max_abs_return_pct,
            max_abs_forward_return_pct=max_abs_return_pct,
        )
        dataset_builder = ModelingDatasetBuilder(session, config=dataset_config)
        validator = WalkForwardModelValidator(
            session,
            training_window_days=args.training_window_days,
            evaluation_window_days=args.evaluation_window_days,
            step_days=args.step_days,
            min_training_rows=args.min_training_rows,
            min_class_count=args.min_class_count,
            iterations=args.iterations,
            top_k=args.top_k,
            dataset_builder=dataset_builder,
        )
        model_report = validator.run(
            start_date=start_date,
            end_date=end_date,
            stock_codes=stock_codes,
        )

        broad_trust_builder = ModelingDatasetBuilder(
            session,
            config=ModelingDatasetConfig(
                target_definition=DirectionTargetDefinition(
                    horizon_trading_days=args.horizon_days,
                    positive_threshold_pct=args.positive_threshold_pct,
                ),
                allowed_bar_statuses=ALL_BAR_STATUSES,
                allowed_quality_flags=ALL_QUALITY_FLAGS,
                require_complete_data=False,
                min_confidence_score=None,
                max_abs_anchor_return_pct=max_abs_return_pct,
                max_abs_forward_return_pct=max_abs_return_pct,
            ),
        )
        trust_report = TrustValidator(
            session,
            dataset_builder=broad_trust_builder,
        ).run(
            start_date=start_date,
            end_date=end_date,
            stock_codes=stock_codes,
        )

    predictions = [
        prediction
        for fold in model_report.folds
        for prediction in fold.predictions
    ]
    output = {
        "validation_window": {
            "data_start": data_start.isoformat(),
            "data_end": data_end.isoformat(),
            "evaluation_start": start_date.isoformat(),
            "evaluation_end": end_date.isoformat(),
            "stock_codes": stock_codes or "ALL",
        },
        "config": {
            "training_window_days": args.training_window_days,
            "evaluation_window_days": args.evaluation_window_days,
            "step_days": args.step_days,
            "min_training_rows": args.min_training_rows,
            "min_class_count": args.min_class_count,
            "iterations": args.iterations,
            "top_k": args.top_k,
            "horizon_days": args.horizon_days,
            "positive_threshold_pct": args.positive_threshold_pct,
            "round_trip_cost_pct": args.round_trip_cost_pct,
            "min_confidence_score": args.min_confidence_score,
            "max_abs_return_pct": max_abs_return_pct,
        },
        "walk_forward": {
            "fold_count": model_report.fold_count,
            "skipped_folds": model_report.skipped_folds,
            "total_evaluated_rows": model_report.total_evaluated_rows,
            "overall_hit_rate": model_report.overall_hit_rate,
            "overall_brier_score": model_report.overall_brier_score,
            "overall_average_forward_return_10d": (
                model_report.overall_average_forward_return_10d
            ),
            "overall_top_k_hit_rate": model_report.overall_top_k_hit_rate,
            "overall_top_k_average_forward_return_10d": (
                model_report.overall_top_k_average_forward_return_10d
            ),
            "baseline": asdict(model_report.overall_baseline_comparison),
            "probability_buckets": [
                asdict(item) for item in model_report.overall_bucket_stats
            ],
            "threshold_candidates": summarize_probability_thresholds(
                predictions,
                probability_thresholds,
                round_trip_cost_pct=args.round_trip_cost_pct,
            ),
        },
        "trust": {
            "overall_row_count": trust_report.overall_row_count,
            "filter_comparisons": [
                asdict(item) for item in trust_report.filter_comparisons
            ],
            "confidence_band_stats": [
                asdict(item) for item in trust_report.confidence_band_stats
            ],
            "quality_flag_stats": [
                asdict(item) for item in trust_report.quality_flag_stats
            ],
            "bar_status_stats": [
                asdict(item) for item in trust_report.bar_status_stats
            ],
            "history_threshold_stats": [
                asdict(item) for item in trust_report.history_threshold_stats
            ],
        },
    }
    rendered = json.dumps(output, indent=2, sort_keys=True)
    if args.output:
        with open(args.output, "w", encoding="utf-8") as handle:
            handle.write(rendered)
            handle.write("\n")
    else:
        print(rendered)


def summarize_probability_thresholds(
    predictions: Sequence[ValidationPrediction],
    thresholds: Iterable[float],
    *,
    round_trip_cost_pct: float,
) -> list[dict[str, float]]:
    total_count = len(predictions)
    summaries: list[dict[str, float]] = []
    for threshold in thresholds:
        selected = [
            item
            for item in predictions
            if item.predicted_probability_10d_up >= threshold
        ]
        row_count = len(selected)
        net_returns = [
            item.forward_return_10d - round_trip_cost_pct
            for item in selected
        ]
        wins = [value for value in net_returns if value > 0.0]
        losses = [value for value in net_returns if value <= 0.0]
        summaries.append(
            {
                "threshold": threshold,
                "row_count": row_count,
                "retained_pct": (row_count / total_count) * 100.0
                if total_count
                else 0.0,
                "positive_rate": (
                    sum(item.target_up_10d for item in selected) / row_count
                    if row_count
                    else 0.0
                ),
                "average_forward_return_10d": (
                    sum(item.forward_return_10d for item in selected) / row_count
                    if row_count
                    else 0.0
                ),
                "average_net_return_pct": (
                    sum(net_returns) / row_count
                    if row_count
                    else 0.0
                ),
                "net_win_rate": (
                    len(wins) / row_count
                    if row_count
                    else 0.0
                ),
                "average_net_win_pct": (
                    sum(wins) / len(wins)
                    if wins
                    else 0.0
                ),
                "average_net_loss_pct": (
                    sum(losses) / len(losses)
                    if losses
                    else 0.0
                ),
                "average_predicted_probability": (
                    sum(item.predicted_probability_10d_up for item in selected)
                    / row_count
                    if row_count
                    else 0.0
                ),
            }
        )
    return summaries


def _price_date_bounds(session) -> tuple[Optional[date], Optional[date]]:
    return session.query(
        func.min(FactDailyPrice.price_date),
        func.max(FactDailyPrice.price_date),
    ).one()


def _parse_date(raw_value: str) -> date:
    return date.fromisoformat(raw_value)


def _parse_stock_codes(raw_value: str) -> Optional[list[str]]:
    stock_codes = [item.strip().upper() for item in raw_value.split(",") if item.strip()]
    return stock_codes or None


def _parse_probability_thresholds(raw_value: str) -> tuple[float, ...]:
    thresholds = tuple(
        float(item.strip()) for item in raw_value.split(",") if item.strip()
    )
    for threshold in thresholds:
        if threshold < 0 or threshold > 1:
            raise argparse.ArgumentTypeError(
                "probability thresholds must be between 0 and 1"
            )
    return thresholds


def _default_label_buffer_days(horizon_days: int) -> int:
    return max(horizon_days * 3, horizon_days + 14)


if __name__ == "__main__":
    main()
