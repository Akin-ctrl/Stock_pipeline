#!/usr/bin/env python3
"""Run walk-forward advisory backtests with an explicit bad-trade risk filter."""

from __future__ import annotations

import argparse
import json
import logging
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Optional, Sequence

from sqlalchemy import func

from app.config.database import get_db
from app.models import FactDailyPrice
from app.services.advisory.advisor import StockRecommendation
from app.services.backtesting import BacktestResult, RecommendationBacktester
from app.services.modeling import (
    BadTradeRiskModelConfig,
    DirectionTargetDefinition,
    FittedBadTradeRiskEstimator,
    ModelingDatasetBuilder,
    ModelingDatasetConfig,
    generate_walk_forward_windows,
)
from app.services.modeling.dataset_builder import ModelingDatasetRow


@dataclass
class RiskFilterStats:
    """Mutable per-fold counters for the advisory risk filter."""

    evaluated: int = 0
    blocked: int = 0
    kept: int = 0

    @property
    def blocked_pct(self) -> float:
        """Return the percentage of evaluated recommendations blocked."""
        if self.evaluated == 0:
            return 0.0
        return self.blocked / self.evaluated * 100.0

    def to_dict(self) -> dict:
        """Serialize counters for JSON output."""
        return {
            "evaluated": self.evaluated,
            "blocked": self.blocked,
            "kept": self.kept,
            "blocked_pct": round(self.blocked_pct, 2),
        }


class BadTradeRecommendationFilter:
    """Filter advisory recommendations with a fitted bad-trade risk estimator."""

    def __init__(
        self,
        *,
        estimator: FittedBadTradeRiskEstimator,
        max_bad_trade_probability: float,
    ):
        self.estimator = estimator
        self.max_bad_trade_probability = max_bad_trade_probability
        self.stats = RiskFilterStats()

    def __call__(
        self,
        recommendation_date: date,
        recommendations: list[StockRecommendation],
    ) -> list[StockRecommendation]:
        kept = []
        for recommendation in recommendations:
            self.stats.evaluated += 1
            probability = self.estimator.estimate_bad_trade_probability(
                _build_recommendation_snapshot(recommendation, recommendation_date)
            )
            if (
                probability is not None
                and probability >= self.max_bad_trade_probability
            ):
                self.stats.blocked += 1
                continue
            self.stats.kept += 1
            kept.append(recommendation)
        return kept


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Walk-forward backtest with an advisory bad-trade risk filter."
    )
    parser.add_argument("--start-date", type=_parse_date, required=True)
    parser.add_argument("--end-date", type=_parse_date, required=True)
    parser.add_argument("--stocks", default="")
    parser.add_argument("--horizon-days", type=int, default=10)
    parser.add_argument("--training-window-days", type=int, default=720)
    parser.add_argument("--evaluation-window-days", type=int, default=90)
    parser.add_argument("--step-days", type=int, default=90)
    parser.add_argument("--bad-loss-threshold-pct", type=float, default=-5.0)
    parser.add_argument("--max-bad-trade-probability", type=float, default=0.55)
    parser.add_argument("--min-training-rows", type=int, default=500)
    parser.add_argument("--min-bad-count", type=int, default=40)
    parser.add_argument("--iterations", type=int, default=300)
    parser.add_argument("--learning-rate", type=float, default=0.08)
    parser.add_argument("--candidate-min-price", type=float, default=8.0)
    parser.add_argument("--candidate-min-score", type=float, default=70.0)
    parser.add_argument("--candidate-min-signal-agreement", type=float, default=0.60)
    parser.add_argument(
        "--strategy-profile",
        default="steady_20p_10d",
        choices=["steady_20p_10d", "steady_20p_10d_v2"],
    )
    parser.add_argument("--round-trip-cost-pct", type=float, default=0.20)
    parser.add_argument("--max-abs-gross-return-pct", type=float, default=50.0)
    parser.add_argument("--min-score", type=float, default=None)
    parser.add_argument("--min-confidence", type=float, default=None)
    parser.add_argument("--top-n-per-day", type=int, default=1)
    parser.add_argument("--avoid-overlap", action="store_true")
    parser.add_argument("--summary-only", action="store_true")
    parser.add_argument("--output")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    _quiet_runtime_logs()

    stock_codes = _parse_stock_codes(args.stocks)
    max_abs_gross_return_pct = (
        None if args.max_abs_gross_return_pct < 0 else args.max_abs_gross_return_pct
    )
    risk_config = BadTradeRiskModelConfig(
        bad_loss_threshold_pct=args.bad_loss_threshold_pct,
        min_training_rows=args.min_training_rows,
        min_bad_count=args.min_bad_count,
        iterations=args.iterations,
        learning_rate=args.learning_rate,
        candidate_min_price=args.candidate_min_price,
        candidate_min_score=args.candidate_min_score,
        candidate_min_signal_agreement=args.candidate_min_signal_agreement,
    )

    db = get_db()
    db.engine.echo = False
    with db.get_session() as session:
        data_start, data_end = session.query(
            func.min(FactDailyPrice.price_date),
            func.max(FactDailyPrice.price_date),
        ).one()

        rows = _build_model_rows(
            session=session,
            args=args,
            stock_codes=stock_codes,
        )
        report = _run_walk_forward_backtest(
            session=session,
            rows=rows,
            args=args,
            stock_codes=stock_codes,
            risk_config=risk_config,
            max_abs_gross_return_pct=max_abs_gross_return_pct,
        )

    payload = {
        "validation_window": {
            "data_start": data_start.isoformat() if data_start else None,
            "data_end": data_end.isoformat() if data_end else None,
            "evaluation_start": args.start_date.isoformat(),
            "evaluation_end": args.end_date.isoformat(),
            "stock_codes": stock_codes or "ALL",
        },
        "config": {
            "horizon_days": args.horizon_days,
            "training_window_days": args.training_window_days,
            "evaluation_window_days": args.evaluation_window_days,
            "step_days": args.step_days,
            "bad_loss_threshold_pct": args.bad_loss_threshold_pct,
            "max_bad_trade_probability": args.max_bad_trade_probability,
            "top_n_per_day": args.top_n_per_day,
            "avoid_overlap": args.avoid_overlap,
        },
        **report,
    }
    if args.summary_only:
        payload["combined_result"].pop("trades", None)
        for fold in payload["folds"]:
            fold["result"].pop("trades", None)

    output = json.dumps(payload, indent=2, default=str)
    if args.output:
        Path(args.output).write_text(output + "\n", encoding="utf-8")
    else:
        print(output)


def _build_model_rows(
    *,
    session,
    args: argparse.Namespace,
    stock_codes: Optional[list[str]],
) -> list[ModelingDatasetRow]:
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
    return dataset_builder.build(
        start_date=args.start_date - timedelta(days=args.training_window_days + 180),
        end_date=args.end_date + timedelta(days=_label_buffer_days(args.horizon_days)),
        stock_codes=stock_codes,
    )


def _run_walk_forward_backtest(
    *,
    session,
    rows: Sequence[ModelingDatasetRow],
    args: argparse.Namespace,
    stock_codes: Optional[list[str]],
    risk_config: BadTradeRiskModelConfig,
    max_abs_gross_return_pct: Optional[float],
) -> dict:
    folds = []
    all_trades = []
    skipped_folds = 0
    windows = generate_walk_forward_windows(
        start_date=args.start_date,
        end_date=args.end_date,
        training_window_days=args.training_window_days,
        evaluation_window_days=args.evaluation_window_days,
        step_days=args.step_days,
    )

    for window in windows:
        training_rows = _filter_rows(rows, window.training_start, window.training_end)
        training_result = FittedBadTradeRiskEstimator.fit_from_rows(
            training_rows,
            config=risk_config,
            trained_as_of_date=window.training_end,
        )
        if not training_result.trained:
            skipped_folds += 1
            folds.append(
                {
                    "window": _window_to_dict(window),
                    "skipped_reason": training_result.skipped_reason,
                    "training_rows": training_result.training_rows,
                    "training_bad_count": training_result.training_bad_count,
                    "training_good_count": training_result.training_good_count,
                    "risk_filter": RiskFilterStats().to_dict(),
                    "result": BacktestResult(
                        start_date=window.evaluation_start,
                        end_date=window.evaluation_end,
                        horizon_days=args.horizon_days,
                    ).to_dict(),
                }
            )
            continue

        risk_filter = BadTradeRecommendationFilter(
            estimator=training_result.estimator,
            max_bad_trade_probability=args.max_bad_trade_probability,
        )
        backtester = RecommendationBacktester(
            session=session,
            strategy_profile=args.strategy_profile,
            round_trip_cost_pct=args.round_trip_cost_pct,
            max_abs_gross_return_pct=max_abs_gross_return_pct,
            calculate_probability=False,
            recommendation_filter=risk_filter,
        )
        result = backtester.run(
            start_date=window.evaluation_start,
            end_date=min(window.evaluation_end, args.end_date),
            horizon_days=args.horizon_days,
            stock_codes=stock_codes,
            min_score=args.min_score,
            min_confidence=args.min_confidence,
            include_hold=False,
            top_n_per_day=args.top_n_per_day,
            avoid_overlapping_positions=args.avoid_overlap,
        )
        all_trades.extend(result.trades)
        folds.append(
            {
                "window": _window_to_dict(window),
                "training_rows": training_result.training_rows,
                "training_bad_count": training_result.training_bad_count,
                "training_good_count": training_result.training_good_count,
                "risk_filter": risk_filter.stats.to_dict(),
                "result": result.to_dict(),
            }
        )

    combined_result = BacktestResult(
        start_date=args.start_date,
        end_date=args.end_date,
        horizon_days=args.horizon_days,
        trades=all_trades,
    )
    return {
        "fold_count": len(folds),
        "skipped_folds": skipped_folds,
        "combined_result": combined_result.to_dict(),
        "folds": folds,
    }


def _build_recommendation_snapshot(
    recommendation: StockRecommendation,
    recommendation_date: date,
) -> dict[str, object]:
    return {
        "stock_id": recommendation.stock_id,
        "stock_code": recommendation.stock_code,
        "recommendation_date": recommendation_date,
        "signal_type": recommendation.signal_type.value,
        "signal_agreement": recommendation.signal_agreement,
        "heuristic_score": recommendation.heuristic_score,
        "heuristic_score_category": recommendation.heuristic_score_category.value,
        "indicators": recommendation.indicators,
    }


def _filter_rows(
    rows: Sequence[ModelingDatasetRow],
    start_date: date,
    end_date: date,
) -> list[ModelingDatasetRow]:
    return [row for row in rows if start_date <= row.anchor_date <= end_date]


def _window_to_dict(window) -> dict[str, str]:
    return {
        "training_start": window.training_start.isoformat(),
        "training_end": window.training_end.isoformat(),
        "evaluation_start": window.evaluation_start.isoformat(),
        "evaluation_end": window.evaluation_end.isoformat(),
    }


def _parse_stock_codes(raw_value: str) -> Optional[list[str]]:
    stock_codes = [code.strip().upper() for code in raw_value.split(",") if code.strip()]
    return stock_codes or None


def _parse_date(raw_value: str) -> date:
    return date.fromisoformat(raw_value)


def _label_buffer_days(horizon_days: int) -> int:
    return max(horizon_days * 3, horizon_days + 14)


def _quiet_runtime_logs() -> None:
    logging.getLogger("sqlalchemy.engine").setLevel(logging.WARNING)
    logging.getLogger("database").setLevel(logging.WARNING)
    logging.getLogger("modeling_dataset_builder").setLevel(logging.WARNING)
    logging.getLogger("stock_screener").setLevel(logging.WARNING)
    logging.getLogger("recommendation_backtester").setLevel(logging.WARNING)
    logging.getLogger("historical_probability_estimator").setLevel(logging.WARNING)


if __name__ == "__main__":
    main()
