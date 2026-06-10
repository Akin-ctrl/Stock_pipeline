#!/usr/bin/env python3
"""Run a historical backtest of the recommendation engine."""

from __future__ import annotations

import argparse
import json
import logging
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Optional

from app.config.database import get_db
from app.services.advisory.advisor import StockRecommendation
from app.services.backtesting.portfolio_simulator import (
    PortfolioSimulationConfig,
    PortfolioSimulator,
)
from app.services.backtesting.recommendation_backtester import RecommendationBacktester


@dataclass
class TechnicalGuardStats:
    """Counters for optional technical guard backtest filters."""

    evaluated: int = 0
    rejected: int = 0
    kept: int = 0

    @property
    def rejected_pct(self) -> float:
        if self.evaluated == 0:
            return 0.0
        return self.rejected / self.evaluated * 100.0

    def to_dict(self) -> dict:
        return {
            "evaluated": self.evaluated,
            "rejected": self.rejected,
            "kept": self.kept,
            "rejected_pct": round(self.rejected_pct, 2),
        }


@dataclass(frozen=True)
class TechnicalGuardConfig:
    """Optional backtest-only filters for testing exhaustion guardrails."""

    min_drawdown_20d_pct: Optional[float] = None
    max_price_change_20d_pct: Optional[float] = None
    reject_near_20d_high_pct: Optional[float] = None
    reject_near_20d_high_rsi_min: Optional[float] = None
    reject_near_20d_high_price_change_10d_min: Optional[float] = None


class TechnicalGuardRecommendationFilter:
    """Reject recommendations that match explicit exhaustion guard rules."""

    def __init__(self, config: TechnicalGuardConfig):
        self.config = config
        self.stats = TechnicalGuardStats()

    @property
    def enabled(self) -> bool:
        return any(value is not None for value in self.config.__dict__.values())

    def __call__(
        self,
        recommendation_date: date,
        recommendations: list[StockRecommendation],
    ) -> list[StockRecommendation]:
        kept = []
        for recommendation in recommendations:
            self.stats.evaluated += 1
            if self._rejects(recommendation):
                self.stats.rejected += 1
                continue
            self.stats.kept += 1
            kept.append(recommendation)
        return kept

    def _rejects(self, recommendation: StockRecommendation) -> bool:
        indicators = recommendation.indicators
        cfg = self.config

        drawdown_20d_pct = indicators.get("drawdown_20d_pct")
        if (
            cfg.min_drawdown_20d_pct is not None
            and drawdown_20d_pct is not None
            and drawdown_20d_pct < cfg.min_drawdown_20d_pct
        ):
            return True

        price_change_20d = indicators.get("price_change_20d")
        if (
            cfg.max_price_change_20d_pct is not None
            and price_change_20d is not None
            and price_change_20d > cfg.max_price_change_20d_pct
        ):
            return True

        if cfg.reject_near_20d_high_pct is not None:
            close_vs_20d_high_pct = indicators.get("close_vs_20d_high_pct")
            near_high = (
                close_vs_20d_high_pct is not None
                and close_vs_20d_high_pct >= cfg.reject_near_20d_high_pct
            )
            rsi_qualifies = _optional_minimum_passes(
                indicators.get("rsi_14"),
                cfg.reject_near_20d_high_rsi_min,
            )
            momentum_qualifies = _optional_minimum_passes(
                indicators.get("price_change_10d"),
                cfg.reject_near_20d_high_price_change_10d_min,
            )
            if near_high and rsi_qualifies and momentum_qualifies:
                return True

        return False


class WarningCaptureHandler(logging.Handler):
    """Capture warning+ log records so readiness gates can fail on hidden errors."""

    def __init__(self):
        super().__init__(level=logging.WARNING)
        self.records: list[dict] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.records.append(
            {
                "logger": record.name,
                "level": record.levelname,
                "message": record.getMessage(),
            }
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Backtest recommendation engine on historical close prices")
    parser.add_argument("--start-date", required=True, help="Start date in YYYY-MM-DD format")
    parser.add_argument("--end-date", required=True, help="End date in YYYY-MM-DD format")
    parser.add_argument("--horizon-days", type=int, default=5, help="Trading-day holding period")
    parser.add_argument("--stocks", default="", help="Comma-separated stock codes; blank = all active stocks")
    parser.add_argument("--min-score", type=float, default=None, help="Legacy minimum heuristic score")
    parser.add_argument("--min-confidence", type=float, default=None, help="Legacy minimum signal agreement")
    parser.add_argument(
        "--min-predicted-probability",
        type=float,
        default=None,
        help="Override profile minimum predicted 10-day up probability; use 0 to disable the profile gate",
    )
    parser.add_argument(
        "--strategy-profile",
        default="steady_20p_10d",
        choices=["steady_20p_10d", "steady_20p_10d_v2"],
        help="Recommendation profile",
    )
    parser.add_argument("--round-trip-cost-pct", type=float, default=0.20, help="Estimated total transaction cost in percent")
    parser.add_argument("--max-abs-gross-return-pct", type=float, default=50.0, help="Exclude trades with absolute gross return above this percent; use a negative value to disable")
    parser.add_argument("--include-hold", action="store_true", help="Include HOLD signals in the evaluation")
    parser.add_argument("--top-n-per-day", type=int, default=None, help="Evaluate only the top N ranked recommendations per entry date")
    parser.add_argument("--avoid-overlap", action="store_true", help="Skip new entries for a stock while a prior horizon trade is still open")
    parser.add_argument("--disable-probability", action="store_true", help="Disable diagnostic probability estimation when no probability gate is being tested")
    parser.add_argument("--min-drawdown-20d-pct", type=float, default=None, help="Backtest-only guard: reject candidates with 20d drawdown below this percent")
    parser.add_argument("--max-price-change-20d-pct", type=float, default=None, help="Backtest-only guard: reject candidates with 20d price change above this percent")
    parser.add_argument("--reject-near-20d-high-pct", type=float, default=None, help="Backtest-only guard: reject candidates at/above this close-vs-20d-high percent")
    parser.add_argument("--reject-near-20d-high-rsi-min", type=float, default=None, help="Only apply the near-high guard when RSI is at/above this value")
    parser.add_argument("--reject-near-20d-high-price-change-10d-min", type=float, default=None, help="Only apply the near-high guard when 10d price change is at/above this percent")
    parser.add_argument("--simulate-portfolio", action="store_true", help="Add portfolio-level capital allocation metrics to the output")
    parser.add_argument("--portfolio-initial-capital", type=float, default=1_000_000.0, help="Starting capital for portfolio simulation")
    parser.add_argument("--portfolio-max-concurrent-positions", type=int, default=3, help="Maximum open portfolio positions")
    parser.add_argument("--portfolio-max-entries-per-day", type=int, default=1, help="Maximum new portfolio entries per trading day")
    parser.add_argument("--portfolio-position-size-pct", type=float, default=0.20, help="Capital fraction allocated to each accepted position")
    parser.add_argument("--portfolio-cooldown-days-after-loss", type=int, default=0, help="Days to pause new entries after a realized losing position")
    parser.add_argument("--portfolio-consecutive-loss-limit", type=int, default=0, help="Consecutive realized losses required to trigger the longer cooldown; 0 disables")
    parser.add_argument("--portfolio-cooldown-days-after-consecutive-losses", type=int, default=0, help="Days to pause after the consecutive-loss limit is reached")
    parser.add_argument("--readiness-gate", action="store_true", help="Fail the command when production-readiness thresholds are not met")
    parser.add_argument("--readiness-min-total-trades", type=int, default=50, help="Minimum backtest trades required by --readiness-gate")
    parser.add_argument("--readiness-min-portfolio-return-pct", type=float, default=20.0, help="Minimum portfolio return required by --readiness-gate")
    parser.add_argument("--readiness-min-portfolio-win-rate-pct", type=float, default=55.0, help="Minimum portfolio win rate required by --readiness-gate")
    parser.add_argument("--readiness-min-portfolio-profit-factor", type=float, default=2.0, help="Minimum portfolio profit factor required by --readiness-gate")
    parser.add_argument("--readiness-max-portfolio-drawdown-pct", type=float, default=10.0, help="Maximum portfolio drawdown allowed by --readiness-gate")
    parser.add_argument("--summary-only", action="store_true", help="Print only aggregate metrics (omit full trade list)")
    parser.add_argument("--output", help="Optional path to write the JSON result")
    return parser.parse_args()


def main() -> None:
    logging.getLogger("sqlalchemy.engine").setLevel(logging.WARNING)
    logging.getLogger("database").setLevel(logging.WARNING)
    logging.getLogger("modeling_dataset_builder").setLevel(logging.WARNING)
    logging.getLogger("historical_probability_estimator").setLevel(logging.WARNING)
    logging.getLogger("stock_screener").setLevel(logging.WARNING)
    logging.getLogger("recommendation_backtester").setLevel(logging.WARNING)
    args = parse_args()
    start_date = date.fromisoformat(args.start_date)
    end_date = date.fromisoformat(args.end_date)
    stock_codes = [code.strip().upper() for code in args.stocks.split(",") if code.strip()] or None
    warning_capture = WarningCaptureHandler()
    captured_loggers = [
        logging.getLogger("stock_screener"),
        logging.getLogger("recommendation_backtester"),
    ]
    if args.readiness_gate:
        for logger in captured_loggers:
            logger.addHandler(warning_capture)

    db = get_db()
    try:
        db.engine.echo = False
        max_abs_gross_return_pct = (
            None if args.max_abs_gross_return_pct < 0 else args.max_abs_gross_return_pct
        )
        if args.disable_probability and args.min_predicted_probability is not None:
            raise SystemExit("--disable-probability cannot be combined with --min-predicted-probability")

        guard_filter = TechnicalGuardRecommendationFilter(
            TechnicalGuardConfig(
                min_drawdown_20d_pct=args.min_drawdown_20d_pct,
                max_price_change_20d_pct=args.max_price_change_20d_pct,
                reject_near_20d_high_pct=args.reject_near_20d_high_pct,
                reject_near_20d_high_rsi_min=args.reject_near_20d_high_rsi_min,
                reject_near_20d_high_price_change_10d_min=(
                    args.reject_near_20d_high_price_change_10d_min
                ),
            )
        )

        with db.get_session() as session:
            backtester = RecommendationBacktester(
                session=session,
                strategy_profile=args.strategy_profile,
                round_trip_cost_pct=args.round_trip_cost_pct,
                max_abs_gross_return_pct=max_abs_gross_return_pct,
                calculate_probability=not args.disable_probability,
                recommendation_filter=guard_filter if guard_filter.enabled else None,
            )
            result = backtester.run(
                start_date=start_date,
                end_date=end_date,
                horizon_days=args.horizon_days,
                stock_codes=stock_codes,
                min_score=args.min_score,
                min_confidence=args.min_confidence,
                min_predicted_probability=args.min_predicted_probability,
                include_hold=args.include_hold,
                top_n_per_day=args.top_n_per_day,
                avoid_overlapping_positions=args.avoid_overlap,
            )

        payload = result.to_dict()
        if guard_filter.enabled:
            payload["technical_guard"] = {
                "config": guard_filter.config.__dict__,
                "stats": guard_filter.stats.to_dict(),
            }
        if args.simulate_portfolio:
            portfolio_config = PortfolioSimulationConfig(
                initial_capital=args.portfolio_initial_capital,
                max_concurrent_positions=args.portfolio_max_concurrent_positions,
                max_entries_per_day=args.portfolio_max_entries_per_day,
                position_size_pct=args.portfolio_position_size_pct,
                cooldown_days_after_loss=args.portfolio_cooldown_days_after_loss,
                consecutive_loss_limit=args.portfolio_consecutive_loss_limit,
                cooldown_days_after_consecutive_losses=(
                    args.portfolio_cooldown_days_after_consecutive_losses
                ),
            )
            portfolio_result = PortfolioSimulator(portfolio_config).simulate(result.trades)
            payload["portfolio"] = portfolio_result.to_dict()
        if args.readiness_gate:
            payload["runtime_warnings"] = warning_capture.records
            payload["readiness_gate"] = _build_readiness_gate(payload, args)
        if args.summary_only:
            payload.pop("trades", None)
            if "portfolio" in payload:
                payload["portfolio"].pop("accepted_positions", None)
                payload["portfolio"].pop("equity_curve", None)
        output = json.dumps(payload, indent=2, default=str, allow_nan=False)
        if args.output:
            Path(args.output).write_text(output + "\n", encoding="utf-8")
        else:
            print(output)
        if args.readiness_gate and payload["readiness_gate"]["status"] != "PASS":
            raise SystemExit(2)
    finally:
        if args.readiness_gate:
            for logger in captured_loggers:
                logger.removeHandler(warning_capture)


def _build_readiness_gate(payload: dict, args: argparse.Namespace) -> dict:
    """Build a strict, explicit production-readiness decision."""
    portfolio = payload.get("portfolio")
    runtime_warnings = payload.get("runtime_warnings", [])
    checks = [
        {
            "name": "minimum_total_trades",
            "actual": payload.get("total_trades"),
            "expected": f">= {args.readiness_min_total_trades}",
            "passed": _minimum_passes(
                payload.get("total_trades"),
                args.readiness_min_total_trades,
            ),
        }
    ]
    checks.append(
        {
            "name": "runtime_warnings_absent",
            "actual": len(runtime_warnings),
            "expected": "0",
            "passed": len(runtime_warnings) == 0,
        }
    )

    if portfolio is None:
        checks.append(
            {
                "name": "portfolio_metrics_present",
                "actual": None,
                "expected": "run with --simulate-portfolio",
                "passed": False,
            }
        )
    else:
        checks.extend(
            [
                {
                    "name": "minimum_portfolio_return_pct",
                    "actual": portfolio.get("total_return_pct"),
                    "expected": f">= {args.readiness_min_portfolio_return_pct}",
                    "passed": _minimum_passes(
                        portfolio.get("total_return_pct"),
                        args.readiness_min_portfolio_return_pct,
                    ),
                },
                {
                    "name": "minimum_portfolio_win_rate_pct",
                    "actual": portfolio.get("win_rate_pct"),
                    "expected": f">= {args.readiness_min_portfolio_win_rate_pct}",
                    "passed": _minimum_passes(
                        portfolio.get("win_rate_pct"),
                        args.readiness_min_portfolio_win_rate_pct,
                    ),
                },
                {
                    "name": "minimum_portfolio_profit_factor",
                    "actual": portfolio.get("profit_factor"),
                    "expected": f">= {args.readiness_min_portfolio_profit_factor}",
                    "passed": _minimum_passes(
                        portfolio.get("profit_factor"),
                        args.readiness_min_portfolio_profit_factor,
                    ),
                },
                {
                    "name": "maximum_portfolio_drawdown_pct",
                    "actual": portfolio.get("max_drawdown_pct"),
                    "expected": f"<= {args.readiness_max_portfolio_drawdown_pct}",
                    "passed": _maximum_passes(
                        portfolio.get("max_drawdown_pct"),
                        args.readiness_max_portfolio_drawdown_pct,
                    ),
                },
            ]
        )

    failed_checks = [check["name"] for check in checks if not check["passed"]]
    return {
        "status": "PASS" if not failed_checks else "FAIL",
        "failed_checks": failed_checks,
        "checks": checks,
    }


def _minimum_passes(
    value: Optional[float],
    threshold: float,
) -> bool:
    return value is not None and value >= threshold


def _maximum_passes(
    value: Optional[float],
    threshold: float,
) -> bool:
    return value is not None and value <= threshold


def _optional_minimum_passes(
    value: Optional[float],
    threshold: Optional[float],
) -> bool:
    if threshold is None:
        return True
    return value is not None and value >= threshold


if __name__ == "__main__":
    main()
