#!/usr/bin/env python3
"""
Run weekly backtest for the steady profile and persist results for dashboards.
"""

from __future__ import annotations

from datetime import date, timedelta
import argparse
import logging
from typing import Any, List, Optional

from app.config.database import get_db
from app.models import Base, BacktestRun, BacktestTrade, RecommendationSnapshot, DecisionSignal
from app.services.backtesting import RecommendationBacktester
from app.services.advisory.advisor import RecommendationProfile, StockScreener


def _as_float(value):
    return float(value) if value is not None else None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Weekly backtest + recommendation snapshot")
    parser.add_argument("--smoke", action="store_true", help="Run a lightweight smoke test on a small stock subset")
    parser.add_argument("--stocks", default="", help="Comma-separated stock codes to limit the run")
    parser.add_argument("--min-score", type=float, default=60.0, help="Minimum score for backtest screening")
    parser.add_argument("--min-confidence", type=float, default=0.70, help="Minimum confidence for backtest screening")
    parser.add_argument("--lookback-runs", type=int, default=4, help="Number of comparable weekly runs to aggregate")
    parser.add_argument("--min-trades", type=int, default=80, help="Minimum trades required for a run to be included in decision signal")
    return parser.parse_args()


def _to_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "y"}
    if isinstance(value, (int, float)):
        return bool(value)
    return False


def _is_comparable_run(
    run: BacktestRun,
    profile: str,
    horizon_days: int,
    min_score: Optional[float],
    min_confidence: Optional[float],
    min_trades: int,
) -> bool:
    if run.profile != profile:
        return False
    if int(run.horizon_days) != int(horizon_days):
        return False
    if int(run.total_trades) < int(min_trades):
        return False

    metadata = run.run_metadata or {}
    if _to_bool(metadata.get("smoke", False)):
        return False

    # If thresholds are recorded in metadata, enforce strict comparability.
    run_min_score = metadata.get("min_score")
    run_min_conf = metadata.get("min_confidence")
    if run_min_score is not None and min_score is not None and float(run_min_score) != float(min_score):
        return False
    if run_min_conf is not None and min_confidence is not None and float(run_min_conf) != float(min_confidence):
        return False
    return True


def _select_recent_comparable_runs(
    session,
    *,
    profile: str,
    horizon_days: int,
    min_score: Optional[float],
    min_confidence: Optional[float],
    min_trades: int,
    lookback_runs: int,
) -> List[BacktestRun]:
    candidates = (
        session.query(BacktestRun)
        .filter(BacktestRun.profile == profile)
        .order_by(BacktestRun.run_date.desc(), BacktestRun.created_at.desc(), BacktestRun.run_id.desc())
        .all()
    )

    selected: List[BacktestRun] = []
    seen_run_dates = set()

    for run in candidates:
        if run.run_date in seen_run_dates:
            continue
        if not _is_comparable_run(
            run=run,
            profile=profile,
            horizon_days=horizon_days,
            min_score=min_score,
            min_confidence=min_confidence,
            min_trades=min_trades,
        ):
            continue
        selected.append(run)
        seen_run_dates.add(run.run_date)
        if len(selected) >= lookback_runs:
            break
    return selected


def main() -> None:
    logging.getLogger("sqlalchemy.engine").setLevel(logging.WARNING)
    logging.getLogger("stock_screener").setLevel(logging.WARNING)
    args = parse_args()
    run_date = date.today()
    start_date = run_date - timedelta(days=30 if args.smoke else 183)
    end_date = run_date
    horizon_days = 5 if args.smoke else 10
    profile = RecommendationProfile.STEADY_20P_10D.value
    stock_codes = [code.strip().upper() for code in args.stocks.split(",") if code.strip()] or None
    if args.smoke and not stock_codes:
        stock_codes = ["GTCO", "ZENITHBANK", "CADBURY", "STANBIC", "WAPCO"]

    db = get_db()
    Base.metadata.create_all(db.engine)
    with db.get_session() as session:
        backtester = RecommendationBacktester(
            session=session,
            strategy_profile=profile,
            round_trip_cost_pct=0.20,
        )
        result = backtester.run(
            start_date=start_date,
            end_date=end_date,
            horizon_days=horizon_days,
            stock_codes=stock_codes,
            min_score=args.min_score,
            min_confidence=args.min_confidence,
            include_hold=False,
        )

        run = BacktestRun(
            run_date=run_date,
            start_date=start_date,
            end_date=end_date,
            horizon_days=horizon_days,
            profile=profile,
            total_trades=result.total_trades,
            win_rate_pct=round(result.win_rate_pct, 2),
            average_return_pct=round(result.average_return_pct, 2),
            average_win_pct=round(result.average_win_pct, 2),
            average_loss_pct=round(result.average_loss_pct, 2),
            profit_factor=round(result.profit_factor, 4),
            directional_accuracy_pct=round(result.directional_accuracy_pct, 2),
            max_drawdown_pct=round(result.max_drawdown_pct, 2),
            run_metadata={
                "wins": result.wins,
                "losses": result.losses,
                "smoke": args.smoke,
                "stock_codes": stock_codes or "ALL",
                "min_score": args.min_score,
                "min_confidence": args.min_confidence,
            },
        )
        session.add(run)
        session.flush()

        trades: List[BacktestTrade] = []
        for trade in result.trades:
            trades.append(
                BacktestTrade(
                    run_id=run.run_id,
                    stock_code=trade.stock_code,
                    entry_date=trade.entry_date,
                    exit_date=trade.exit_date,
                    signal_type=trade.signal_type,
                    confidence=trade.confidence,
                    score=trade.score,
                    entry_price=trade.entry_price,
                    exit_price=trade.exit_price,
                    gross_return_pct=trade.gross_return_pct,
                    net_return_pct=trade.net_return_pct,
                    correct_direction=trade.correct_direction,
                )
            )
        session.add_all(trades)

        screener = StockScreener(session, strategy_profile=profile)
        recommendations = screener.generate_recommendations(
            recommendation_date=end_date,
            strategy_profile=profile,
        )

        recommendations.sort(key=lambda rec: float(rec.score), reverse=True)
        snapshots: List[RecommendationSnapshot] = []
        for rec in recommendations[:10]:
            snapshots.append(
                RecommendationSnapshot(
                    run_id=run.run_id,
                    snapshot_date=end_date,
                    profile=profile,
                    stock_code=rec.stock_code,
                    company_name=rec.stock_name,
                    signal_type=rec.signal_type.value,
                    confidence=_as_float(rec.confidence),
                    score=_as_float(rec.score),
                    current_price=_as_float(rec.current_price),
                    target_price=_as_float(rec.target_price),
                    stop_loss=_as_float(rec.stop_loss),
                    reasons=rec.reasons,
                )
            )
        session.add_all(snapshots)

        recent_runs = _select_recent_comparable_runs(
            session,
            profile=profile,
            horizon_days=horizon_days,
            min_score=args.min_score,
            min_confidence=args.min_confidence,
            min_trades=args.min_trades,
            lookback_runs=args.lookback_runs,
        )
        if recent_runs:
            session.query(DecisionSignal).filter(
                DecisionSignal.run_date == run_date,
                DecisionSignal.profile == profile,
            ).delete(synchronize_session=False)

            avg_win_rate = sum(float(run.win_rate_pct) for run in recent_runs) / len(recent_runs)
            avg_return = sum(float(run.average_return_pct) for run in recent_runs) / len(recent_runs)
            avg_pf = sum(float(run.profit_factor) for run in recent_runs) / len(recent_runs)
            max_dd = max(float(run.max_drawdown_pct) for run in recent_runs)

            if avg_win_rate >= 45 and avg_return > 0 and avg_pf >= 1.2 and max_dd <= 25:
                status = "GREEN"
                rationale = ["Win rate >= 45%", "Average return > 0", "Profit factor >= 1.2", "Max drawdown <= 25%"]
            elif avg_win_rate >= 35 and avg_return > -2 and avg_pf >= 0.9 and max_dd <= 35:
                status = "YELLOW"
                rationale = ["Win rate >= 35%", "Average return not severely negative", "Profit factor >= 0.9", "Max drawdown <= 35%"]
            else:
                status = "RED"
                rationale = [
                    "Low win rate or return",
                    "Profit factor weak or drawdown high",
                    f"Comparable runs only (n={len(recent_runs)}, min_trades={args.min_trades}, min_score={args.min_score}, min_confidence={args.min_confidence})",
                ]

            session.add(
                DecisionSignal(
                    run_date=run_date,
                    profile=profile,
                    status=status,
                    win_rate_pct=round(avg_win_rate, 2),
                    average_return_pct=round(avg_return, 2),
                    profit_factor=round(avg_pf, 4),
                    max_drawdown_pct=round(max_dd, 2),
                    lookback_runs=len(recent_runs),
                    rationale=rationale,
                )
            )

    print(
        {
            "run_date": run_date.isoformat(),
            "profile": profile,
            "total_trades": result.total_trades,
            "snapshots": len(recommendations[:10]),
            "smoke": args.smoke,
            "min_score": args.min_score,
            "min_confidence": args.min_confidence,
            "lookback_runs": args.lookback_runs,
            "min_trades": args.min_trades,
        }
    )


if __name__ == "__main__":
    main()
