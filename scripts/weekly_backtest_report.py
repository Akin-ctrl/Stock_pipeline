#!/usr/bin/env python3
"""
Run weekly backtest for the steady profile and persist results for dashboards.
"""

from __future__ import annotations

from collections import defaultdict
from datetime import date, datetime, timedelta
import argparse
import logging
from math import isinf
from typing import Any, List, Optional

from app.config.database import get_db
from sqlalchemy import func

from app.models import (
    Base,
    BacktestPortfolioEquityPoint,
    BacktestPortfolioPosition,
    BacktestRun,
    BacktestSectorPerformance,
    BacktestStockPerformance,
    BacktestTrade,
    BacktestYearlyPerformance,
    DecisionSignal,
    DimSector,
    DimStock,
    FactDailyPrice,
    RecommendationSnapshot,
)
from app.services.backtesting import (
    PortfolioSimulationConfig,
    PortfolioSimulator,
    RecommendationBacktester,
)
from app.services.advisory.advisor import RecommendationProfile, StockScreener
from app.services.modeling import (
    HistoricalLogisticProbabilityEstimator,
    ModelingDatasetBuilder,
    ModelingDatasetConfig,
    NullProbabilityEstimator,
)


PROBABILITY_TRAINING_WINDOW_DAYS = 540


def _as_float(value):
    return float(value) if value is not None else None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Weekly backtest + recommendation snapshot")
    parser.add_argument("--smoke", action="store_true", help="Run a lightweight smoke test on a small stock subset")
    parser.add_argument("--stocks", default="", help="Comma-separated stock codes to limit the run")
    parser.add_argument("--min-score", type=float, default=70.0, help="Legacy minimum heuristic score for backtest screening")
    parser.add_argument("--min-confidence", type=float, default=0.60, help="Legacy minimum signal agreement for backtest screening")
    parser.add_argument(
        "--min-predicted-probability",
        type=float,
        default=None,
        help="Minimum predicted 10-day up probability for backtest screening; use 0 to disable",
    )
    parser.add_argument("--max-abs-gross-return-pct", type=float, default=50.0, help="Exclude trades with absolute gross return above this percent; use a negative value to disable")
    parser.add_argument("--lookback-runs", type=int, default=4, help="Number of comparable weekly runs to aggregate")
    parser.add_argument("--min-trades", type=int, default=20, help="Minimum trades required for a run to be included in decision signal")
    parser.add_argument("--full-validation", action="store_true", help="Persist a full dashboard-ready validation run")
    parser.add_argument("--start-date", default="", help="Backtest start date, YYYY-MM-DD. Defaults to 2020-01-01 for full validation.")
    parser.add_argument("--end-date", default="", help="Backtest end date, YYYY-MM-DD. Defaults to latest stored price date for full validation.")
    parser.add_argument("--run-date", default="", help="Run date to persist, YYYY-MM-DD. Defaults to today.")
    parser.add_argument("--disable-probability", action="store_true", help="Disable diagnostic probability estimation when no probability gate is being tested")
    return parser.parse_args()


def _parse_date(value: str, *, field_name: str) -> Optional[date]:
    if not value:
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError as exc:
        raise ValueError(f"{field_name} must be YYYY-MM-DD, got {value!r}") from exc


def _latest_price_date(session) -> date:
    latest = session.query(func.max(FactDailyPrice.price_date)).scalar()
    if latest is None:
        raise RuntimeError("Cannot run validation: fact_daily_prices is empty.")
    return latest


def _build_probability_estimator(session, *, start_date: date, end_date: date):
    """Preload model rows once while preserving rolling training semantics."""
    dataset_builder = ModelingDatasetBuilder(
        session,
        config=ModelingDatasetConfig(min_confidence_score=60.0),
    )
    preloaded_rows = dataset_builder.build(
        start_date=start_date - timedelta(days=PROBABILITY_TRAINING_WINDOW_DAYS),
        end_date=end_date,
    )
    return HistoricalLogisticProbabilityEstimator(
        session,
        training_window_days=PROBABILITY_TRAINING_WINDOW_DAYS,
        dataset_builder=dataset_builder,
        preloaded_rows=preloaded_rows,
    )


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
    run_type: str,
    probability_enabled: bool,
    min_score: Optional[float],
    min_confidence: Optional[float],
    min_predicted_probability: Optional[float],
    max_abs_gross_return_pct: Optional[float],
    min_trades: int,
) -> bool:
    if run.profile != profile:
        return False
    if int(run.horizon_days) != int(horizon_days):
        return False
    existing_run_type = getattr(run, "run_type", None) or (
        (run.run_metadata or {}).get("run_type", "weekly_validation")
    )
    if existing_run_type != run_type:
        return False
    if int(run.total_trades) < int(min_trades):
        return False

    metadata = run.run_metadata or {}
    if _to_bool(metadata.get("smoke", False)):
        return False
    if _to_bool(metadata.get("probability_enabled", True)) != probability_enabled:
        return False

    # If thresholds are recorded in metadata, enforce strict comparability.
    run_min_score = metadata.get("min_score")
    run_min_conf = metadata.get("min_confidence")
    run_min_probability = metadata.get("min_predicted_probability")
    run_max_abs_return = metadata.get("max_abs_gross_return_pct")
    if run_min_score is not None and min_score is not None and float(run_min_score) != float(min_score):
        return False
    if run_min_conf is not None and min_confidence is not None and float(run_min_conf) != float(min_confidence):
        return False
    if (
        run_min_probability is not None
        and min_predicted_probability is not None
        and float(run_min_probability) != float(min_predicted_probability)
    ):
        return False
    if run_max_abs_return is None and max_abs_gross_return_pct is not None:
        return False
    if run_max_abs_return is not None and max_abs_gross_return_pct is None:
        return False
    if (
        run_max_abs_return is not None
        and max_abs_gross_return_pct is not None
        and float(run_max_abs_return) != float(max_abs_gross_return_pct)
    ):
        return False
    return True


def _select_recent_comparable_runs(
    session,
    *,
    profile: str,
    horizon_days: int,
    run_type: str,
    probability_enabled: bool,
    min_score: Optional[float],
    min_confidence: Optional[float],
    min_predicted_probability: Optional[float],
    max_abs_gross_return_pct: Optional[float],
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
            run_type=run_type,
            probability_enabled=probability_enabled,
            min_score=min_score,
            min_confidence=min_confidence,
            min_predicted_probability=min_predicted_probability,
            max_abs_gross_return_pct=max_abs_gross_return_pct,
            min_trades=min_trades,
        ):
            continue
        selected.append(run)
        seen_run_dates.add(run.run_date)
        if len(selected) >= lookback_runs:
            break
    return selected


def _run_metadata_matches(
    run: BacktestRun,
    *,
    run_type: str = "weekly_validation",
    smoke: bool,
    stock_codes: Optional[List[str]],
    probability_enabled: bool = True,
    min_score: Optional[float],
    min_confidence: Optional[float],
    min_predicted_probability: Optional[float],
    max_abs_gross_return_pct: Optional[float],
) -> bool:
    metadata = run.run_metadata or {}
    existing_run_type = getattr(run, "run_type", None) or metadata.get("run_type", "weekly_validation")
    if existing_run_type != run_type:
        return False
    if _to_bool(metadata.get("smoke", False)) != bool(smoke):
        return False
    if _to_bool(metadata.get("probability_enabled", True)) != probability_enabled:
        return False

    existing_stocks = metadata.get("stock_codes", "ALL")
    expected_stocks = stock_codes or "ALL"
    if existing_stocks != expected_stocks:
        return False

    comparisons = (
        ("min_score", metadata.get("min_score"), min_score),
        ("min_confidence", metadata.get("min_confidence"), min_confidence),
        (
            "min_predicted_probability",
            metadata.get("min_predicted_probability"),
            min_predicted_probability,
        ),
        (
            "max_abs_gross_return_pct",
            metadata.get("max_abs_gross_return_pct"),
            max_abs_gross_return_pct,
        ),
    )
    for _, existing_value, expected_value in comparisons:
        if existing_value is None and expected_value is None:
            continue
        if existing_value is None or expected_value is None:
            return False
        if float(existing_value) != float(expected_value):
            return False
    return True


def _replace_equivalent_run(
    session,
    *,
    run_date: date,
    start_date: date,
    end_date: date,
    horizon_days: int,
    profile: str,
    run_type: str = "weekly_validation",
    smoke: bool,
    stock_codes: Optional[List[str]],
    probability_enabled: bool = True,
    min_score: Optional[float],
    min_confidence: Optional[float],
    min_predicted_probability: Optional[float],
    max_abs_gross_return_pct: Optional[float],
) -> int:
    """Delete an equivalent persisted run so reruns remain idempotent."""
    candidates = (
        session.query(BacktestRun)
        .filter(
            BacktestRun.run_date == run_date,
            BacktestRun.start_date == start_date,
            BacktestRun.end_date == end_date,
            BacktestRun.horizon_days == horizon_days,
            BacktestRun.profile == profile,
        )
        .all()
    )

    deleted = 0
    for run in candidates:
        if not _run_metadata_matches(
            run,
            run_type=run_type,
            smoke=smoke,
            stock_codes=stock_codes,
            probability_enabled=probability_enabled,
            min_score=min_score,
            min_confidence=min_confidence,
            min_predicted_probability=min_predicted_probability,
            max_abs_gross_return_pct=max_abs_gross_return_pct,
        ):
            continue
        session.delete(run)
        deleted += 1

    if deleted:
        session.flush()
    return deleted


def _json_profit_factor(profit_factor: float) -> float | None:
    if isinf(profit_factor):
        return None
    return round(profit_factor, 4)


def _profit_factor_from_pnl(values: list[float]) -> float | None:
    gross_profit = sum(value for value in values if value > 0)
    gross_loss = abs(sum(value for value in values if value < 0))
    if gross_loss > 0:
        return round(gross_profit / gross_loss, 4)
    return None if gross_profit > 0 else 0.0


def _sector_by_stock_code(session) -> dict[str, str]:
    rows = (
        session.query(DimStock.stock_code, DimSector.sector_name)
        .outerjoin(DimSector, DimSector.sector_id == DimStock.sector_id)
        .all()
    )
    return {
        stock_code.upper(): sector_name or "Unclassified"
        for stock_code, sector_name in rows
    }


def _trade_lookup(trades) -> dict[tuple[str, date, date], list[Any]]:
    lookup: dict[tuple[str, date, date], list[Any]] = defaultdict(list)
    for trade in trades:
        lookup[(trade.stock_code.upper(), trade.entry_date, trade.exit_date)].append(trade)
    return lookup


def _build_portfolio_position_rows(
    *,
    run_id: int,
    portfolio_result,
    trades,
    sector_by_stock: dict[str, str],
) -> list[BacktestPortfolioPosition]:
    lookup = _trade_lookup(trades)
    rows: list[BacktestPortfolioPosition] = []
    for position in portfolio_result.accepted_positions:
        key = (
            position.stock_code.upper(),
            position.entry_date,
            position.exit_date,
        )
        matching_trades = lookup.get(key, [])
        trade = matching_trades.pop(0) if matching_trades else None
        if trade is None:
            continue

        rows.append(
            BacktestPortfolioPosition(
                run_id=run_id,
                stock_code=position.stock_code,
                sector_name=sector_by_stock.get(position.stock_code.upper(), "Unclassified"),
                entry_date=position.entry_date,
                exit_date=position.exit_date,
                holding_days=(position.exit_date - position.entry_date).days,
                signal_type=trade.action_type,
                confidence=trade.confidence,
                score=trade.score,
                predicted_probability_10d_up=trade.predicted_probability_10d_up,
                entry_price=trade.entry_price,
                exit_price=trade.exit_price,
                allocated_capital=position.allocated_capital,
                net_return_pct=position.net_return_pct,
                realized_pnl=position.realized_pnl,
                exit_value=position.exit_value,
                was_winner=position.realized_pnl > 0,
            )
        )
    return rows


def _build_portfolio_equity_rows(
    *,
    run_id: int,
    portfolio_result,
) -> list[BacktestPortfolioEquityPoint]:
    return [
        BacktestPortfolioEquityPoint(
            run_id=run_id,
            point_index=index,
            event_date=point.event_date,
            cash=point.cash,
            open_position_capital=point.open_position_capital,
            equity=point.equity,
            drawdown_pct=point.drawdown_pct,
            open_positions=point.open_positions,
        )
        for index, point in enumerate(portfolio_result.equity_curve, start=1)
    ]


def _equity_at_or_before(points, target_date: date, default: float) -> float:
    equity = default
    for point in sorted(points, key=lambda item: item.event_date):
        if point.event_date > target_date:
            break
        equity = point.equity
    return equity


def _build_yearly_rows(
    *,
    run_id: int,
    portfolio_result,
) -> list[BacktestYearlyPerformance]:
    if portfolio_result.start_date is None or portfolio_result.end_date is None:
        return []

    rows: list[BacktestYearlyPerformance] = []
    positions = portfolio_result.accepted_positions
    points = portfolio_result.equity_curve
    initial = portfolio_result.config.initial_capital
    for year in range(portfolio_result.start_date.year, portfolio_result.end_date.year + 1):
        year_start = date(year, 1, 1)
        year_end = date(year, 12, 31)
        year_positions = [
            position for position in positions if position.entry_date.year == year
        ]
        returns = [position.net_return_pct for position in year_positions]
        pnl_values = [position.realized_pnl for position in year_positions]
        wins = [position for position in year_positions if position.realized_pnl > 0]
        starting_equity = _equity_at_or_before(
            points,
            year_start - timedelta(days=1),
            initial,
        )
        ending_equity = _equity_at_or_before(points, year_end, starting_equity)
        year_points = [
            point for point in points if year_start <= point.event_date <= year_end
        ]
        trade_count = len(year_positions)
        rows.append(
            BacktestYearlyPerformance(
                run_id=run_id,
                calendar_year=year,
                trade_count=trade_count,
                win_rate_pct=round(len(wins) / trade_count * 100.0, 2) if trade_count else 0.0,
                average_return_pct=round(sum(returns) / trade_count, 2) if trade_count else 0.0,
                profit_factor=_profit_factor_from_pnl(pnl_values),
                portfolio_return_pct=round(
                    ((ending_equity - starting_equity) / starting_equity * 100.0)
                    if starting_equity
                    else 0.0,
                    2,
                ),
                portfolio_max_drawdown_pct=round(
                    max((point.drawdown_pct for point in year_points), default=0.0),
                    2,
                ),
                portfolio_win_rate_pct=round(len(wins) / trade_count * 100.0, 2) if trade_count else 0.0,
                portfolio_profit_factor=_profit_factor_from_pnl(pnl_values),
                starting_equity=round(starting_equity, 4),
                ending_equity=round(ending_equity, 4),
            )
        )
    return rows


def _build_stock_rows(
    *,
    run_id: int,
    portfolio_result,
    sector_by_stock: dict[str, str],
) -> list[BacktestStockPerformance]:
    grouped: dict[str, list[Any]] = defaultdict(list)
    for position in portfolio_result.accepted_positions:
        grouped[position.stock_code.upper()].append(position)

    rows: list[BacktestStockPerformance] = []
    for stock_code, positions in sorted(grouped.items()):
        returns = [position.net_return_pct for position in positions]
        pnl_values = [position.realized_pnl for position in positions]
        wins = [position for position in positions if position.realized_pnl > 0]
        rows.append(
            BacktestStockPerformance(
                run_id=run_id,
                stock_code=stock_code,
                sector_name=sector_by_stock.get(stock_code, "Unclassified"),
                trade_count=len(positions),
                win_rate_pct=round(len(wins) / len(positions) * 100.0, 2),
                average_return_pct=round(sum(returns) / len(positions), 2),
                total_realized_pnl=round(sum(pnl_values), 4),
                best_trade_pct=round(max(returns), 4),
                worst_trade_pct=round(min(returns), 4),
                profit_factor=_profit_factor_from_pnl(pnl_values),
            )
        )
    return rows


def _build_sector_rows(
    *,
    run_id: int,
    portfolio_result,
    sector_by_stock: dict[str, str],
) -> list[BacktestSectorPerformance]:
    grouped: dict[str, list[Any]] = defaultdict(list)
    for position in portfolio_result.accepted_positions:
        sector = sector_by_stock.get(position.stock_code.upper(), "Unclassified")
        grouped[sector].append(position)

    rows: list[BacktestSectorPerformance] = []
    for sector_name, positions in sorted(grouped.items()):
        returns = [position.net_return_pct for position in positions]
        pnl_values = [position.realized_pnl for position in positions]
        wins = [position for position in positions if position.realized_pnl > 0]
        rows.append(
            BacktestSectorPerformance(
                run_id=run_id,
                sector_name=sector_name,
                trade_count=len(positions),
                win_rate_pct=round(len(wins) / len(positions) * 100.0, 2),
                average_return_pct=round(sum(returns) / len(positions), 2),
                total_realized_pnl=round(sum(pnl_values), 4),
                profit_factor=_profit_factor_from_pnl(pnl_values),
            )
        )
    return rows


def main() -> None:
    logging.getLogger("sqlalchemy.engine").setLevel(logging.WARNING)
    logging.getLogger("stock_screener").setLevel(logging.WARNING)
    args = parse_args()
    run_date = _parse_date(args.run_date, field_name="run-date") or date.today()
    horizon_days = 5 if args.smoke else 10
    profile = RecommendationProfile.STEADY_20P_10D.value
    run_type = "full_validation" if args.full_validation else "weekly_validation"
    stock_codes = [code.strip().upper() for code in args.stocks.split(",") if code.strip()] or None
    max_abs_gross_return_pct = (
        None if args.max_abs_gross_return_pct < 0 else args.max_abs_gross_return_pct
    )
    if args.smoke and not stock_codes:
        stock_codes = ["GTCO", "ZENITHBANK", "CADBURY", "STANBIC", "WAPCO"]
    if args.disable_probability and args.min_predicted_probability is not None:
        raise SystemExit("--disable-probability cannot be combined with --min-predicted-probability")

    db = get_db()
    db.engine.echo = False
    Base.metadata.create_all(db.engine)
    with db.get_session() as session:
        requested_start_date = _parse_date(args.start_date, field_name="start-date")
        requested_end_date = _parse_date(args.end_date, field_name="end-date")
        if args.full_validation:
            start_date = requested_start_date or date(2020, 1, 1)
            end_date = requested_end_date or _latest_price_date(session)
        else:
            end_date = requested_end_date or run_date
            start_date = requested_start_date or (
                end_date - timedelta(days=30 if args.smoke else 183)
            )
        if start_date > end_date:
            raise ValueError(
                f"Invalid validation window: {start_date} is after {end_date}."
            )
        probability_estimator = None
        if not args.disable_probability:
            probability_estimator = _build_probability_estimator(
                session,
                start_date=start_date,
                end_date=end_date,
            )

        _replace_equivalent_run(
            session,
            run_date=run_date,
            start_date=start_date,
            end_date=end_date,
            horizon_days=horizon_days,
            profile=profile,
            run_type=run_type,
            smoke=args.smoke,
            stock_codes=stock_codes,
            probability_enabled=not args.disable_probability,
            min_score=args.min_score,
            min_confidence=args.min_confidence,
            min_predicted_probability=args.min_predicted_probability,
            max_abs_gross_return_pct=max_abs_gross_return_pct,
        )

        backtester = RecommendationBacktester(
            session=session,
            strategy_profile=profile,
            round_trip_cost_pct=0.20,
            max_abs_gross_return_pct=max_abs_gross_return_pct,
            calculate_probability=not args.disable_probability,
            probability_estimator=probability_estimator,
        )
        result = backtester.run(
            start_date=start_date,
            end_date=end_date,
            horizon_days=horizon_days,
            stock_codes=stock_codes,
            min_score=args.min_score,
            min_confidence=args.min_confidence,
            min_predicted_probability=args.min_predicted_probability,
            include_hold=False,
            top_n_per_day=1,
            avoid_overlapping_positions=True,
        )
        portfolio_config = PortfolioSimulationConfig(
            initial_capital=1_000_000.0,
            max_concurrent_positions=3,
            max_entries_per_day=1,
            position_size_pct=0.20,
        )
        portfolio_result = PortfolioSimulator(portfolio_config).simulate(result.trades)
        portfolio_payload = portfolio_result.to_dict()
        portfolio_payload.pop("accepted_positions", None)
        portfolio_payload.pop("equity_curve", None)

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
            run_type=run_type,
            run_metadata={
                "wins": result.wins,
                "losses": result.losses,
                "run_type": run_type,
                "smoke": args.smoke,
                "stock_codes": stock_codes or "ALL",
                "min_score": args.min_score,
                "min_confidence": args.min_confidence,
                "min_predicted_probability": args.min_predicted_probability,
                "probability_enabled": not args.disable_probability,
                "max_abs_gross_return_pct": max_abs_gross_return_pct,
                "top_n_per_day": 1,
                "avoid_overlapping_positions": True,
                "portfolio": portfolio_payload,
            },
        )
        session.add(run)
        session.flush()

        sector_by_stock = _sector_by_stock_code(session)

        trades: List[BacktestTrade] = []
        for trade in result.trades:
            trades.append(
                BacktestTrade(
                    run_id=run.run_id,
                    stock_code=trade.stock_code,
                    entry_date=trade.entry_date,
                    exit_date=trade.exit_date,
                    signal_type=trade.action_type,
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
        session.add_all(
            _build_portfolio_position_rows(
                run_id=run.run_id,
                portfolio_result=portfolio_result,
                trades=result.trades,
                sector_by_stock=sector_by_stock,
            )
        )
        session.add_all(
            _build_portfolio_equity_rows(
                run_id=run.run_id,
                portfolio_result=portfolio_result,
            )
        )
        session.add_all(
            _build_yearly_rows(
                run_id=run.run_id,
                portfolio_result=portfolio_result,
            )
        )
        session.add_all(
            _build_stock_rows(
                run_id=run.run_id,
                portfolio_result=portfolio_result,
                sector_by_stock=sector_by_stock,
            )
        )
        session.add_all(
            _build_sector_rows(
                run_id=run.run_id,
                portfolio_result=portfolio_result,
                sector_by_stock=sector_by_stock,
            )
        )

        screener = StockScreener(
            session,
            strategy_profile=profile,
            probability_estimator=probability_estimator,
        )
        if args.disable_probability:
            screener.probability_estimator = NullProbabilityEstimator()
        recommendations = screener.generate_recommendations(
            recommendation_date=end_date,
            strategy_profile=profile,
            stock_codes=stock_codes,
        )
        snapshots: List[RecommendationSnapshot] = []
        for rec in recommendations[:10]:
            snapshots.append(
                RecommendationSnapshot(
                    run_id=run.run_id,
                    snapshot_date=end_date,
                    profile=profile,
                    stock_code=rec.stock_code,
                    company_name=rec.stock_name,
                    signal_type=rec.action_type.value,
                    confidence=_as_float(rec.confidence),
                    score=_as_float(rec.heuristic_score),
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
            run_type=run_type,
            probability_enabled=not args.disable_probability,
            min_score=args.min_score,
            min_confidence=args.min_confidence,
            min_predicted_probability=args.min_predicted_probability,
            max_abs_gross_return_pct=max_abs_gross_return_pct,
            min_trades=args.min_trades,
            lookback_runs=args.lookback_runs,
        )
        if recent_runs:
            session.query(DecisionSignal).filter(
                DecisionSignal.run_date == run_date,
                DecisionSignal.profile == profile,
                DecisionSignal.run_type == run_type,
            ).delete(synchronize_session=False)

            portfolio_runs = [
                run.run_metadata.get("portfolio")
                for run in recent_runs
                if run.run_metadata and run.run_metadata.get("portfolio")
            ]
            if portfolio_runs:
                avg_win_rate = sum(
                    float(run["win_rate_pct"])
                    for run in portfolio_runs
                ) / len(portfolio_runs)
                avg_return = sum(
                    float(run["total_return_pct"])
                    for run in portfolio_runs
                ) / len(portfolio_runs)
                bounded_profit_factors = [
                    float(run["profit_factor"])
                    for run in portfolio_runs
                    if run["profit_factor"] is not None
                ]
                avg_pf = (
                    sum(bounded_profit_factors) / len(bounded_profit_factors)
                    if bounded_profit_factors
                    else 0.0
                )
                max_dd = max(
                    float(run["max_drawdown_pct"])
                    for run in portfolio_runs
                )
            else:
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
                    (
                        f"Comparable runs only (n={len(recent_runs)}, "
                        f"min_trades={args.min_trades}, min_score={args.min_score}, "
                        f"min_confidence={args.min_confidence}, "
                        f"min_predicted_probability={args.min_predicted_probability}, "
                        f"max_abs_gross_return_pct={max_abs_gross_return_pct})"
                    ),
                ]

            session.add(
                DecisionSignal(
                    run_date=run_date,
                    profile=profile,
                    run_type=run_type,
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
                    "run_type": run_type,
                    "start_date": start_date.isoformat(),
                    "end_date": end_date.isoformat(),
                    "profile": profile,
                    "total_trades": result.total_trades,
                    "snapshots": len(recommendations[:10]),
                    "smoke": args.smoke,
                    "min_score": args.min_score,
                    "min_confidence": args.min_confidence,
                    "min_predicted_probability": args.min_predicted_probability,
                    "probability_enabled": not args.disable_probability,
                    "max_abs_gross_return_pct": max_abs_gross_return_pct,
                    "lookback_runs": args.lookback_runs,
                    "min_trades": args.min_trades,
                    "portfolio": portfolio_payload,
        }
    )


if __name__ == "__main__":
    main()
