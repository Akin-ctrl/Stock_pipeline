"""Integration tests for weekly backtest report persistence helpers."""

from datetime import date
from decimal import Decimal

import pytest
from sqlalchemy.orm import Session

from app.models import (
    BacktestPortfolioEquityPoint,
    BacktestPortfolioPosition,
    BacktestRun,
    BacktestTrade,
    RecommendationSnapshot,
)
from scripts.weekly_backtest_report import _replace_equivalent_run


@pytest.mark.integration
@pytest.mark.database
def test_replace_equivalent_run_is_idempotent(db_session: Session):
    """Equivalent weekly report reruns should replace the same persisted run."""
    run = BacktestRun(
        run_date=date(2026, 5, 27),
        start_date=date(2026, 1, 1),
        end_date=date(2026, 5, 27),
        horizon_days=10,
        profile="steady_20p_10d",
        total_trades=12,
        win_rate_pct=Decimal("55.50"),
        average_return_pct=Decimal("3.10"),
        average_win_pct=Decimal("6.25"),
        average_loss_pct=Decimal("-2.10"),
        profit_factor=Decimal("1.4200"),
        directional_accuracy_pct=Decimal("58.30"),
        max_drawdown_pct=Decimal("8.50"),
        run_metadata={
            "wins": 7,
            "losses": 5,
            "smoke": False,
            "stock_codes": "ALL",
            "min_score": 60.0,
            "min_confidence": 0.70,
            "min_predicted_probability": None,
            "max_abs_gross_return_pct": 50.0,
        },
    )
    db_session.add(run)
    db_session.flush()

    db_session.add(
        BacktestTrade(
            run_id=run.run_id,
            stock_code="GTCO",
            entry_date=date(2026, 5, 1),
            exit_date=date(2026, 5, 11),
            signal_type="BUY",
            confidence=Decimal("0.7300"),
            score=Decimal("68.50"),
            entry_price=Decimal("42.0000"),
            exit_price=Decimal("44.1000"),
            gross_return_pct=Decimal("5.0000"),
            net_return_pct=Decimal("4.8000"),
            correct_direction=True,
        )
    )
    db_session.add(
        RecommendationSnapshot(
            run_id=run.run_id,
            snapshot_date=date(2026, 5, 27),
            profile="steady_20p_10d",
            stock_code="GTCO",
            company_name="GTCO Plc",
            signal_type="BUY",
            confidence=Decimal("0.7300"),
            score=Decimal("68.50"),
            current_price=Decimal("44.1000"),
            target_price=Decimal("49.0000"),
            stop_loss=Decimal("41.0000"),
            reasons=["Momentum intact"],
        )
    )
    db_session.add(
        BacktestPortfolioPosition(
            run_id=run.run_id,
            stock_code="GTCO",
            sector_name="Financial Services",
            entry_date=date(2026, 5, 1),
            exit_date=date(2026, 5, 11),
            holding_days=10,
            signal_type="BUY",
            confidence=Decimal("0.7300"),
            score=Decimal("68.50"),
            predicted_probability_10d_up=Decimal("0.6100"),
            entry_price=Decimal("42.0000"),
            exit_price=Decimal("44.1000"),
            allocated_capital=Decimal("200000.0000"),
            net_return_pct=Decimal("4.8000"),
            realized_pnl=Decimal("9600.0000"),
            exit_value=Decimal("209600.0000"),
            was_winner=True,
        )
    )
    db_session.add(
        BacktestPortfolioEquityPoint(
            run_id=run.run_id,
            point_index=1,
            event_date=date(2026, 5, 1),
            cash=Decimal("800000.0000"),
            open_position_capital=Decimal("200000.0000"),
            equity=Decimal("1000000.0000"),
            drawdown_pct=Decimal("0.0000"),
            open_positions=1,
        )
    )
    db_session.commit()

    deleted = _replace_equivalent_run(
        db_session,
        run_date=date(2026, 5, 27),
        start_date=date(2026, 1, 1),
        end_date=date(2026, 5, 27),
        horizon_days=10,
        profile="steady_20p_10d",
        smoke=False,
        stock_codes=None,
        min_score=60.0,
        min_confidence=0.70,
        min_predicted_probability=None,
        max_abs_gross_return_pct=50.0,
    )
    db_session.commit()

    assert deleted == 1
    assert db_session.query(BacktestRun).count() == 0
    assert db_session.query(BacktestTrade).count() == 0
    assert db_session.query(RecommendationSnapshot).count() == 0
    assert db_session.query(BacktestPortfolioPosition).count() == 0
    assert db_session.query(BacktestPortfolioEquityPoint).count() == 0
