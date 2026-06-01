from datetime import date

import pytest

from app.services.backtesting import (
    BacktestResult,
    BacktestTrade,
    PortfolioSimulationConfig,
    PortfolioSimulator,
)


def test_portfolio_simulator_allocates_capital_and_reports_equity_metrics():
    trades = [
        _trade("AAA", date(2026, 1, 2), date(2026, 1, 12), 10.0, score=90.0),
        _trade("BBB", date(2026, 1, 2), date(2026, 1, 12), -5.0, score=80.0),
        _trade("CCC", date(2026, 1, 13), date(2026, 1, 23), 20.0, score=85.0),
    ]
    simulator = PortfolioSimulator(
        PortfolioSimulationConfig(
            initial_capital=100_000.0,
            max_concurrent_positions=2,
            max_entries_per_day=1,
            position_size_pct=0.50,
        )
    )

    result = simulator.simulate(trades)

    assert result.realized_trade_count == 2
    assert result.skipped_reasons == {"daily_entry_limit": 1}
    assert result.final_equity == pytest.approx(115_500.0)
    assert result.total_return_pct == pytest.approx(15.5)
    assert result.win_rate_pct == pytest.approx(100.0)
    assert result.max_drawdown_pct == pytest.approx(0.0)


def test_portfolio_simulator_applies_loss_cooldown_before_new_entries():
    trades = [
        _trade("AAA", date(2026, 1, 2), date(2026, 1, 5), -10.0, score=90.0),
        _trade("BBB", date(2026, 1, 6), date(2026, 1, 16), 30.0, score=90.0),
        _trade("CCC", date(2026, 1, 9), date(2026, 1, 19), 20.0, score=90.0),
    ]
    simulator = PortfolioSimulator(
        PortfolioSimulationConfig(
            initial_capital=100_000.0,
            max_concurrent_positions=1,
            max_entries_per_day=1,
            position_size_pct=1.0,
            cooldown_days_after_loss=3,
        )
    )

    result = simulator.simulate(trades)

    assert [position.stock_code for position in result.accepted_positions] == [
        "AAA",
        "CCC",
    ]
    assert result.skipped_reasons == {"cooldown_active": 1}
    assert result.final_equity == pytest.approx(108_000.0)
    assert result.max_drawdown_pct == pytest.approx(10.0)


def test_portfolio_config_rejects_invalid_risk_settings():
    with pytest.raises(ValueError, match="position_size_pct"):
        PortfolioSimulator(
            PortfolioSimulationConfig(position_size_pct=1.5)
        )


def test_backtest_and_portfolio_serialization_keep_profit_factor_strict_json():
    trade = _trade("AAA", date(2026, 1, 2), date(2026, 1, 12), 10.0, score=90.0)
    backtest_payload = BacktestResult(
        start_date=date(2026, 1, 2),
        end_date=date(2026, 1, 12),
        horizon_days=10,
        trades=[trade],
    ).to_dict()

    portfolio_payload = PortfolioSimulator().simulate([trade]).to_dict()

    assert backtest_payload["profit_factor"] is None
    assert backtest_payload["profit_factor_unbounded"] is True
    assert portfolio_payload["profit_factor"] is None
    assert portfolio_payload["profit_factor_unbounded"] is True


def _trade(
    stock_code: str,
    entry_date: date,
    exit_date: date,
    net_return_pct: float,
    *,
    score: float,
) -> BacktestTrade:
    return BacktestTrade(
        stock_code=stock_code,
        entry_date=entry_date,
        exit_date=exit_date,
        action_type="BUY",
        signal_type="BUY",
        confidence=0.8,
        score=score,
        predicted_probability_10d_up=None,
        entry_price=100.0,
        exit_price=100.0 * (1 + net_return_pct / 100.0),
        gross_return_pct=net_return_pct,
        net_return_pct=net_return_pct,
        correct_direction=net_return_pct > 0,
    )
