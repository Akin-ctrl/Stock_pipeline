from datetime import date

import pytest

from app.services.advisory import (
    PortfolioPolicyConfig,
    ProductionPortfolioPolicy,
    RecommendationAction,
    StockRecommendation,
)
from app.services.advisory.scoring import ScoreCategory, StockScore
from app.services.advisory.signals import SignalType, TechnicalSignal


def test_portfolio_policy_approves_one_daily_entry_and_blocks_the_rest():
    policy = ProductionPortfolioPolicy(
        PortfolioPolicyConfig(
            max_concurrent_positions=3,
            max_entries_per_day=1,
            position_size_pct=0.20,
        )
    )

    recommendations = policy.apply(
        [
            _recommendation("AAA", score=90.0),
            _recommendation("BBB", score=80.0),
        ],
        existing_open_positions=0,
    )

    assert recommendations[0].portfolio_approved is True
    assert recommendations[0].portfolio_rank == 1
    assert recommendations[0].portfolio_position_size_pct == 0.20
    assert recommendations[0].portfolio_rejection_reason is None
    assert recommendations[1].portfolio_approved is False
    assert recommendations[1].portfolio_rejection_reason == "daily_entry_limit"


def test_portfolio_policy_blocks_when_existing_positions_fill_capacity():
    policy = ProductionPortfolioPolicy(
        PortfolioPolicyConfig(max_concurrent_positions=3)
    )

    recommendations = policy.apply(
        [_recommendation("AAA", score=90.0)],
        existing_open_positions=3,
    )

    assert recommendations[0].portfolio_approved is False
    assert recommendations[0].portfolio_rejection_reason == "max_concurrent_positions"
    assert recommendations[0].portfolio_available_slots_before == 0


def test_portfolio_policy_rejects_invalid_config():
    with pytest.raises(ValueError, match="position_size_pct"):
        ProductionPortfolioPolicy(
            PortfolioPolicyConfig(position_size_pct=0)
        )


def _recommendation(stock_code: str, *, score: float) -> StockRecommendation:
    signal = TechnicalSignal(
        signal_type=SignalType.BUY,
        signal_agreement=0.8,
        reasons=["Valid setup"],
        indicators={"rsi_14": 55.0},
    )
    stock_score = StockScore(
        total_score=score,
        category=ScoreCategory.GOOD,
        technical_score=70.0,
        momentum_score=80.0,
        volatility_score=70.0,
        trend_score=75.0,
        volume_score=60.0,
        breakdown={"momentum": 80.0},
    )
    return StockRecommendation(
        stock_id=hash(stock_code) % 10_000,
        stock_code=stock_code,
        stock_name=f"{stock_code} Plc",
        recommendation_date=date(2026, 5, 29),
        signal_type=SignalType.BUY,
        action_type=RecommendationAction.BUY,
        signal_agreement=0.8,
        predicted_probability_10d_up=None,
        heuristic_score=score,
        heuristic_score_category=ScoreCategory.GOOD,
        policy_target_price=None,
        policy_stop_loss=None,
        heuristic_risk_level="MEDIUM",
        current_price=100,
        reasons=["Valid setup"],
        indicators={"rsi_14": 55.0},
        technical_signal=signal,
        stock_score=stock_score,
    )
