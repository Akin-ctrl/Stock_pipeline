from datetime import date

from app.services.advisory.advisor import (
    RecommendationAction,
    StockRecommendation,
    StockScreener,
)
from app.services.advisory.policy import RecommendationPolicyEngine
from app.services.advisory.scoring import ScoreCategory, StockScore
from app.services.advisory.signals import SignalType, TechnicalSignal


def test_technical_signal_confidence_alias_maps_to_signal_agreement():
    signal = TechnicalSignal(
        signal_type=SignalType.BUY,
        signal_agreement=0.72,
        reasons=["Momentum improving"],
        indicators={"rsi_14": 55.0},
    )

    assert signal.signal_agreement == 0.72
    assert signal.confidence == 0.72


def test_stock_recommendation_confidence_alias_maps_to_signal_agreement():
    recommendation = StockRecommendation(
        stock_id=1,
        stock_code="TEST",
        stock_name="Test Plc",
        recommendation_date=date(2026, 5, 27),
        signal_type=SignalType.BUY,
        action_type=RecommendationAction.BUY,
        signal_agreement=0.68,
        predicted_probability_10d_up=None,
        heuristic_score=71.5,
        heuristic_score_category=ScoreCategory.GOOD,
        policy_target_price=None,
        policy_stop_loss=None,
        heuristic_risk_level="MEDIUM",
        current_price=100,
        reasons=["Good setup"],
        indicators={"rsi_14": 58.0},
        technical_signal=TechnicalSignal(
            signal_type=SignalType.BUY,
            signal_agreement=0.68,
            reasons=["Good setup"],
            indicators={"rsi_14": 58.0},
        ),
        stock_score=StockScore(
            total_score=71.5,
            category=ScoreCategory.GOOD,
            technical_score=70.0,
            momentum_score=75.0,
            volatility_score=65.0,
            trend_score=74.0,
            volume_score=60.0,
            breakdown={"technical": 70.0},
        ),
    )

    assert recommendation.signal_agreement == 0.68
    assert recommendation.confidence == 0.68
    assert recommendation.action_type == RecommendationAction.BUY
    assert recommendation.is_actionable is True
    assert recommendation.heuristic_score == 71.5
    assert recommendation.score == 71.5
    assert recommendation.target_price is None
    assert recommendation.stop_loss is None
    assert recommendation.risk_level == "MEDIUM"
    assert recommendation.predicted_probability_10d_up is None


def test_signal_to_action_mapping_uses_long_only_avoid_states():
    assert StockScreener._map_signal_to_action(SignalType.STRONG_BUY) == RecommendationAction.STRONG_BUY
    assert StockScreener._map_signal_to_action(SignalType.BUY) == RecommendationAction.BUY
    assert StockScreener._map_signal_to_action(SignalType.HOLD) == RecommendationAction.HOLD
    assert StockScreener._map_signal_to_action(SignalType.SELL) == RecommendationAction.AVOID
    assert StockScreener._map_signal_to_action(SignalType.STRONG_SELL) == RecommendationAction.STRONGLY_AVOID


def test_policy_engine_marks_trade_levels_as_policy_outputs():
    class _Profile:
        target_upside_buy = 1.20
        target_upside_strong = 1.25
        stop_loss_buy = 0.94
        stop_loss_strong = 0.92

    engine = RecommendationPolicyEngine(_Profile())
    output = engine.build_policy_output(
        current_price=100.0,
        action_value="BUY",
        indicators={"volatility": 0.18, "rsi_14": 58.0},
        signal_agreement=0.70,
        heuristic_score=72.0,
    )

    assert output.policy_target_price == 120.0
    assert output.policy_stop_loss == 94.0
    assert output.heuristic_risk_level == "MEDIUM"
