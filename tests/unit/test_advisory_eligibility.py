from app.services.advisory.advisor import PROFILE_CONFIGS, RecommendationProfile
from app.services.advisory.eligibility import (
    EligibilityConfig,
    RecommendationEligibilityEvaluator,
)


def test_eligibility_evaluator_applies_hard_gates_in_order():
    evaluator = RecommendationEligibilityEvaluator(
        EligibilityConfig(
            min_price=1.0,
            max_volatility=0.75,
            min_volume_ratio=0.8,
            rsi_min=40.0,
            rsi_max=75.0,
            min_trusted_history_days=30,
            min_price_confidence_score=60.0,
            require_complete_data=True,
            require_official=False,
        )
    )

    decision = evaluator.evaluate(
        {
            "current_price": 10.0,
            "volatility": 0.25,
            "volume_ratio": 1.2,
            "trusted_history_days": 45,
            "price_confidence_score": 82.0,
            "has_complete_data": True,
            "is_official": False,
            "rsi_14": 58.0,
        }
    )
    failing_decision = evaluator.evaluate(
        {
            "current_price": 10.0,
            "volatility": 0.25,
            "volume_ratio": 0.5,
            "trusted_history_days": 45,
            "price_confidence_score": 82.0,
            "has_complete_data": True,
            "is_official": False,
            "rsi_14": 58.0,
        }
    )

    assert decision.eligible is True
    assert decision.rejection_reason is None
    assert failing_decision.eligible is False
    assert failing_decision.rejection_reason == "below_min_volume_ratio"


def test_eligibility_evaluator_applies_optional_exhaustion_guards():
    evaluator = RecommendationEligibilityEvaluator(
        EligibilityConfig(
            min_price=1.0,
            max_volatility=0.75,
            min_volume_ratio=0.8,
            rsi_min=40.0,
            rsi_max=75.0,
            min_trusted_history_days=30,
            min_price_confidence_score=60.0,
            require_complete_data=True,
            require_official=False,
            min_drawdown_20d_pct=1.0,
            max_price_change_20d_pct=12.0,
        )
    )

    near_high = evaluator.evaluate(
        {
            "current_price": 20.0,
            "volatility": 0.25,
            "volume_ratio": 1.2,
            "trusted_history_days": 45,
            "price_confidence_score": 82.0,
            "has_complete_data": True,
            "is_official": False,
            "drawdown_20d_pct": 0.2,
            "price_change_20d": 8.0,
            "rsi_14": 58.0,
        }
    )
    extended_run = evaluator.evaluate(
        {
            "current_price": 20.0,
            "volatility": 0.25,
            "volume_ratio": 1.2,
            "trusted_history_days": 45,
            "price_confidence_score": 82.0,
            "has_complete_data": True,
            "is_official": False,
            "drawdown_20d_pct": 3.0,
            "price_change_20d": 15.0,
            "rsi_14": 58.0,
        }
    )

    assert near_high.eligible is False
    assert near_high.rejection_reason == "below_min_drawdown_20d"
    assert extended_run.eligible is False
    assert extended_run.rejection_reason == "above_max_price_change_20d"


def test_active_profile_enables_exhaustion_guards():
    config = PROFILE_CONFIGS[
        RecommendationProfile.STEADY_20P_10D
    ].eligibility_config

    assert config.min_drawdown_20d_pct == 1.0
    assert config.max_price_change_20d_pct == 12.0
