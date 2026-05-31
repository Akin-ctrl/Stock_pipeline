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
