from app.services.advisory.selection import (
    RecommendationSelectionEvaluator,
    SelectionConfig,
)


def test_selection_evaluator_applies_action_and_threshold_rules():
    evaluator = RecommendationSelectionEvaluator(
        SelectionConfig(
            min_heuristic_score=55.0,
            min_signal_agreement=0.60,
            buy_only=True,
        )
    )

    accepted = evaluator.evaluate(
        action_value="BUY",
        heuristic_score=70.0,
        signal_agreement=0.75,
        predicted_probability_10d_up=0.68,
        min_heuristic_score=55.0,
        min_signal_agreement=0.60,
        min_predicted_probability=0.65,
    )
    rejected_action = evaluator.evaluate(
        action_value="AVOID",
        heuristic_score=70.0,
        signal_agreement=0.75,
        predicted_probability_10d_up=0.68,
        min_heuristic_score=55.0,
        min_signal_agreement=0.60,
        min_predicted_probability=0.65,
    )
    rejected_probability = evaluator.evaluate(
        action_value="BUY",
        heuristic_score=70.0,
        signal_agreement=0.75,
        predicted_probability_10d_up=0.50,
        min_heuristic_score=55.0,
        min_signal_agreement=0.60,
        min_predicted_probability=0.65,
    )
    rejected_missing_probability = evaluator.evaluate(
        action_value="BUY",
        heuristic_score=70.0,
        signal_agreement=0.75,
        predicted_probability_10d_up=None,
        min_heuristic_score=55.0,
        min_signal_agreement=0.60,
        min_predicted_probability=0.65,
    )

    assert accepted.selected is True
    assert accepted.rejection_reason is None
    assert rejected_action.selected is False
    assert rejected_action.rejection_reason == "buy_only_excludes_action"
    assert rejected_probability.selected is False
    assert rejected_probability.rejection_reason == "below_min_predicted_probability"
    assert rejected_missing_probability.selected is False
    assert rejected_missing_probability.rejection_reason == "missing_predicted_probability"
