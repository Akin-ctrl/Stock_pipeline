from types import SimpleNamespace

from app.services.advisory.advisor import StockScreener
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


def test_selection_evaluator_uses_config_thresholds_when_overrides_are_none():
    evaluator = RecommendationSelectionEvaluator(
        SelectionConfig(
            min_heuristic_score=70.0,
            min_signal_agreement=0.60,
            buy_only=True,
        )
    )

    rejected_score = evaluator.evaluate(
        action_value="BUY",
        heuristic_score=69.0,
        signal_agreement=0.75,
        predicted_probability_10d_up=None,
        min_heuristic_score=None,
        min_signal_agreement=None,
        min_predicted_probability=None,
    )
    rejected_signal = evaluator.evaluate(
        action_value="BUY",
        heuristic_score=75.0,
        signal_agreement=0.59,
        predicted_probability_10d_up=None,
        min_heuristic_score=None,
        min_signal_agreement=None,
        min_predicted_probability=None,
    )

    assert rejected_score.selected is False
    assert rejected_score.rejection_reason == "below_min_heuristic_score"
    assert rejected_signal.selected is False
    assert rejected_signal.rejection_reason == "below_min_signal_agreement"


def test_selection_evaluator_rejects_missing_candidate_metrics():
    evaluator = RecommendationSelectionEvaluator(
        SelectionConfig(
            min_heuristic_score=70.0,
            min_signal_agreement=0.60,
            buy_only=True,
        )
    )

    missing_score = evaluator.evaluate(
        action_value="BUY",
        heuristic_score=None,
        signal_agreement=0.75,
        predicted_probability_10d_up=None,
        min_heuristic_score=None,
        min_signal_agreement=None,
        min_predicted_probability=None,
    )
    missing_signal = evaluator.evaluate(
        action_value="BUY",
        heuristic_score=75.0,
        signal_agreement=None,
        predicted_probability_10d_up=None,
        min_heuristic_score=None,
        min_signal_agreement=None,
        min_predicted_probability=None,
    )

    assert missing_score.selected is False
    assert missing_score.rejection_reason == "missing_heuristic_score"
    assert missing_signal.selected is False
    assert missing_signal.rejection_reason == "missing_signal_agreement"


def test_recommendation_rank_key_keeps_probability_diagnostic():
    stronger_heuristic = SimpleNamespace(
        heuristic_score=75.0,
        signal_agreement=0.70,
        predicted_probability_10d_up=0.40,
    )
    weaker_heuristic_higher_probability = SimpleNamespace(
        heuristic_score=70.0,
        signal_agreement=0.70,
        predicted_probability_10d_up=0.95,
    )
    same_heuristic_higher_probability = SimpleNamespace(
        heuristic_score=75.0,
        signal_agreement=0.70,
        predicted_probability_10d_up=0.60,
    )

    assert StockScreener._recommendation_rank_key(
        stronger_heuristic
    ) > StockScreener._recommendation_rank_key(
        weaker_heuristic_higher_probability
    )
    assert StockScreener._recommendation_rank_key(
        same_heuristic_higher_probability
    ) > StockScreener._recommendation_rank_key(
        stronger_heuristic
    )
