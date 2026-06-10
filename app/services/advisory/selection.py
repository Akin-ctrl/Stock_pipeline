"""Selection evaluation for eligible recommendation candidates."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class SelectionConfig:
    """Strategy-level selection thresholds applied after eligibility."""

    min_heuristic_score: float
    min_signal_agreement: float
    buy_only: bool
    min_predicted_probability: Optional[float] = None


@dataclass(frozen=True)
class SelectionDecision:
    """Result of applying selection thresholds to a recommendation candidate."""

    selected: bool
    rejection_reason: str | None = None


class RecommendationSelectionEvaluator:
    """Apply strategy thresholds and long-only semantics to candidates."""

    def __init__(self, config: SelectionConfig):
        self.config = config

    def evaluate(
        self,
        *,
        action_value: str,
        heuristic_score: Optional[float],
        signal_agreement: Optional[float],
        predicted_probability_10d_up: Optional[float],
        min_heuristic_score: Optional[float],
        min_signal_agreement: Optional[float],
        min_predicted_probability: Optional[float],
    ) -> SelectionDecision:
        """Return whether the candidate survives selection thresholds."""
        cfg = self.config
        effective_min_heuristic_score = (
            cfg.min_heuristic_score
            if min_heuristic_score is None
            else min_heuristic_score
        )
        effective_min_signal_agreement = (
            cfg.min_signal_agreement
            if min_signal_agreement is None
            else min_signal_agreement
        )
        effective_min_predicted_probability = (
            cfg.min_predicted_probability
            if min_predicted_probability is None
            else min_predicted_probability
        )

        if cfg.buy_only and action_value not in {"BUY", "STRONG_BUY"}:
            return SelectionDecision(False, "buy_only_excludes_action")

        if heuristic_score is None:
            return SelectionDecision(False, "missing_heuristic_score")
        if heuristic_score < effective_min_heuristic_score:
            return SelectionDecision(False, "below_min_heuristic_score")

        if signal_agreement is None:
            return SelectionDecision(False, "missing_signal_agreement")
        if signal_agreement < effective_min_signal_agreement:
            return SelectionDecision(False, "below_min_signal_agreement")

        if effective_min_predicted_probability is not None:
            if predicted_probability_10d_up is None:
                return SelectionDecision(False, "missing_predicted_probability")
            if predicted_probability_10d_up < effective_min_predicted_probability:
                return SelectionDecision(False, "below_min_predicted_probability")

        return SelectionDecision(True, None)
