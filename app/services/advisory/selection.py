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
        heuristic_score: float,
        signal_agreement: float,
        predicted_probability_10d_up: Optional[float],
        min_heuristic_score: float,
        min_signal_agreement: float,
        min_predicted_probability: Optional[float],
    ) -> SelectionDecision:
        """Return whether the candidate survives selection thresholds."""
        cfg = self.config

        if cfg.buy_only and action_value not in {"BUY", "STRONG_BUY"}:
            return SelectionDecision(False, "buy_only_excludes_action")

        if heuristic_score < min_heuristic_score:
            return SelectionDecision(False, "below_min_heuristic_score")

        if signal_agreement < min_signal_agreement:
            return SelectionDecision(False, "below_min_signal_agreement")

        if min_predicted_probability is not None:
            if predicted_probability_10d_up is None:
                return SelectionDecision(False, "missing_predicted_probability")
            if predicted_probability_10d_up < min_predicted_probability:
                return SelectionDecision(False, "below_min_predicted_probability")

        return SelectionDecision(True, None)
