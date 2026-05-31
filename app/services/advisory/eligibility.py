"""Eligibility evaluation for recommendation candidates."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping


@dataclass(frozen=True)
class EligibilityConfig:
    """Hard-gating rules that determine whether a stock can be considered."""

    min_price: float
    max_volatility: float
    min_volume_ratio: float
    rsi_min: float
    rsi_max: float
    min_trusted_history_days: int
    min_price_confidence_score: float
    require_complete_data: bool
    require_official: bool


@dataclass(frozen=True)
class EligibilityDecision:
    """Result of evaluating one stock snapshot against eligibility rules."""

    eligible: bool
    rejection_reason: str | None = None


class RecommendationEligibilityEvaluator:
    """Apply hard eligibility rules before recommendation selection."""

    def __init__(self, config: EligibilityConfig):
        self.config = config

    def evaluate(self, indicators: Mapping[str, float]) -> EligibilityDecision:
        """Return whether the current stock snapshot is eligible."""
        cfg = self.config

        current_price = indicators.get("current_price")
        if current_price is not None and current_price < cfg.min_price:
            return EligibilityDecision(False, "below_min_price")

        volatility = indicators.get("volatility")
        if volatility is not None and volatility > cfg.max_volatility:
            return EligibilityDecision(False, "above_max_volatility")

        volume_ratio = indicators.get("volume_ratio")
        if volume_ratio is not None and volume_ratio < cfg.min_volume_ratio:
            return EligibilityDecision(False, "below_min_volume_ratio")

        trusted_history_days = indicators.get("trusted_history_days")
        if (
            trusted_history_days is not None
            and trusted_history_days < cfg.min_trusted_history_days
        ):
            return EligibilityDecision(False, "insufficient_trusted_history")

        price_confidence_score = indicators.get("price_confidence_score")
        if (
            price_confidence_score is not None
            and price_confidence_score < cfg.min_price_confidence_score
        ):
            return EligibilityDecision(False, "below_min_price_confidence")

        if cfg.require_complete_data and not indicators.get("has_complete_data", False):
            return EligibilityDecision(False, "requires_complete_data")

        if cfg.require_official and not indicators.get("is_official", False):
            return EligibilityDecision(False, "requires_official_data")

        rsi_val = indicators.get("rsi_14")
        if rsi_val is not None and (rsi_val < cfg.rsi_min or rsi_val > cfg.rsi_max):
            return EligibilityDecision(False, "outside_rsi_band")

        return EligibilityDecision(True, None)
