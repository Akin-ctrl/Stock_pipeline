"""Policy helpers for strategy targets, stops, and heuristic risk labels."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping, Optional


@dataclass(frozen=True)
class RecommendationPolicyOutput:
    """Strategy-policy outputs applied after scoring and probability estimation."""

    policy_target_price: Optional[float]
    policy_stop_loss: Optional[float]
    heuristic_risk_level: str


class RecommendationPolicyEngine:
    """Apply profile-driven trade policy and heuristic risk classification."""

    def __init__(self, profile_config):
        self.profile_config = profile_config

    def build_policy_output(
        self,
        *,
        current_price: float,
        action_value: str,
        indicators: Mapping[str, float],
        signal_agreement: float,
        heuristic_score: float,
    ) -> RecommendationPolicyOutput:
        """Build policy target/stop outputs and a heuristic risk label."""
        policy_target_price, policy_stop_loss = self._calculate_policy_targets(
            current_price=current_price,
            action_value=action_value,
        )
        heuristic_risk_level = self._assess_heuristic_risk(
            indicators=indicators,
            signal_agreement=signal_agreement,
            heuristic_score=heuristic_score,
        )

        return RecommendationPolicyOutput(
            policy_target_price=policy_target_price,
            policy_stop_loss=policy_stop_loss,
            heuristic_risk_level=heuristic_risk_level,
        )

    def _calculate_policy_targets(
        self,
        *,
        current_price: float,
        action_value: str,
    ) -> tuple[Optional[float], Optional[float]]:
        """Apply profile policy for long-entry target and stop levels."""
        cfg = self.profile_config
        if action_value not in {"BUY", "STRONG_BUY"}:
            return None, None

        target_multiplier = (
            cfg.target_upside_strong
            if action_value == "STRONG_BUY"
            else cfg.target_upside_buy
        )
        stop_loss_multiplier = (
            cfg.stop_loss_strong
            if action_value == "STRONG_BUY"
            else cfg.stop_loss_buy
        )
        return current_price * target_multiplier, current_price * stop_loss_multiplier

    def _assess_heuristic_risk(
        self,
        *,
        indicators: Mapping[str, float],
        signal_agreement: float,
        heuristic_score: float,
    ) -> str:
        """Classify heuristic risk using the current rule-based policy."""
        risk_factors = []

        volatility = indicators.get("volatility", 0.30)
        if volatility > 0.50:
            risk_factors.append("high_volatility")
        elif volatility < 0.20:
            risk_factors.append("low_volatility")

        if signal_agreement < 0.6:
            risk_factors.append("low_signal_agreement")

        if heuristic_score < 50:
            risk_factors.append("low_heuristic_score")

        rsi = indicators.get("rsi_14")
        if rsi and (rsi < 25 or rsi > 75):
            risk_factors.append("extreme_rsi")

        if len(risk_factors) >= 3 or "high_volatility" in risk_factors:
            return "HIGH"
        if len(risk_factors) >= 1:
            return "MEDIUM"
        return "LOW"
