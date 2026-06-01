"""Production portfolio controls for daily recommendation output."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from typing import Iterable, Optional

from sqlalchemy import func

from app.models import FactRecommendation
from app.services.advisory.advisor import RecommendationAction, StockRecommendation


ACTIONABLE_ACTIONS = {
    RecommendationAction.BUY,
    RecommendationAction.STRONG_BUY,
}


@dataclass(frozen=True)
class PortfolioPolicyConfig:
    """Risk limits that turn model candidates into portfolio-valid picks."""

    max_concurrent_positions: int = 3
    max_entries_per_day: int = 1
    position_size_pct: float = 0.20
    open_position_lookback_days: int = 21
    policy_version: str = "portfolio_policy_v1"

    def validate(self) -> None:
        if self.max_concurrent_positions <= 0:
            raise ValueError("max_concurrent_positions must be positive")
        if self.max_entries_per_day <= 0:
            raise ValueError("max_entries_per_day must be positive")
        if not 0 < self.position_size_pct <= 1:
            raise ValueError("position_size_pct must be within (0, 1]")
        if self.open_position_lookback_days <= 0:
            raise ValueError("open_position_lookback_days must be positive")


class ProductionPortfolioPolicy:
    """Apply portfolio-level gates to same-day recommendation candidates."""

    def __init__(self, config: PortfolioPolicyConfig | None = None):
        self.config = config or PortfolioPolicyConfig()
        self.config.validate()

    def count_open_positions(
        self,
        session,
        *,
        recommendation_date: date,
        profile: str,
    ) -> int:
        """Count currently open portfolio-approved positions from recent output."""
        lookback_start = recommendation_date - timedelta(
            days=self.config.open_position_lookback_days
        )
        return int(
            session.query(func.count(FactRecommendation.recommendation_id))
            .filter(
                FactRecommendation.recommendation_date >= lookback_start,
                FactRecommendation.recommendation_date < recommendation_date,
                FactRecommendation.profile == profile,
                FactRecommendation.is_active.is_(True),
                FactRecommendation.portfolio_approved.is_(True),
                FactRecommendation.action_type.in_(("BUY", "STRONG_BUY")),
            )
            .scalar()
            or 0
        )

    def apply(
        self,
        recommendations: Iterable[StockRecommendation],
        *,
        existing_open_positions: int = 0,
    ) -> list[StockRecommendation]:
        """Annotate recommendations with portfolio approval metadata."""
        open_positions = max(existing_open_positions, 0)
        daily_entries = 0
        annotated = []

        for recommendation in recommendations:
            available_slots = max(
                self.config.max_concurrent_positions - open_positions,
                0,
            )
            rejection_reason = self._rejection_reason(
                recommendation=recommendation,
                daily_entries=daily_entries,
                open_positions=open_positions,
            )

            approved = rejection_reason is None
            recommendation.portfolio_approved = approved
            recommendation.portfolio_rejection_reason = rejection_reason
            recommendation.portfolio_position_size_pct = (
                self.config.position_size_pct
                if approved
                else None
            )
            recommendation.portfolio_rank = daily_entries + 1 if approved else None
            recommendation.portfolio_policy_version = self.config.policy_version
            recommendation.portfolio_open_positions_before = open_positions
            recommendation.portfolio_available_slots_before = available_slots
            recommendation.portfolio_max_concurrent_positions = (
                self.config.max_concurrent_positions
            )
            recommendation.portfolio_max_entries_per_day = (
                self.config.max_entries_per_day
            )

            if approved:
                daily_entries += 1
                open_positions += 1

            annotated.append(recommendation)

        return annotated

    def _rejection_reason(
        self,
        *,
        recommendation: StockRecommendation,
        daily_entries: int,
        open_positions: int,
    ) -> Optional[str]:
        if recommendation.action_type not in ACTIONABLE_ACTIONS:
            return "non_actionable"
        if daily_entries >= self.config.max_entries_per_day:
            return "daily_entry_limit"
        if open_positions >= self.config.max_concurrent_positions:
            return "max_concurrent_positions"
        return None
