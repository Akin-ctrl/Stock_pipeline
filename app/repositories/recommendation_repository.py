"""
Repository for investment recommendations.

Handles database operations for stock recommendations.
"""

from datetime import date
from typing import List, Optional, TYPE_CHECKING
from decimal import Decimal

from sqlalchemy.orm import Session, joinedload
from sqlalchemy import and_, desc, func
from sqlalchemy import text

from app.models import FactRecommendation, FactRecommendationAudit, DimStock
from app.repositories.base import BaseRepository

if TYPE_CHECKING:
    from app.services.advisory import StockRecommendation


def _decimal_or_none(value) -> Optional[Decimal]:
    """Convert numeric-like values to Decimal while preserving nulls."""
    if value is None:
        return None
    return Decimal(str(value))


class RecommendationRepository(BaseRepository[FactRecommendation]):
    """
    Repository for managing stock recommendations.
    
    Provides methods for creating, retrieving, and analyzing
    investment recommendations.
    """
    
    def __init__(self, db: Session):
        """Initialize repository with database session."""
        super().__init__(FactRecommendation, db)
    
    def create_recommendation(
        self,
        recommendation: "StockRecommendation"
    ) -> FactRecommendation:
        """
        Create a new recommendation record.
        
        Args:
            recommendation: StockRecommendation object
            
        Returns:
            Created FactRecommendation instance
        """
        self._delete_existing_rows(
            recommendation_date=recommendation.recommendation_date,
            stock_ids=[recommendation.stock_id],
            profile=getattr(recommendation, "strategy_profile", "steady_20p_10d"),
        )

        fact_rec = self._build_fact_recommendation(recommendation)
        self.session.add(fact_rec)
        self.session.flush()
        self.session.refresh(fact_rec)
        
        return fact_rec

    def _delete_existing_rows(
        self,
        recommendation_date: date,
        stock_ids: List[int],
        profile: str = "steady_20p_10d",
    ) -> int:
        """Remove existing rows for the same stock/date pair before reinserting."""
        if not stock_ids:
            return 0

        deleted = (
            self.session.query(FactRecommendation)
            .filter(
                FactRecommendation.recommendation_date == recommendation_date,
                FactRecommendation.stock_id.in_(stock_ids),
                FactRecommendation.profile == profile,
            )
            .delete(synchronize_session=False)
        )
        return deleted

    def delete_recommendations_for_date_profile(
        self,
        recommendation_date: date,
        profile: str = "steady_20p_10d",
    ) -> int:
        """Delete one full recommendation snapshot before regenerating it."""
        return (
            self.session.query(FactRecommendation)
            .filter(
                FactRecommendation.recommendation_date == recommendation_date,
                FactRecommendation.profile == profile,
            )
            .delete(synchronize_session=False)
        )
    
    def create_recommendations_bulk(
        self,
        recommendations: List["StockRecommendation"]
    ) -> int:
        """
        Create multiple recommendations in bulk.
        
        Args:
            recommendations: List of StockRecommendation objects
            
        Returns:
            Number of records created
        """
        if not recommendations:
            return 0

        deduped: dict[tuple[int, date], "StockRecommendation"] = {}
        for rec in recommendations:
            deduped[(rec.stock_id, rec.recommendation_date)] = rec

        recommendations = list(deduped.values())

        recs_by_date_profile: dict[tuple[date, str], List["StockRecommendation"]] = {}
        for rec in recommendations:
            profile = getattr(rec, "strategy_profile", "steady_20p_10d")
            recs_by_date_profile.setdefault((rec.recommendation_date, profile), []).append(rec)

        for (recommendation_date, profile), recs in recs_by_date_profile.items():
            self._delete_existing_rows(
                recommendation_date=recommendation_date,
                stock_ids=[rec.stock_id for rec in recs],
                profile=profile,
            )

        count = 0
        for rec in recommendations:
            self.session.add(self._build_fact_recommendation(rec))
            count += 1

        self.session.flush()
        return count

    def replace_audit_entries(
        self,
        *,
        recommendation_date: date,
        profile: str,
        audit_entries: List["RecommendationAuditEntry"],
        full_snapshot: bool = True,
    ) -> int:
        """Replace one full recommendation audit snapshot idempotently."""
        if not audit_entries:
            if full_snapshot:
                self.session.query(FactRecommendationAudit).filter(
                    FactRecommendationAudit.recommendation_date == recommendation_date,
                    FactRecommendationAudit.profile == profile,
                ).delete(synchronize_session=False)
            self.session.flush()
            return 0

        deduped: dict[int, "RecommendationAuditEntry"] = {}
        for entry in audit_entries:
            deduped[entry.stock_id] = entry

        delete_query = self.session.query(FactRecommendationAudit).filter(
            FactRecommendationAudit.recommendation_date == recommendation_date,
            FactRecommendationAudit.profile == profile,
        )
        if not full_snapshot:
            delete_query = delete_query.filter(
                FactRecommendationAudit.stock_id.in_(list(deduped.keys()))
            )
        delete_query.delete(synchronize_session=False)

        for entry in deduped.values():
            self.session.add(self._build_audit_row(entry))

        self.session.flush()
        return len(deduped)

    def _build_audit_row(
        self,
        entry: "RecommendationAuditEntry",
    ) -> FactRecommendationAudit:
        """Map a screener audit entry to the database audit fact."""
        indicators = dict(entry.indicators or {})
        score_breakdown = dict(entry.score_breakdown or {})

        return FactRecommendationAudit(
            stock_id=entry.stock_id,
            recommendation_date=entry.recommendation_date,
            profile=entry.profile,
            price_date=entry.price_date,
            indicator_date=entry.indicator_date,
            current_price=_decimal_or_none(entry.current_price),
            stage_reached=entry.stage_reached,
            rejection_reason=entry.rejection_reason,
            eligible=entry.eligible,
            selected=entry.selected,
            candidate_tier=entry.candidate_tier,
            portfolio_approved=entry.portfolio_approved,
            portfolio_rejection_reason=entry.portfolio_rejection_reason,
            portfolio_rank=entry.portfolio_rank,
            action_type=entry.action_type,
            technical_signal_type=entry.technical_signal_type,
            signal_agreement=_decimal_or_none(entry.signal_agreement),
            predicted_probability_10d_up=_decimal_or_none(
                entry.predicted_probability_10d_up
            ),
            heuristic_score=_decimal_or_none(entry.heuristic_score),
            heuristic_score_category=entry.heuristic_score_category,
            rsi_14=_decimal_or_none(indicators.get("rsi_14")),
            volatility=_decimal_or_none(indicators.get("volatility")),
            volume_ratio=_decimal_or_none(indicators.get("volume_ratio")),
            price_change_20d=_decimal_or_none(indicators.get("price_change_20d")),
            drawdown_20d_pct=_decimal_or_none(indicators.get("drawdown_20d_pct")),
            trusted_history_days=indicators.get("trusted_history_days"),
            price_quality_flag=indicators.get("price_quality_flag"),
            bar_status=indicators.get("bar_status"),
            has_complete_data=indicators.get("has_complete_data"),
            is_official=indicators.get("is_official"),
            indicators=indicators,
            score_breakdown=score_breakdown,
            model_version=entry.model_version,
        )

    def _build_fact_recommendation(
        self,
        rec: "StockRecommendation",
    ) -> FactRecommendation:
        """Map the revamped advisory object to the model-aligned fact table."""
        policy_upside_pct = None
        policy_downside_pct = None
        risk_reward_ratio = None

        if rec.policy_target_price is not None and rec.current_price:
            policy_upside_pct = (
                (rec.policy_target_price - rec.current_price) / rec.current_price
            ) * 100

        if rec.policy_stop_loss is not None and rec.current_price:
            policy_downside_pct = (
                (rec.current_price - rec.policy_stop_loss) / rec.current_price
            ) * 100

        if policy_upside_pct is not None and policy_downside_pct not in (None, 0):
            risk_reward_ratio = policy_upside_pct / policy_downside_pct

        return FactRecommendation(
            stock_id=rec.stock_id,
            recommendation_date=rec.recommendation_date,
            profile=getattr(rec, "strategy_profile", "steady_20p_10d"),
            action_type=rec.action_type.value,
            technical_signal_type=rec.signal_type.value,
            signal_agreement=Decimal(str(rec.signal_agreement)),
            predicted_probability_10d_up=_decimal_or_none(
                rec.predicted_probability_10d_up
            ),
            heuristic_score=Decimal(str(rec.heuristic_score)),
            heuristic_score_category=rec.heuristic_score_category.value,
            current_price=rec.current_price,
            policy_target_price=rec.policy_target_price,
            policy_stop_loss=rec.policy_stop_loss,
            policy_upside_pct=_decimal_or_none(policy_upside_pct),
            policy_downside_pct=_decimal_or_none(policy_downside_pct),
            risk_reward_ratio=_decimal_or_none(risk_reward_ratio),
            heuristic_risk_level=rec.heuristic_risk_level,
            reasons=rec.reasons,
            technical_score=Decimal(str(rec.stock_score.technical_score)),
            momentum_score=Decimal(str(rec.stock_score.momentum_score)),
            volatility_score=Decimal(str(rec.stock_score.volatility_score)),
            trend_score=Decimal(str(rec.stock_score.trend_score)),
            volume_score=Decimal(str(rec.stock_score.volume_score)),
            rsi_14=_decimal_or_none(rec.indicators.get('rsi_14')),
            macd=_decimal_or_none(rec.indicators.get('macd')),
            portfolio_approved=bool(getattr(rec, "portfolio_approved", False)),
            portfolio_rejection_reason=getattr(
                rec,
                "portfolio_rejection_reason",
                None,
            ),
            portfolio_rank=getattr(rec, "portfolio_rank", None),
            portfolio_position_size_pct=_decimal_or_none(
                getattr(rec, "portfolio_position_size_pct", None)
            ),
            portfolio_policy_version=getattr(
                rec,
                "portfolio_policy_version",
                None,
            ),
            portfolio_open_positions_before=getattr(
                rec,
                "portfolio_open_positions_before",
                None,
            ),
            portfolio_available_slots_before=getattr(
                rec,
                "portfolio_available_slots_before",
                None,
            ),
            portfolio_max_concurrent_positions=getattr(
                rec,
                "portfolio_max_concurrent_positions",
                None,
            ),
            portfolio_max_entries_per_day=getattr(
                rec,
                "portfolio_max_entries_per_day",
                None,
            ),
            model_version="historical_logistic_v1"
            if rec.predicted_probability_10d_up is not None
            else None,
            is_active=True,
        )
    
    def get_recommendations_by_date(
        self,
        recommendation_date: date,
        include_stock: bool = True
    ) -> List[FactRecommendation]:
        """
        Get all recommendations for a specific date.
        
        Args:
            recommendation_date: Date to query
            include_stock: Whether to eagerly load stock relationship
            
        Returns:
            List of FactRecommendation instances
        """
        query = self.session.query(FactRecommendation).filter(
            FactRecommendation.recommendation_date == recommendation_date
        )
        
        if include_stock:
            query = query.options(joinedload(FactRecommendation.stock))
        
        return query.order_by(desc(FactRecommendation.heuristic_score)).all()
    
    def get_recommendations_by_date_range(
        self,
        start_date: date,
        end_date: date,
        signal_type: Optional[str] = None
    ) -> List[FactRecommendation]:
        """
        Get recommendations within a date range.
        
        Args:
            start_date: Start date (inclusive)
            end_date: End date (inclusive)
            signal_type: Optional filter by signal type
            
        Returns:
            List of FactRecommendation instances
        """
        query = self.session.query(FactRecommendation).filter(
            and_(
                FactRecommendation.recommendation_date >= start_date,
                FactRecommendation.recommendation_date <= end_date
            )
        )
        
        if signal_type:
            query = query.filter(FactRecommendation.action_type == signal_type)
        
        return query.options(
            joinedload(FactRecommendation.stock)
        ).order_by(
            desc(FactRecommendation.recommendation_date),
            desc(FactRecommendation.heuristic_score)
        ).all()
    
    def get_recommendations_by_stock(
        self,
        stock_id: int,
        limit: int = 10
    ) -> List[FactRecommendation]:
        """
        Get recent recommendations for a specific stock.
        
        Args:
            stock_id: Stock ID
            limit: Maximum number of records to return
            
        Returns:
            List of FactRecommendation instances
        """
        return self.session.query(FactRecommendation).filter(
            FactRecommendation.stock_id == stock_id
        ).order_by(
            desc(FactRecommendation.recommendation_date)
        ).limit(limit).all()
    
    def get_active_recommendations(
        self,
        signal_type: Optional[str] = None
    ) -> List[FactRecommendation]:
        """
        Get all active recommendations.
        
        Args:
            signal_type: Optional filter by signal type
            
        Returns:
            List of active FactRecommendation instances
        """
        query = self.session.query(FactRecommendation).filter(
            FactRecommendation.is_active == True
        )
        
        if signal_type:
            query = query.filter(FactRecommendation.action_type == signal_type)
        
        return query.options(
            joinedload(FactRecommendation.stock)
        ).order_by(
            desc(FactRecommendation.heuristic_score)
        ).all()
    
    def get_top_picks(
        self,
        recommendation_date: date,
        signal_type: str = 'BUY',
        top_n: int = 10
    ) -> List[FactRecommendation]:
        """
        Get top stock picks for a date.
        
        Args:
            recommendation_date: Date to query
            signal_type: Signal type filter (default: 'BUY')
            top_n: Number of top picks
            
        Returns:
            List of top FactRecommendation instances
        """
        action_filter = (
            FactRecommendation.action_type.in_(['BUY', 'STRONG_BUY'])
            if signal_type == 'BUY'
            else FactRecommendation.action_type == signal_type
        )
        return self.session.query(FactRecommendation).filter(
            and_(
                FactRecommendation.recommendation_date == recommendation_date,
                FactRecommendation.portfolio_approved.is_(True),
                action_filter,
            )
        ).options(
            joinedload(FactRecommendation.stock)
        ).order_by(
            desc(FactRecommendation.predicted_probability_10d_up),
            desc(FactRecommendation.heuristic_score),
            desc(FactRecommendation.signal_agreement)
        ).limit(top_n).all()
    
    def update_recommendation_outcome(
        self,
        recommendation_id: int,
        outcome: str,
        outcome_date: date,
        actual_return_pct: Optional[Decimal] = None
    ) -> Optional[FactRecommendation]:
        """
        Update recommendation outcome.
        
        Args:
            recommendation_id: Recommendation ID
            outcome: Outcome status (HIT_TARGET, HIT_STOP_LOSS, EXPIRED)
            outcome_date: Date of outcome
            actual_return_pct: Actual return percentage
            
        Returns:
            Updated FactRecommendation or None
        """
        rec = self.get_by_id(recommendation_id)
        if not rec:
            return None
        
        rec.outcome = outcome
        rec.outcome_date = outcome_date
        rec.actual_return_pct = actual_return_pct
        rec.is_active = False
        
        self.session.commit()
        self.session.refresh(rec)
        
        return rec
    
    def mark_expired(
        self,
        cutoff_date: date,
        max_days: int = 30
    ) -> int:
        """
        Mark old recommendations as expired.
        
        Args:
            cutoff_date: Current date
            max_days: Maximum days before expiring
            
        Returns:
            Number of records updated
        """
        expired_date = date.fromordinal(cutoff_date.toordinal() - max_days)
        
        count = self.session.query(FactRecommendation).filter(
            and_(
                FactRecommendation.is_active == True,
                FactRecommendation.recommendation_date <= expired_date,
                FactRecommendation.outcome.is_(None)
            )
        ).update({
            'outcome': 'EXPIRED',
            'outcome_date': cutoff_date,
            'is_active': False
        })
        
        self.session.commit()
        return count
    
    def get_performance_stats(
        self,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None
    ) -> dict:
        """
        Get recommendation performance statistics.
        
        Args:
            start_date: Start date for analysis
            end_date: End date for analysis
            
        Returns:
            Dict of performance metrics
        """
        query = self.session.query(FactRecommendation)
        
        if start_date:
            query = query.filter(FactRecommendation.recommendation_date >= start_date)
        if end_date:
            query = query.filter(FactRecommendation.recommendation_date <= end_date)
        
        total = query.count()
        
        # Count by outcome
        outcomes = query.filter(
            FactRecommendation.outcome.isnot(None)
        ).with_entities(
            FactRecommendation.outcome,
            func.count(FactRecommendation.recommendation_id)
        ).group_by(FactRecommendation.outcome).all()
        
        outcome_counts = {outcome: count for outcome, count in outcomes}
        
        # Average returns
        avg_return = query.filter(
            FactRecommendation.actual_return_pct.isnot(None)
        ).with_entities(
            func.avg(FactRecommendation.actual_return_pct)
        ).scalar()
        
        # Win rate (hit target vs hit stop loss)
        wins = outcome_counts.get('HIT_TARGET', 0)
        losses = outcome_counts.get('HIT_STOP_LOSS', 0)
        win_rate = (wins / (wins + losses) * 100) if (wins + losses) > 0 else 0
        
        return {
            'total_recommendations': total,
            'outcomes': outcome_counts,
            'average_return_pct': float(avg_return) if avg_return else 0.0,
            'win_rate_pct': win_rate,
            'wins': wins,
            'losses': losses
        }

    def deduplicate_existing_rows(self) -> int:
        """
        Remove duplicate recommendation rows, keeping the newest row per stock/date.

        Returns:
            Number of duplicate rows deleted
        """
        result = self.session.execute(
            text(
                """
                DELETE FROM fact_recommendations fr
                USING (
                    SELECT recommendation_id
                    FROM (
                        SELECT
                            recommendation_id,
                            ROW_NUMBER() OVER (
                                PARTITION BY stock_id, recommendation_date, profile
                                ORDER BY recommendation_id DESC
                            ) AS rn
                        FROM fact_recommendations
                    ) ranked
                    WHERE ranked.rn > 1
                ) dupes
                WHERE fr.recommendation_id = dupes.recommendation_id
                """
            )
        )
        return result.rowcount or 0
    
    def get_by_id(self, recommendation_id: int) -> Optional[FactRecommendation]:
        """Get recommendation by ID."""
        return self.session.query(FactRecommendation).filter(
            FactRecommendation.recommendation_id == recommendation_id
        ).options(joinedload(FactRecommendation.stock)).first()
