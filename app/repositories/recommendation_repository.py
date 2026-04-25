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

from app.models import FactRecommendation, DimStock
from app.repositories.base import BaseRepository

if TYPE_CHECKING:
    from app.services.advisory import StockRecommendation


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
        # Build recommendation reason from reasons list
        reason_text = " | ".join(recommendation.reasons[:3])  # Top 3 reasons
        
        # Calculate potential return
        potential_return = None
        if recommendation.target_price and recommendation.current_price:
            potential_return = (
                (recommendation.target_price - recommendation.current_price) 
                / recommendation.current_price
            ) * 100
        
        self._delete_existing_rows(
            recommendation_date=recommendation.recommendation_date,
            stock_ids=[recommendation.stock_id],
        )

        fact_rec = FactRecommendation(
            stock_id=recommendation.stock_id,
            recommendation_date=recommendation.recommendation_date,
            signal_type=recommendation.signal_type.value,
            confidence_score=Decimal(str(recommendation.confidence * 100)),
            overall_score=Decimal(str(recommendation.score)),
            score_category=recommendation.score_category.value,
            current_price=recommendation.current_price,
            target_price=recommendation.target_price,
            stop_loss=recommendation.stop_loss,
            potential_return_pct=Decimal(str(potential_return)) if potential_return else None,
            risk_level=recommendation.risk_level,
            recommendation_reason=reason_text,
            technical_score=Decimal(str(recommendation.stock_score.technical_score)),
            momentum_score=Decimal(str(recommendation.stock_score.momentum_score)),
            volatility_score=Decimal(str(recommendation.stock_score.volatility_score)),
            trend_score=Decimal(str(recommendation.stock_score.trend_score)),
            volume_score=Decimal(str(recommendation.stock_score.volume_score)),
            rsi_value=Decimal(str(recommendation.indicators.get('rsi_14'))) 
                if 'rsi_14' in recommendation.indicators else None,
            macd_value=Decimal(str(recommendation.indicators.get('macd')))
                if 'macd' in recommendation.indicators else None,
            is_active=True
        )
        
        self.session.add(fact_rec)
        self.session.flush()
        self.session.refresh(fact_rec)
        
        return fact_rec

    def _delete_existing_rows(
        self,
        recommendation_date: date,
        stock_ids: List[int],
    ) -> int:
        """Remove existing rows for the same stock/date pair before reinserting."""
        if not stock_ids:
            return 0

        deleted = (
            self.session.query(FactRecommendation)
            .filter(
                FactRecommendation.recommendation_date == recommendation_date,
                FactRecommendation.stock_id.in_(stock_ids),
            )
            .delete(synchronize_session=False)
        )
        return deleted
    
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

        recs_by_date: dict[date, List["StockRecommendation"]] = {}
        for rec in recommendations:
            recs_by_date.setdefault(rec.recommendation_date, []).append(rec)

        for recommendation_date, recs in recs_by_date.items():
            self._delete_existing_rows(
                recommendation_date=recommendation_date,
                stock_ids=[rec.stock_id for rec in recs],
            )

        count = 0
        for rec in recommendations:
            reason_text = " | ".join(rec.reasons[:3])

            potential_return = None
            if rec.target_price and rec.current_price:
                potential_return = (
                    (rec.target_price - rec.current_price) / rec.current_price
                ) * 100

            fact_rec = FactRecommendation(
                stock_id=rec.stock_id,
                recommendation_date=rec.recommendation_date,
                signal_type=rec.signal_type.value,
                confidence_score=Decimal(str(rec.confidence * 100)),
                overall_score=Decimal(str(rec.score)),
                score_category=rec.score_category.value,
                current_price=rec.current_price,
                target_price=rec.target_price,
                stop_loss=rec.stop_loss,
                potential_return_pct=Decimal(str(potential_return)) if potential_return is not None else None,
                risk_level=rec.risk_level,
                recommendation_reason=reason_text,
                technical_score=Decimal(str(rec.stock_score.technical_score)),
                momentum_score=Decimal(str(rec.stock_score.momentum_score)),
                volatility_score=Decimal(str(rec.stock_score.volatility_score)),
                trend_score=Decimal(str(rec.stock_score.trend_score)),
                volume_score=Decimal(str(rec.stock_score.volume_score)),
                rsi_value=Decimal(str(rec.indicators.get('rsi_14')))
                if 'rsi_14' in rec.indicators else None,
                macd_value=Decimal(str(rec.indicators.get('macd')))
                if 'macd' in rec.indicators else None,
                is_active=True,
            )
            self.session.add(fact_rec)
            count += 1

        self.session.flush()
        return count
    
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
        
        return query.order_by(desc(FactRecommendation.overall_score)).all()
    
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
            query = query.filter(FactRecommendation.signal_type == signal_type)
        
        return query.options(
            joinedload(FactRecommendation.stock)
        ).order_by(
            desc(FactRecommendation.recommendation_date),
            desc(FactRecommendation.overall_score)
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
            query = query.filter(FactRecommendation.signal_type == signal_type)
        
        return query.options(
            joinedload(FactRecommendation.stock)
        ).order_by(
            desc(FactRecommendation.overall_score)
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
        return self.session.query(FactRecommendation).filter(
            and_(
                FactRecommendation.recommendation_date == recommendation_date,
                FactRecommendation.signal_type.in_(['BUY', 'STRONG_BUY'])
                    if signal_type == 'BUY'
                    else FactRecommendation.signal_type == signal_type
            )
        ).options(
            joinedload(FactRecommendation.stock)
        ).order_by(
            desc(FactRecommendation.overall_score),
            desc(FactRecommendation.confidence_score)
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
                                PARTITION BY stock_id, recommendation_date
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
