"""
Repository for investment recommendations.

Handles database operations for stock recommendations.
"""

from datetime import date
from typing import List, Optional, TYPE_CHECKING
from decimal import Decimal

from sqlalchemy.orm import Session, joinedload
from sqlalchemy import and_, desc, func

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
        
        self.db.add(fact_rec)
        self.db.commit()
        self.db.refresh(fact_rec)
        
        return fact_rec
    
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
        count = 0
        for rec in recommendations:
            self.create_recommendation(rec)
            count += 1
        
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
        query = self.db.query(FactRecommendation).filter(
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
        query = self.db.query(FactRecommendation).filter(
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
        return self.db.query(FactRecommendation).filter(
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
        query = self.db.query(FactRecommendation).filter(
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
        return self.db.query(FactRecommendation).filter(
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
        
        self.db.commit()
        self.db.refresh(rec)
        
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
        
        count = self.db.query(FactRecommendation).filter(
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
        
        self.db.commit()
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
        query = self.db.query(FactRecommendation)
        
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
    
    def get_by_id(self, recommendation_id: int) -> Optional[FactRecommendation]:
        """Get recommendation by ID."""
        return self.db.query(FactRecommendation).filter(
            FactRecommendation.recommendation_id == recommendation_id
        ).options(joinedload(FactRecommendation.stock)).first()
