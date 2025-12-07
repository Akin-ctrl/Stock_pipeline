"""
Repository for technical indicators (fact_technical_indicators) operations.

Handles all database operations related to calculated technical indicators.
"""

from typing import Optional, List, Dict, Any
from datetime import date, timedelta
from sqlalchemy.orm import Session, joinedload
from sqlalchemy import and_, desc
import pandas as pd

from app.repositories.base import BaseRepository
from app.models import FactTechnicalIndicator, DimStock


class IndicatorRepository(BaseRepository[FactTechnicalIndicator]):
    """
    Repository for technical indicator operations.
    
    Provides methods for saving calculated indicators and retrieving
    indicator history for analysis.
    """
    
    def __init__(self, session: Session):
        """
        Initialize indicator repository.
        
        Args:
            session: Active database session
        """
        super().__init__(FactTechnicalIndicator, session)
    
    def get_latest(self, stock_id: int) -> Optional[FactTechnicalIndicator]:
        """
        Get most recent indicators for a stock.
        
        Args:
            stock_id: Stock identifier
            
        Returns:
            Latest indicator record or None
        """
        return (
            self.session.query(FactTechnicalIndicator)
            .filter(FactTechnicalIndicator.stock_id == stock_id)
            .order_by(desc(FactTechnicalIndicator.calculation_date))
            .first()
        )
    
    def get_latest_by_code(self, stock_code: str) -> Optional[FactTechnicalIndicator]:
        """
        Get most recent indicators by stock code.
        
        Args:
            stock_code: Stock ticker
            
        Returns:
            Latest indicator record or None
        """
        return (
            self.session.query(FactTechnicalIndicator)
            .join(DimStock)
            .filter(DimStock.stock_code == stock_code.upper())
            .order_by(desc(FactTechnicalIndicator.calculation_date))
            .first()
        )
    
    def get_indicator_history(
        self,
        stock_id: int,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        limit: Optional[int] = None
    ) -> List[FactTechnicalIndicator]:
        """
        Get indicator history for a stock within date range.
        
        Args:
            stock_id: Stock identifier
            start_date: Start date (inclusive)
            end_date: End date (inclusive)
            limit: Maximum records to return
            
        Returns:
            List of indicator records ordered by date descending
        """
        query = (
            self.session.query(FactTechnicalIndicator)
            .filter(FactTechnicalIndicator.stock_id == stock_id)
        )
        
        if start_date:
            query = query.filter(FactTechnicalIndicator.calculation_date >= start_date)
        if end_date:
            query = query.filter(FactTechnicalIndicator.calculation_date <= end_date)
        
        query = query.order_by(desc(FactTechnicalIndicator.calculation_date))
        
        if limit:
            query = query.limit(limit)
        
        return query.all()
    
    def get_indicator_history_df(
        self,
        stock_id: int,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None
    ) -> pd.DataFrame:
        """
        Get indicator history as pandas DataFrame.
        
        Args:
            stock_id: Stock identifier
            start_date: Start date (inclusive)
            end_date: End date (inclusive)
            
        Returns:
            DataFrame with all indicator columns
        """
        indicators = self.get_indicator_history(stock_id, start_date, end_date)
        
        if not indicators:
            return pd.DataFrame()
        
        data = {
            'date': [i.calculation_date for i in indicators],
            'ma_7': [i.ma_7 for i in indicators],
            'ma_30': [i.ma_30 for i in indicators],
            'ma_90': [i.ma_90 for i in indicators],
            'rsi_14': [i.rsi_14 for i in indicators],
            'macd': [i.macd for i in indicators],
            'macd_signal': [i.macd_signal for i in indicators],
            'macd_histogram': [i.macd_histogram for i in indicators],
            'volatility_30': [i.volatility_30 for i in indicators],
            'ma_crossover_signal': [i.ma_crossover_signal for i in indicators],
            'trend_strength': [i.trend_strength for i in indicators],
        }
        
        df = pd.DataFrame(data)
        df = df.sort_values('date')  # Ascending for time series
        df = df.set_index('date')
        return df
    
    def save_indicators(
        self,
        stock_id: int,
        calculation_date: date,
        indicators: Dict[str, Any]
    ) -> FactTechnicalIndicator:
        """
        Save calculated indicators for a stock (upsert).
        
        If indicators for stock_id + date exist, update them.
        Otherwise, create new record.
        
        Args:
            stock_id: Stock identifier
            calculation_date: Date of calculation
            indicators: Dict with indicator values
                Keys: ma_7, ma_30, rsi_14, macd, volatility_30, etc.
                
        Returns:
            Indicator record (created or updated)
        """
        existing = (
            self.session.query(FactTechnicalIndicator)
            .filter(
                and_(
                    FactTechnicalIndicator.stock_id == stock_id,
                    FactTechnicalIndicator.calculation_date == calculation_date
                )
            )
            .first()
        )
        
        if existing:
            return self.update(existing, **indicators)
        else:
            return self.create(
                stock_id=stock_id,
                calculation_date=calculation_date,
                **indicators
            )
    
    def bulk_save_indicators(
        self,
        indicator_data: List[Dict[str, Any]]
    ) -> int:
        """
        Bulk save indicator records.
        
        Args:
            indicator_data: List of dicts with indicator information
                Required keys: stock_id, calculation_date
                Optional: ma_7, ma_30, rsi_14, macd, etc.
                
        Returns:
            Number of records saved
        """
        if not indicator_data:
            return 0
        
        instances = [FactTechnicalIndicator(**data) for data in indicator_data]
        self.bulk_insert(instances)
        
        self.logger.info(
            f"Bulk saved {len(instances)} indicator records",
            extra={"count": len(instances)}
        )
        
        return len(instances)
    
    def get_ma_crossovers(
        self,
        signal: str,
        days: int = 7
    ) -> List[FactTechnicalIndicator]:
        """
        Get recent MA crossover signals.
        
        Args:
            signal: 'BULLISH', 'BEARISH', or 'NEUTRAL'
            days: Look back N days
            
        Returns:
            List of indicator records with specified crossover signal
        """
        cutoff_date = date.today() - timedelta(days=days)
        
        return (
            self.session.query(FactTechnicalIndicator)
            .options(joinedload(FactTechnicalIndicator.stock))
            .filter(
                and_(
                    FactTechnicalIndicator.ma_crossover_signal == signal,
                    FactTechnicalIndicator.calculation_date >= cutoff_date
                )
            )
            .order_by(desc(FactTechnicalIndicator.calculation_date))
            .all()
        )
    
    def get_rsi_extremes(
        self,
        oversold_threshold: float = 30.0,
        overbought_threshold: float = 70.0,
        days: int = 7
    ) -> Dict[str, List[FactTechnicalIndicator]]:
        """
        Get stocks with extreme RSI values (oversold/overbought).
        
        Args:
            oversold_threshold: RSI below this is oversold (default 30)
            overbought_threshold: RSI above this is overbought (default 70)
            days: Look back N days
            
        Returns:
            Dict with 'oversold' and 'overbought' lists
        """
        cutoff_date = date.today() - timedelta(days=days)
        
        oversold = (
            self.session.query(FactTechnicalIndicator)
            .options(joinedload(FactTechnicalIndicator.stock))
            .filter(
                and_(
                    FactTechnicalIndicator.rsi_14 <= oversold_threshold,
                    FactTechnicalIndicator.calculation_date >= cutoff_date
                )
            )
            .order_by(FactTechnicalIndicator.rsi_14)
            .all()
        )
        
        overbought = (
            self.session.query(FactTechnicalIndicator)
            .options(joinedload(FactTechnicalIndicator.stock))
            .filter(
                and_(
                    FactTechnicalIndicator.rsi_14 >= overbought_threshold,
                    FactTechnicalIndicator.calculation_date >= cutoff_date
                )
            )
            .order_by(desc(FactTechnicalIndicator.rsi_14))
            .all()
        )
        
        return {
            'oversold': oversold,
            'overbought': overbought
        }
    
    def get_high_volatility_stocks(
        self,
        threshold: float = 0.3,
        days: int = 7
    ) -> List[FactTechnicalIndicator]:
        """
        Get stocks with high volatility.
        
        Args:
            threshold: Volatility threshold (e.g., 0.3 = 30% annualized)
            days: Look back N days
            
        Returns:
            List of indicator records with high volatility
        """
        cutoff_date = date.today() - timedelta(days=days)
        
        return (
            self.session.query(FactTechnicalIndicator)
            .options(joinedload(FactTechnicalIndicator.stock))
            .filter(
                and_(
                    FactTechnicalIndicator.volatility_30 >= threshold,
                    FactTechnicalIndicator.calculation_date >= cutoff_date
                )
            )
            .order_by(desc(FactTechnicalIndicator.volatility_30))
            .all()
        )
    
    def delete_old_indicators(self, stock_id: int, before_date: date) -> int:
        """
        Delete indicator records older than specified date.
        
        Args:
            stock_id: Stock identifier
            before_date: Delete records before this date
            
        Returns:
            Number of records deleted
        """
        deleted = (
            self.session.query(FactTechnicalIndicator)
            .filter(
                and_(
                    FactTechnicalIndicator.stock_id == stock_id,
                    FactTechnicalIndicator.calculation_date < before_date
                )
            )
            .delete()
        )
        
        self.logger.info(
            f"Deleted {deleted} old indicator records",
            extra={"stock_id": stock_id, "before_date": str(before_date)}
        )
        
        return deleted
