"""
Repository for price data (fact_daily_prices) operations.

Handles all database operations related to daily stock price history.
"""

from typing import Optional, List, Dict, Any
from datetime import date, datetime, timedelta
from sqlalchemy.orm import Session, joinedload
from sqlalchemy import and_, desc, func
import pandas as pd

from app.repositories.base import BaseRepository
from app.models import FactDailyPrice, DimStock
from app.utils.exceptions import RecordNotFoundError


class PriceRepository(BaseRepository[FactDailyPrice]):
    """
    Repository for daily stock price operations.
    
    Provides methods for bulk price insertion, historical queries,
    and latest price retrieval with optimized queries.
    """
    
    def __init__(self, session: Session):
        """
        Initialize price repository.
        
        Args:
            session: Active database session
        """
        super().__init__(FactDailyPrice, session)
    
    def get_latest(self, stock_id: int) -> Optional[FactDailyPrice]:
        """
        Get most recent price for a stock.
        
        Args:
            stock_id: Stock identifier
            
        Returns:
            Latest price record or None
        """
        return (
            self.session.query(FactDailyPrice)
            .filter(FactDailyPrice.stock_id == stock_id)
            .order_by(desc(FactDailyPrice.price_date))
            .first()
        )
    
    def get_latest_by_code(self, stock_code: str) -> Optional[FactDailyPrice]:
        """
        Get most recent price by stock code.
        
        Args:
            stock_code: Stock ticker
            
        Returns:
            Latest price record or None
        """
        return (
            self.session.query(FactDailyPrice)
            .join(DimStock)
            .filter(DimStock.stock_code == stock_code.upper())
            .order_by(desc(FactDailyPrice.price_date))
            .first()
        )
    
    def get_price_history(
        self,
        stock_id: int,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        limit: Optional[int] = None
    ) -> List[FactDailyPrice]:
        """
        Get price history for a stock within date range.
        
        Args:
            stock_id: Stock identifier
            start_date: Start date (inclusive), None for all
            end_date: End date (inclusive), None for today
            limit: Maximum records to return
            
        Returns:
            List of price records ordered by date descending
        """
        query = (
            self.session.query(FactDailyPrice)
            .filter(FactDailyPrice.stock_id == stock_id)
        )
        
        if start_date:
            query = query.filter(FactDailyPrice.price_date >= start_date)
        if end_date:
            query = query.filter(FactDailyPrice.price_date <= end_date)
        
        query = query.order_by(desc(FactDailyPrice.price_date))
        
        if limit:
            query = query.limit(limit)
        
        return query.all()
    
    def get_price_history_df(
        self,
        stock_id: int,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None
    ) -> pd.DataFrame:
        """
        Get price history as pandas DataFrame.
        
        Args:
            stock_id: Stock identifier
            start_date: Start date (inclusive)
            end_date: End date (inclusive)
            
        Returns:
            DataFrame with columns: date, open, high, low, close, volume
        """
        prices = self.get_price_history(stock_id, start_date, end_date)
        
        if not prices:
            return pd.DataFrame()
        
        data = {
            'date': [p.price_date for p in prices],
            'open': [p.open_price for p in prices],
            'high': [p.high_price for p in prices],
            'low': [p.low_price for p in prices],
            'close': [p.close_price for p in prices],
            'volume': [p.volume for p in prices],
            'change_1d_pct': [p.change_1d_pct for p in prices],
        }
        
        df = pd.DataFrame(data)
        df = df.sort_values('date')  # Ascending for time series
        df = df.set_index('date')
        return df
    
    def bulk_insert_prices(self, price_data: List[Dict[str, Any]]) -> int:
        """
        Bulk insert price records from dictionary list.
        
        Args:
            price_data: List of dicts with price information
                Required keys: stock_id, price_date, close_price, source
                Optional: open_price, high_price, low_price, volume, etc.
                
        Returns:
            Number of records inserted
        """
        if not price_data:
            return 0
        
        instances = [FactDailyPrice(**data) for data in price_data]
        self.bulk_insert(instances)
        
        self.logger.info(
            f"Bulk inserted {len(instances)} price records",
            extra={"count": len(instances)}
        )
        
        return len(instances)
    
    def upsert_price(
        self,
        stock_id: int,
        price_date: date,
        close_price: float,
        source: str,
        **kwargs
    ) -> FactDailyPrice:
        """
        Insert or update a price record (upsert).
        
        If price for stock_id + price_date exists, update it.
        Otherwise, create new record.
        
        Args:
            stock_id: Stock identifier
            price_date: Date of price
            close_price: Closing price
            source: Data source name
            **kwargs: Additional fields (open_price, high_price, etc.)
            
        Returns:
            Price record (created or updated)
        """
        existing = (
            self.session.query(FactDailyPrice)
            .filter(
                and_(
                    FactDailyPrice.stock_id == stock_id,
                    FactDailyPrice.price_date == price_date
                )
            )
            .first()
        )
        
        if existing:
            return self.update(
                existing,
                close_price=close_price,
                source=source,
                **kwargs
            )
        else:
            return self.create(
                stock_id=stock_id,
                price_date=price_date,
                close_price=close_price,
                source=source,
                **kwargs
            )
    
    def get_recent_prices(
        self,
        days: int = 30,
        exchange: Optional[str] = None
    ) -> List[FactDailyPrice]:
        """
        Get all price records from last N days.
        
        Args:
            days: Number of days to look back
            exchange: Filter by exchange ('NGX' or 'LSE')
            
        Returns:
            List of recent price records
        """
        cutoff_date = date.today() - timedelta(days=days)
        
        query = (
            self.session.query(FactDailyPrice)
            .join(DimStock)
            .filter(FactDailyPrice.price_date >= cutoff_date)
        )
        
        if exchange:
            query = query.filter(DimStock.exchange == exchange.upper())
        
        return query.order_by(desc(FactDailyPrice.price_date)).all()
    
    def get_date_range(self, stock_id: int) -> Dict[str, Optional[date]]:
        """
        Get earliest and latest price dates for a stock.
        
        Args:
            stock_id: Stock identifier
            
        Returns:
            Dict with 'min_date' and 'max_date' keys
        """
        result = (
            self.session.query(
                func.min(FactDailyPrice.price_date),
                func.max(FactDailyPrice.price_date)
            )
            .filter(FactDailyPrice.stock_id == stock_id)
            .first()
        )
        
        return {
            'min_date': result[0] if result else None,
            'max_date': result[1] if result else None
        }
    
    def get_prices_by_quality(
        self,
        quality_flag: str,
        limit: int = 100
    ) -> List[FactDailyPrice]:
        """
        Get prices with specific data quality flag.
        
        Args:
            quality_flag: 'GOOD', 'SUSPICIOUS', 'MISSING', or 'STALE'
            limit: Maximum records to return
            
        Returns:
            List of price records with matching quality flag
        """
        return (
            self.session.query(FactDailyPrice)
            .options(joinedload(FactDailyPrice.stock))
            .filter(FactDailyPrice.data_quality_flag == quality_flag)
            .order_by(desc(FactDailyPrice.price_date))
            .limit(limit)
            .all()
        )
    
    def delete_old_prices(self, stock_id: int, before_date: date) -> int:
        """
        Delete price records older than specified date.
        
        Args:
            stock_id: Stock identifier
            before_date: Delete records before this date
            
        Returns:
            Number of records deleted
        """
        deleted = (
            self.session.query(FactDailyPrice)
            .filter(
                and_(
                    FactDailyPrice.stock_id == stock_id,
                    FactDailyPrice.price_date < before_date
                )
            )
            .delete()
        )
        
        self.logger.info(
            f"Deleted {deleted} old price records",
            extra={"stock_id": stock_id, "before_date": str(before_date)}
        )
        
        return deleted
