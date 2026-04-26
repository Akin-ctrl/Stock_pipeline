"""
Repository for price data (fact_daily_prices) operations.

Handles all database operations related to daily stock price history.
"""

from typing import Optional, List, Dict, Any
from datetime import date, datetime, timedelta
from sqlalchemy.orm import Session, joinedload
from sqlalchemy import and_, desc, func, or_, text
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

    TRUSTED_BAR_STATUSES = ("RECONCILED", "OFFICIAL")
    TRUSTED_QUALITY_FLAGS = ("GOOD", "INCOMPLETE")
    
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

    def _apply_trusted_filters(
        self,
        query,
        min_confidence: Optional[float] = 65.0,
        require_complete: bool = False,
        require_volume: bool = False,
    ):
        """Apply default production trust filters for financially safer reads."""
        query = query.filter(
            FactDailyPrice.bar_status.in_(self.TRUSTED_BAR_STATUSES),
            FactDailyPrice.data_quality_flag.in_(self.TRUSTED_QUALITY_FLAGS),
        )

        if min_confidence is not None:
            query = query.filter(
                or_(
                    FactDailyPrice.confidence_score.is_(None),
                    FactDailyPrice.confidence_score >= min_confidence,
                )
            )

        if require_complete:
            query = query.filter(FactDailyPrice.has_complete_data.is_(True))

        if require_volume:
            query = query.filter(FactDailyPrice.volume.isnot(None))

        return query
    
    def get_latest_by_code(
        self,
        stock_code: str,
        as_of_date: Optional[date] = None
    ) -> Optional[FactDailyPrice]:
        """
        Get most recent price by stock code.
        
        Args:
            stock_code: Stock ticker
            as_of_date: Optional date cutoff (price_date <= as_of_date)
            
        Returns:
            Latest price record or None
        """
        query = (
            self.session.query(FactDailyPrice)
            .join(DimStock)
            .filter(DimStock.stock_code == stock_code.upper())
        )

        if as_of_date:
            query = query.filter(FactDailyPrice.price_date <= as_of_date)

        return query.order_by(desc(FactDailyPrice.price_date)).first()
    
    def get_latest_price(
        self,
        stock_id: int,
        as_of_date: Optional[date] = None
    ) -> Optional[FactDailyPrice]:
        """
        Backward-compatible latest price lookup by stock ID.

        Args:
            stock_id: Stock identifier
            as_of_date: Optional date cutoff (price_date <= as_of_date)

        Returns:
            Latest price record or None
        """
        query = (
            self.session.query(FactDailyPrice)
            .filter(FactDailyPrice.stock_id == stock_id)
        )

        if as_of_date:
            query = query.filter(FactDailyPrice.price_date <= as_of_date)

        return query.order_by(desc(FactDailyPrice.price_date)).first()

    def get_previous_price(
        self,
        stock_id: int,
        before_date: date
    ) -> Optional[FactDailyPrice]:
        """Get the latest stored price strictly before a given date."""
        return (
            self.session.query(FactDailyPrice)
            .filter(
                FactDailyPrice.stock_id == stock_id,
                FactDailyPrice.price_date < before_date,
            )
            .order_by(desc(FactDailyPrice.price_date))
            .first()
        )

    def get_first_price_of_year(
        self,
        stock_id: int,
        year: int,
        through_date: Optional[date] = None,
    ) -> Optional[FactDailyPrice]:
        """Get the first stored price in a calendar year for a stock."""
        start_of_year = date(year, 1, 1)
        query = (
            self.session.query(FactDailyPrice)
            .filter(
                FactDailyPrice.stock_id == stock_id,
                FactDailyPrice.price_date >= start_of_year,
            )
            .order_by(FactDailyPrice.price_date.asc())
        )

        if through_date is not None:
            query = query.filter(FactDailyPrice.price_date <= through_date)

        return query.first()

    def get_latest_trusted_price(
        self,
        stock_id: int,
        as_of_date: Optional[date] = None,
        min_confidence: Optional[float] = 65.0,
        require_complete: bool = False,
    ) -> Optional[FactDailyPrice]:
        """Get the most recent trusted production price for a stock."""
        query = (
            self.session.query(FactDailyPrice)
            .filter(FactDailyPrice.stock_id == stock_id)
        )

        if as_of_date:
            query = query.filter(FactDailyPrice.price_date <= as_of_date)

        query = self._apply_trusted_filters(
            query,
            min_confidence=min_confidence,
            require_complete=require_complete,
        )

        return query.order_by(desc(FactDailyPrice.price_date)).first()

    def get_latest_trusted_by_code(
        self,
        stock_code: str,
        as_of_date: Optional[date] = None,
        min_confidence: Optional[float] = 65.0,
        require_complete: bool = False,
    ) -> Optional[FactDailyPrice]:
        """Get the most recent trusted production price by stock code."""
        query = (
            self.session.query(FactDailyPrice)
            .join(DimStock)
            .filter(DimStock.stock_code == stock_code.upper())
        )

        if as_of_date:
            query = query.filter(FactDailyPrice.price_date <= as_of_date)

        query = self._apply_trusted_filters(
            query,
            min_confidence=min_confidence,
            require_complete=require_complete,
        )

        return query.order_by(desc(FactDailyPrice.price_date)).first()

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

    def get_trusted_price_history(
        self,
        stock_id: int,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        limit: Optional[int] = None,
        min_confidence: Optional[float] = 65.0,
        require_complete: bool = False,
        require_volume: bool = False,
    ) -> List[FactDailyPrice]:
        """
        Get financially safer production price history using trust filters.

        Returns records ordered by date descending, matching `get_price_history`.
        """
        query = (
            self.session.query(FactDailyPrice)
            .filter(FactDailyPrice.stock_id == stock_id)
        )

        if start_date:
            query = query.filter(FactDailyPrice.price_date >= start_date)
        if end_date:
            query = query.filter(FactDailyPrice.price_date <= end_date)

        query = self._apply_trusted_filters(
            query,
            min_confidence=min_confidence,
            require_complete=require_complete,
            require_volume=require_volume,
        )
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
            DataFrame with columns: date, close, volume, change_1d_pct
        """
        prices = self.get_price_history(stock_id, start_date, end_date)
        
        if not prices:
            return pd.DataFrame()
        
        data = {
            'date': [p.price_date for p in prices],
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
        Uses PostgreSQL INSERT ... ON CONFLICT DO UPDATE for upsert behavior.
        
        Args:
            price_data: List of dicts with price information
                Required keys: stock_id, price_date, close_price, source
                Optional: change_1d_pct, change_ytd_pct, etc.
                
        Returns:
            Number of records inserted/updated
        """
        if not price_data:
            return 0
        
        # Use SQLAlchemy's insert with on_conflict_do_update
        from sqlalchemy.dialects.postgresql import insert
        
        stmt = insert(FactDailyPrice).values(price_data)
        
        # On conflict (stock_id, price_date), update all fields
        stmt = stmt.on_conflict_do_update(
            index_elements=['stock_id', 'price_date'],
            set_={
                'close_price': stmt.excluded.close_price,
                'volume': stmt.excluded.volume,
                'change_1d_pct': stmt.excluded.change_1d_pct,
                'change_ytd_pct': stmt.excluded.change_ytd_pct,
                'source': stmt.excluded.source,
                'source_count': stmt.excluded.source_count,
                'bar_status': stmt.excluded.bar_status,
                'is_official': stmt.excluded.is_official,
                'confidence_score': stmt.excluded.confidence_score,
                'data_quality_flag': stmt.excluded.data_quality_flag,
                'has_complete_data': stmt.excluded.has_complete_data,
            }
        )
        
        self.session.execute(stmt)
        
        self.logger.info(
            f"Bulk upserted {len(price_data)} price records",
            extra={"count": len(price_data)}
        )
        
        return len(price_data)

    def repair_quality_metadata(
        self,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
    ) -> int:
        """
        Recompute in-place quality fields for existing fact_daily_prices rows.

        This is a safe maintenance operation for rows that were loaded before the
        corrected completeness logic was introduced.
        """
        conditions = []
        params: Dict[str, Any] = {}

        if start_date is not None:
            conditions.append("price_date >= :start_date")
            params["start_date"] = start_date

        if end_date is not None:
            conditions.append("price_date <= :end_date")
            params["end_date"] = end_date

        where_clause = ""
        if conditions:
            where_clause = "WHERE " + " AND ".join(conditions)

        result = self.session.execute(
            text(
                f"""
                UPDATE fact_daily_prices
                SET
                    has_complete_data = (
                        close_price IS NOT NULL
                        AND change_1d_pct IS NOT NULL
                        AND change_ytd_pct IS NOT NULL
                    ),
                    data_quality_flag = CASE
                        WHEN close_price IS NULL THEN 'POOR'
                        WHEN close_price IS NOT NULL
                             AND change_1d_pct IS NOT NULL
                             AND change_ytd_pct IS NOT NULL THEN 'GOOD'
                        ELSE 'INCOMPLETE'
                    END,
                    confidence_score = CASE
                        WHEN close_price IS NULL THEN 20.00
                        WHEN close_price IS NOT NULL
                             AND change_1d_pct IS NOT NULL
                             AND change_ytd_pct IS NOT NULL THEN 85.00
                        ELSE 70.00
                    END
                {where_clause}
                """
            ),
            params,
        )

        return result.rowcount or 0
    
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
            **kwargs: Additional optional fields (volume, change_1d_pct, change_ytd_pct, etc.)
            
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
