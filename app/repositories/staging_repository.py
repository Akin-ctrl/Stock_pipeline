"""
Repository for staging data operations.

Handles database operations for staging_daily_prices and staging_audit_log tables.
Supports bulk operations for efficient data loading and reconciliation workflows.
"""

from typing import Optional, List, Dict, Any, Tuple
from datetime import date, datetime
from sqlalchemy.orm import Session
from sqlalchemy import and_, desc, func, or_
from sqlalchemy.dialects.postgresql import insert
from decimal import Decimal
import pandas as pd

from app.repositories.base import BaseRepository
from app.models.staging import StagingDailyPrice, StagingAuditLog
from app.models import FactDailyPrice, DimStock
from app.utils.exceptions import DatabaseError, DuplicateDataError
from app.utils.logger import get_logger

logger = get_logger(__name__)


class StagingRepository(BaseRepository[StagingDailyPrice]):
    """
    Repository for staging table operations.
    
    Manages the staging area for multi-source data reconciliation.
    Provides methods for bulk loading, conflict detection, and promotion to production.
    """
    
    def __init__(self, session: Session):
        """
        Initialize staging repository.
        
        Args:
            session: Active database session
        """
        super().__init__(StagingDailyPrice, session)
    
    def bulk_insert_staging(
        self,
        df: pd.DataFrame,
        source: str,
        batch_size: int = 1000
    ) -> int:
        """
        Bulk insert price data into staging table.
        
        Args:
            df: DataFrame with columns: stock_code, price_date, close_price,
                change_1d_pct, change_ytd_pct, volume
            source: Data source identifier ('afrimarket')
            batch_size: Records per batch for insertion
            
        Returns:
            Number of records inserted
            
        Raises:
            DatabaseError: If insertion fails
        """
        try:
            records: List[Dict[str, Any]] = []
            loaded_at = datetime.now()
            total_inserted = 0
            
            for _, row in df.iterrows():
                record = {
                    "stock_code": str(row['stock_code']).upper().strip(),
                    "source": source,
                    "price_date": row['price_date'],
                    "close_price": Decimal(str(row['close_price'])),
                    "loaded_at": loaded_at,
                    "reconciled": False,
                    "change_1d_pct": None,
                    "change_ytd_pct": None,
                    "volume": None,
                }

                if pd.notna(row.get('change_1d_pct')):
                    record["change_1d_pct"] = Decimal(str(row.get('change_1d_pct')))
                if pd.notna(row.get('change_ytd_pct')):
                    record["change_ytd_pct"] = Decimal(str(row.get('change_ytd_pct')))
                if pd.notna(row.get('volume')):
                    record["volume"] = int(row.get('volume'))
                records.append(record)
                
                if len(records) >= batch_size:
                    stmt = insert(StagingDailyPrice).values(records)
                    stmt = stmt.on_conflict_do_nothing(
                        index_elements=["stock_code", "price_date", "source"]
                    )
                    result = self.session.execute(stmt)
                    self.session.commit()
                    inserted = result.rowcount or 0
                    total_inserted += inserted
                    logger.debug(
                        f"Inserted batch of {inserted} records (ignored duplicates: {len(records) - inserted})"
                    )
                    records = []
            
            if records:
                stmt = insert(StagingDailyPrice).values(records)
                stmt = stmt.on_conflict_do_nothing(
                    index_elements=["stock_code", "price_date", "source"]
                )
                result = self.session.execute(stmt)
                self.session.commit()
                inserted = result.rowcount or 0
                total_inserted += inserted
                logger.debug(
                    f"Inserted final batch of {inserted} records (ignored duplicates: {len(records) - inserted})"
                )
            
            logger.info(f"Bulk inserted {total_inserted} records from {source}")
            
            return total_inserted
            
        except Exception as e:
            self.session.rollback()
            logger.error(f"Failed to bulk insert staging data: {e}")
            raise DatabaseError(f"Staging bulk insert failed: {str(e)}") from e

    def count_by_date_source(self, price_date: date, source: str) -> int:
        """
        Count staging records for a date and source.

        Args:
            price_date: Trading date
            source: Data source identifier

        Returns:
            Number of staging records
        """
        return (
            self.session.query(func.count(StagingDailyPrice.staging_id))
            .filter(
                StagingDailyPrice.price_date == price_date,
                StagingDailyPrice.source == source
            )
            .scalar()
            or 0
        )
    
    def get_unreconciled(
        self,
        price_date: Optional[date] = None,
        stock_code: Optional[str] = None,
        limit: Optional[int] = None
    ) -> List[StagingDailyPrice]:
        """
        Get unreconciled staging records.
        
        Args:
            price_date: Filter by specific date (optional)
            stock_code: Filter by stock code (optional)
            limit: Maximum records to return
            
        Returns:
            List of unreconciled staging records
        """
        query = self.session.query(StagingDailyPrice).filter(
            StagingDailyPrice.reconciled == False
        )
        
        if price_date:
            query = query.filter(StagingDailyPrice.price_date == price_date)
        
        if stock_code:
            query = query.filter(StagingDailyPrice.stock_code == stock_code.upper())
        
        query = query.order_by(
            StagingDailyPrice.price_date.desc(),
            StagingDailyPrice.stock_code
        )
        
        if limit:
            query = query.limit(limit)
        
        return query.all()
    
    def get_unreconciled_count(
        self,
        price_date: Optional[date] = None,
        stock_code: Optional[str] = None
    ) -> int:
        """
        Get count of unreconciled staging records.
        
        Args:
            price_date: Filter by specific date (optional)
            stock_code: Filter by stock code (optional)
            
        Returns:
            Count of unreconciled records
        """
        query = self.session.query(StagingDailyPrice).filter(
            StagingDailyPrice.reconciled == False
        )
        
        if price_date:
            query = query.filter(StagingDailyPrice.price_date == price_date)
        
        if stock_code:
            query = query.filter(StagingDailyPrice.stock_code == stock_code.upper())
        
        return query.count()
    
    def get_unreconciled_dates(self) -> List[date]:
        """
        Get all distinct dates that have unreconciled staging records.
        
        Returns:
            List of dates with unreconciled data
        """
        try:
            dates = (
                self.session.query(StagingDailyPrice.price_date)
                .filter(StagingDailyPrice.reconciled == False)
                .distinct()
                .order_by(StagingDailyPrice.price_date)
                .all()
            )
            
            # Extract date values from tuples
            date_list = [d[0] for d in dates]
            
            logger.info(f"Found {len(date_list)} dates with unreconciled records: {date_list}")
            
            return date_list
            
        except Exception as e:
            logger.error(f"Failed to get unreconciled dates: {e}")
            raise DatabaseError(f"Failed to query unreconciled dates: {str(e)}") from e
    
    def get_all_reconciled(
        self,
        limit: Optional[int] = None
    ) -> List[StagingDailyPrice]:
        """
        Get all reconciled staging records (all dates).
        
        Args:
            limit: Maximum records to return (optional)
            
        Returns:
            List of reconciled staging records
        """
        query = self.session.query(StagingDailyPrice).filter(
            StagingDailyPrice.reconciled == True
        )
        
        query = query.order_by(
            StagingDailyPrice.price_date.desc(),
            StagingDailyPrice.stock_code
        )
        
        if limit:
            query = query.limit(limit)
        
        return query.all()
    
    def get_conflicts(
        self,
        price_date: date,
        stock_code: Optional[str] = None
    ) -> List[Tuple[str, date, List[StagingDailyPrice]]]:
        """
        Find staging records with conflicts (multiple sources for same stock/date).
        
        Args:
            price_date: Date to check for conflicts
            stock_code: Optional stock code filter
            
        Returns:
            List of tuples: (stock_code, date, [staging_records])
        """
        query = self.session.query(StagingDailyPrice).filter(
            and_(
                StagingDailyPrice.price_date == price_date,
                StagingDailyPrice.reconciled == False
            )
        )
        
        if stock_code:
            query = query.filter(StagingDailyPrice.stock_code == stock_code.upper())
        
        records = query.all()
        
        # Group by stock_code and date
        conflicts_map: Dict[Tuple[str, date], List[StagingDailyPrice]] = {}
        
        for record in records:
            key = (record.stock_code, record.price_date)
            if key not in conflicts_map:
                conflicts_map[key] = []
            conflicts_map[key].append(record)
        
        # Filter only conflicts (multiple sources)
        conflicts = [
            (stock_code, dt, recs)
            for (stock_code, dt), recs in conflicts_map.items()
            if len(recs) > 1
        ]
        
        return conflicts
    
    def mark_reconciled(
        self,
        staging_ids: List[int],
        reconciliation_notes: Optional[str] = None
    ) -> int:
        """
        Mark staging records as reconciled.
        
        Args:
            staging_ids: List of staging record IDs
            reconciliation_notes: Optional notes about reconciliation
            
        Returns:
            Number of records updated
        """
        try:
            updated = (
                self.session.query(StagingDailyPrice)
                .filter(StagingDailyPrice.staging_id.in_(staging_ids))
                .update(
                    {
                        'reconciled': True,
                        'promoted_at': datetime.now(),
                        'reconciliation_notes': reconciliation_notes
                    },
                    synchronize_session=False
                )
            )
            
            self.session.commit()
            logger.info(f"Marked {updated} staging records as reconciled")
            
            return updated
            
        except Exception as e:
            self.session.rollback()
            logger.error(f"Failed to mark records as reconciled: {e}")
            raise DatabaseError(f"Failed to update staging records: {str(e)}") from e
    
    def get_by_date_reconciled(
        self,
        price_date: date,
        reconciled_only: bool = True
    ) -> List[StagingDailyPrice]:
        """
        Get staging records by date, optionally filtered to reconciled only.
        
        Args:
            price_date: Date to filter by
            reconciled_only: If True, only return reconciled records
            
        Returns:
            List of staging records
        """
        query = self.session.query(StagingDailyPrice).filter(
            StagingDailyPrice.price_date == price_date
        )
        
        if reconciled_only:
            query = query.filter(StagingDailyPrice.reconciled == True)
        
        query = query.order_by(StagingDailyPrice.stock_code)
        
        return query.all()
    
    def create_audit_log(
        self,
        stock_code: str,
        price_date: date,
        sources: List[str],
        prices: List[Decimal],
        resolution_method: str,
        selected_price: Decimal,
        selected_source: Optional[str],
        conflict_severity: str,
        notes: Optional[str] = None
    ) -> StagingAuditLog:
        """
        Create an audit log entry for reconciliation.
        
        Args:
            stock_code: Stock ticker
            price_date: Trading date
            sources: List of data sources
            prices: List of prices from each source
            resolution_method: How conflict was resolved
            selected_price: Final price selected
            selected_source: Source of final price
            conflict_severity: Severity level
            notes: Additional notes
            
        Returns:
            Created audit log record
        """
        try:
            audit_log = StagingAuditLog(
                stock_code=stock_code,
                price_date=price_date,
                sources=sources,
                prices=prices,
                resolution_method=resolution_method,
                selected_price=selected_price,
                selected_source=selected_source,
                conflict_severity=conflict_severity,
                notes=notes
            )
            
            self.session.add(audit_log)
            self.session.commit()
            
            logger.debug(
                f"Created audit log for {stock_code} on {price_date}: "
                f"{conflict_severity} severity, {resolution_method}"
            )
            
            return audit_log
            
        except Exception as e:
            self.session.rollback()
            logger.error(f"Failed to create audit log: {e}")
            raise DatabaseError(f"Audit log creation failed: {str(e)}") from e
    
    def get_staging_summary(self, price_date: date) -> Dict[str, Any]:
        """
        Get summary statistics for staging data on a specific date.
        
        Args:
            price_date: Date to summarize
            
        Returns:
            Dictionary with summary statistics
        """
        total = (
            self.session.query(func.count(StagingDailyPrice.staging_id))
            .filter(StagingDailyPrice.price_date == price_date)
            .scalar()
        )
        
        reconciled = (
            self.session.query(func.count(StagingDailyPrice.staging_id))
            .filter(
                and_(
                    StagingDailyPrice.price_date == price_date,
                    StagingDailyPrice.reconciled == True
                )
            )
            .scalar()
        )
        
        by_source = (
            self.session.query(
                StagingDailyPrice.source,
                func.count(StagingDailyPrice.staging_id)
            )
            .filter(StagingDailyPrice.price_date == price_date)
            .group_by(StagingDailyPrice.source)
            .all()
        )
        
        return {
            'date': price_date,
            'total_records': total or 0,
            'reconciled': reconciled or 0,
            'pending': (total or 0) - (reconciled or 0),
            'by_source': {source: count for source, count in by_source}
        }
    
    def cleanup_old_staging(self, days_to_keep: int = 60) -> int:
        """
        Delete staging records older than specified days.
        
        Args:
            days_to_keep: Number of days to retain (default 60)
            
        Returns:
            Number of records deleted
        """
        try:
            cutoff_date = date.today() - timedelta(days=days_to_keep)
            
            deleted = (
                self.session.query(StagingDailyPrice)
                .filter(
                    and_(
                        StagingDailyPrice.price_date < cutoff_date,
                        StagingDailyPrice.reconciled == True
                    )
                )
                .delete(synchronize_session=False)
            )
            
            self.session.commit()
            logger.info(f"Deleted {deleted} old staging records (older than {cutoff_date})")
            
            return deleted
            
        except Exception as e:
            self.session.rollback()
            logger.error(f"Failed to cleanup old staging data: {e}")
            raise DatabaseError(f"Cleanup failed: {str(e)}") from e
