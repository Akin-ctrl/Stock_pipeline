"""
Repository for staging data operations.

Handles database operations for staging_daily_prices and staging_audit_log tables.
Supports bulk operations for efficient data loading and reconciliation workflows.
"""

from typing import Optional, List, Dict, Any, Tuple
from datetime import date, datetime
from sqlalchemy.orm import Session
from sqlalchemy import and_, case, desc, func, or_, select
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
            source: Data source identifier
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
                    stmt = stmt.on_conflict_do_update(
                        index_elements=["stock_code", "price_date", "source"],
                        set_={
                            "close_price": stmt.excluded.close_price,
                            "change_1d_pct": stmt.excluded.change_1d_pct,
                            "change_ytd_pct": stmt.excluded.change_ytd_pct,
                            "volume": stmt.excluded.volume,
                            "loaded_at": stmt.excluded.loaded_at,
                            "reconciled": False,
                            "promoted_at": None,
                            "reconciliation_notes": None,
                        }
                    )
                    result = self.session.execute(stmt)
                    self.session.commit()
                    inserted = result.rowcount or 0
                    total_inserted += inserted
                    logger.debug(
                        f"Upserted batch of {inserted} staging records"
                    )
                    records = []
            
            if records:
                stmt = insert(StagingDailyPrice).values(records)
                stmt = stmt.on_conflict_do_update(
                    index_elements=["stock_code", "price_date", "source"],
                    set_={
                        "close_price": stmt.excluded.close_price,
                        "change_1d_pct": stmt.excluded.change_1d_pct,
                        "change_ytd_pct": stmt.excluded.change_ytd_pct,
                        "volume": stmt.excluded.volume,
                        "loaded_at": stmt.excluded.loaded_at,
                        "reconciled": False,
                        "promoted_at": None,
                        "reconciliation_notes": None,
                    }
                )
                result = self.session.execute(stmt)
                self.session.commit()
                inserted = result.rowcount or 0
                total_inserted += inserted
                logger.debug(
                    f"Upserted final batch of {inserted} staging records"
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

    def get_reconciled_by_date(
        self,
        price_date: date
    ) -> List[StagingDailyPrice]:
        """
        Get reconciled staging records for a specific trading date (incremental processing).
        
        This method is optimized for daily DAG runs that should only process today's data,
        not the entire historical staging table.
        
        Args:
            price_date: Trading date to retrieve reconciled records for
            
        Returns:
            List of reconciled staging records for the specified date
        """
        query = self.session.query(StagingDailyPrice).filter(
            and_(
                StagingDailyPrice.reconciled == True,
                StagingDailyPrice.price_date == price_date
            )
        )
        
        query = query.order_by(StagingDailyPrice.stock_code)
        
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
        reconciliation_notes: Optional[str] = None,
        commit: bool = True
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
            
            if commit:
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
        notes: Optional[str] = None,
        commit: bool = True
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
            if commit:
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

    def get_reconciled_since(
        self,
        promoted_after: datetime,
        limit: Optional[int] = None
    ) -> List[StagingDailyPrice]:
        """
        Get staging records reconciled after a specific timestamp.

        Args:
            promoted_after: Lower bound for promoted_at timestamp
            limit: Maximum records to return (optional)

        Returns:
            List of recently reconciled staging records
        """
        query = (
            self.session.query(StagingDailyPrice)
            .filter(
                StagingDailyPrice.reconciled == True,
                StagingDailyPrice.promoted_at.isnot(None),
                StagingDailyPrice.promoted_at >= promoted_after
            )
            .order_by(
                StagingDailyPrice.price_date.desc(),
                StagingDailyPrice.stock_code
            )
        )

        if limit:
            query = query.limit(limit)

        return query.all()

    def get_reconciled_missing_from_fact(
        self,
        promoted_after: Optional[datetime] = None,
        price_dates: Optional[List[date]] = None,
        limit: Optional[int] = None,
    ) -> List[StagingDailyPrice]:
        """
        Get reconciled staging rows that have not yet been promoted to fact_daily_prices.

        This is the safe handoff query for downstream promotion because it finds the
        exact staging rows whose stock/date pair is still missing from production.
        It is especially important after historical backfills, where many old dates
        may already be reconciled but still not present in fact_daily_prices.
        """
        query = (
            self.session.query(StagingDailyPrice)
            .join(
                DimStock,
                DimStock.stock_code == StagingDailyPrice.stock_code,
            )
            .outerjoin(
                FactDailyPrice,
                and_(
                    FactDailyPrice.stock_id == DimStock.stock_id,
                    FactDailyPrice.price_date == StagingDailyPrice.price_date,
                ),
            )
            .filter(
                StagingDailyPrice.reconciled.is_(True),
                FactDailyPrice.price_id.is_(None),
            )
        )

        if promoted_after is not None:
            query = query.filter(
                StagingDailyPrice.promoted_at.isnot(None),
                StagingDailyPrice.promoted_at >= promoted_after,
            )

        if price_dates:
            query = query.filter(StagingDailyPrice.price_date.in_(price_dates))

        query = query.order_by(
            StagingDailyPrice.price_date.asc(),
            StagingDailyPrice.stock_code.asc(),
            StagingDailyPrice.promoted_at.asc().nulls_last(),
            StagingDailyPrice.staging_id.asc(),
        )

        if limit:
            query = query.limit(limit)

        return query.all()

    def get_canonical_reconciled_for_fact_sync(
        self,
        promoted_after: Optional[datetime] = None,
        price_dates: Optional[List[date]] = None,
        only_missing_from_fact: bool = False,
        limit: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """
        Return one canonical reconciled row per stock/date for fact promotion.

        The canonical price/source comes from the latest reconciliation audit log.
        This avoids promoting every reconciled source row and then relying on an
        arbitrary dataframe dedupe to pick the production record.
        """
        latest_audit_ranked = (
            select(
                StagingAuditLog.stock_code.label("stock_code"),
                StagingAuditLog.price_date.label("price_date"),
                StagingAuditLog.selected_price.label("selected_price"),
                StagingAuditLog.selected_source.label("selected_source"),
                func.row_number().over(
                    partition_by=(
                        StagingAuditLog.stock_code,
                        StagingAuditLog.price_date,
                    ),
                    order_by=(
                        StagingAuditLog.created_at.desc(),
                        StagingAuditLog.audit_id.desc(),
                    ),
                ).label("rn"),
            ).subquery("latest_audit_ranked")
        )

        latest_audit = (
            select(
                latest_audit_ranked.c.stock_code,
                latest_audit_ranked.c.price_date,
                latest_audit_ranked.c.selected_price,
                latest_audit_ranked.c.selected_source,
            )
            .where(latest_audit_ranked.c.rn == 1)
            .subquery("latest_audit")
        )

        preferred_source_rank = case(
            (
                or_(
                    latest_audit.c.selected_source.is_(None),
                    latest_audit.c.selected_source == "averaged",
                    StagingDailyPrice.source == latest_audit.c.selected_source,
                ),
                0,
            ),
            else_=1,
        )

        canonical_source = case(
            (
                latest_audit.c.selected_source.isnot(None),
                latest_audit.c.selected_source,
            ),
            else_=StagingDailyPrice.source,
        )

        candidate_rows = (
            select(
                StagingDailyPrice.stock_code.label("stock_code"),
                StagingDailyPrice.price_date.label("price_date"),
                func.coalesce(
                    latest_audit.c.selected_price,
                    StagingDailyPrice.close_price,
                ).label("close_price"),
                StagingDailyPrice.change_1d_pct.label("change_1d_pct"),
                StagingDailyPrice.change_ytd_pct.label("change_ytd_pct"),
                StagingDailyPrice.volume.label("volume"),
                canonical_source.label("source"),
                StagingDailyPrice.promoted_at.label("promoted_at"),
                func.row_number().over(
                    partition_by=(
                        StagingDailyPrice.stock_code,
                        StagingDailyPrice.price_date,
                    ),
                    order_by=(
                        preferred_source_rank.asc(),
                        StagingDailyPrice.staging_id.asc(),
                    ),
                ).label("rn"),
            )
            .select_from(StagingDailyPrice)
            .outerjoin(
                latest_audit,
                and_(
                    latest_audit.c.stock_code == StagingDailyPrice.stock_code,
                    latest_audit.c.price_date == StagingDailyPrice.price_date,
                ),
            )
            .where(StagingDailyPrice.reconciled.is_(True))
        )

        if promoted_after is not None:
            candidate_rows = candidate_rows.where(
                StagingDailyPrice.promoted_at.isnot(None),
                StagingDailyPrice.promoted_at >= promoted_after,
            )

        if price_dates:
            candidate_rows = candidate_rows.where(
                StagingDailyPrice.price_date.in_(price_dates)
            )

        if only_missing_from_fact:
            candidate_rows = (
                candidate_rows
                .join(
                    DimStock,
                    DimStock.stock_code == StagingDailyPrice.stock_code,
                )
                .outerjoin(
                    FactDailyPrice,
                    and_(
                        FactDailyPrice.stock_id == DimStock.stock_id,
                        FactDailyPrice.price_date == StagingDailyPrice.price_date,
                    ),
                )
                .where(FactDailyPrice.price_id.is_(None))
            )

        candidate_subquery = candidate_rows.subquery("canonical_candidate_rows")
        stmt = (
            select(
                candidate_subquery.c.stock_code,
                candidate_subquery.c.price_date,
                candidate_subquery.c.close_price,
                candidate_subquery.c.change_1d_pct,
                candidate_subquery.c.change_ytd_pct,
                candidate_subquery.c.volume,
                candidate_subquery.c.source,
                candidate_subquery.c.promoted_at,
            )
            .where(candidate_subquery.c.rn == 1)
            .order_by(
                candidate_subquery.c.price_date.asc(),
                candidate_subquery.c.stock_code.asc(),
            )
        )

        if limit:
            stmt = stmt.limit(limit)

        rows = self.session.execute(stmt).mappings().all()
        return [dict(row) for row in rows]
    
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
