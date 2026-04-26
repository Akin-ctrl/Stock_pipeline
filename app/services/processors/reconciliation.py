"""
Reconciliation engine for multi-source data consolidation.

Compares prices from different sources, applies resolution rules,
and creates audit trails for data quality tracking.
"""

from typing import List, Dict, Optional, Tuple
from datetime import date
from decimal import Decimal
from dataclasses import dataclass

from app.models.staging import StagingDailyPrice
from app.repositories.staging_repository import StagingRepository
from app.repositories.stock_repository import StockRepository
from app.utils.logger import get_logger

logger = get_logger(__name__)


@dataclass
class ReconciliationResult:
    """Result of reconciling prices from multiple sources."""
    stock_code: str
    price_date: date
    sources: List[str]
    prices: List[Decimal]
    selected_price: Decimal
    selected_source: str
    resolution_method: str
    conflict_severity: str
    notes: Optional[str] = None
    staging_ids: Optional[List[int]] = None


class ReconciliationEngine:
    """
    Engine for reconciling stock prices from multiple data sources.
    
    Applies configurable rules to resolve price conflicts and maintains
    audit trails for all reconciliation decisions.
    
    Resolution Rules:
        - < 1% variance: Average the prices
        - 1-3% variance: Prefer afrimarket source
        - > 3% variance: Flag for manual review
    """
    
    def __init__(
        self,
        staging_repo: Optional[StagingRepository] = None,
        stock_repo: Optional[StockRepository] = None,
        low_variance_threshold: float = 1.0,
        medium_variance_threshold: float = 3.0,
        preferred_source: str = 'afrimarket'
    ):
        """
        Initialize reconciliation engine.
        
        Args:
            staging_repo: Staging data repository
            stock_repo: Stock repository for lookups
            low_variance_threshold: Threshold for low variance (%)
            medium_variance_threshold: Threshold for medium variance (%)
            preferred_source: Preferred source for medium variance conflicts
        """
        self.staging_repo = staging_repo
        self.stock_repo = stock_repo
        self.low_threshold = Decimal(str(low_variance_threshold))
        self.medium_threshold = Decimal(str(medium_variance_threshold))
        self.preferred_source = preferred_source
        self.logger = logger
    
    def reconcile_date(self, price_date: date) -> List[ReconciliationResult]:
        """
        Reconcile all stock prices for a specific date.
        
        Args:
            price_date: Date to reconcile
            
        Returns:
            List of reconciliation results
        """
        self.logger.info(f"Starting reconciliation for {price_date}")
        
        # Get conflicts (multiple sources for same stock/date)
        conflicts = self.staging_repo.get_conflicts(price_date)
        
        results = []
        for stock_code, dt, staging_records in conflicts:
            result = self.reconcile_stock_price(stock_code, dt, staging_records)
            if result:
                results.append(result)
        
        # Process single-source records (no conflicts)
        single_source = self._get_single_source_records(price_date)
        for record in single_source:
            result = self._process_single_source(record)
            if result:
                results.append(result)
        
        self.logger.info(
            f"Reconciliation complete for {price_date}: "
            f"{len(results)} records processed ({len(conflicts)} conflicts)"
        )
        
        return results
    
    def reconcile_stock_price(
        self,
        stock_code: str,
        price_date: date,
        staging_records: List[StagingDailyPrice]
    ) -> Optional[ReconciliationResult]:
        """
        Reconcile prices for a single stock from multiple sources.
        
        Args:
            stock_code: Stock ticker
            price_date: Trading date
            staging_records: List of staging records from different sources
            
        Returns:
            ReconciliationResult or None if reconciliation fails
        """
        if not staging_records:
            return None
        
        # Extract prices and sources
        sources = [r.source for r in staging_records]
        prices = [r.close_price for r in staging_records]
        
        # Calculate variance
        variance_pct = self._calculate_variance(prices)
        
        # Apply resolution rules
        if variance_pct < self.low_threshold:
            # Low variance: average prices
            selected_price = self._average_prices(prices)
            selected_source = 'averaged'
            resolution_method = 'average'
            conflict_severity = 'low'
            notes = f"Variance {variance_pct:.2f}% < {self.low_threshold}%: averaged prices"
            
        elif variance_pct < self.medium_threshold:
            # Medium variance: prefer trusted source
            selected_price, selected_source = self._prefer_source(
                staging_records, self.preferred_source
            )
            resolution_method = 'prefer_source'
            conflict_severity = 'medium'
            notes = (
                f"Variance {variance_pct:.2f}% between {self.low_threshold}% and "
                f"{self.medium_threshold}%: preferred {selected_source}"
            )
            
        else:
            # High variance: flag for review
            selected_price, selected_source = self._prefer_source(
                staging_records, self.preferred_source
            )
            resolution_method = 'prefer_source'
            conflict_severity = 'high'
            notes = (
                f"HIGH VARIANCE {variance_pct:.2f}% > {self.medium_threshold}%: "
                f"needs manual review. Defaulting to {selected_source}"
            )
            self.logger.warning(
                f"High price variance for {stock_code} on {price_date}: {variance_pct:.2f}%"
            )
        
        result = ReconciliationResult(
            stock_code=stock_code,
            price_date=price_date,
            sources=sources,
            prices=prices,
            selected_price=selected_price,
            selected_source=selected_source,
            resolution_method=resolution_method,
            conflict_severity=conflict_severity,
            notes=notes,
            staging_ids=[record.staging_id for record in staging_records]
        )
        
        self.logger.debug(
            f"Reconciled {stock_code} on {price_date}: "
            f"{len(sources)} sources, variance {variance_pct:.2f}%, "
            f"selected {selected_price} from {selected_source}"
        )
        
        return result
    
    def apply_reconciliation(
        self,
        result: ReconciliationResult
    ) -> bool:
        """
        Apply reconciliation result: create audit log and promote to production.
        
        Args:
            result: Reconciliation result to apply
            
        Returns:
            True if successful, False otherwise
        """
        try:
            if not result.staging_ids:
                self.logger.warning(
                    f"No staging IDs found for {result.stock_code} on {result.price_date}"
                )
                return False

            if result.resolution_method == 'average':
                selected_id = result.staging_ids[0]
                (
                    self.staging_repo.session.query(StagingDailyPrice)
                    .filter(StagingDailyPrice.staging_id == selected_id)
                    .update(
                        {'close_price': result.selected_price},
                        synchronize_session=False
                    )
                )
            
            # Create audit log
            self.staging_repo.create_audit_log(
                stock_code=result.stock_code,
                price_date=result.price_date,
                sources=result.sources,
                prices=result.prices,
                resolution_method=result.resolution_method,
                selected_price=result.selected_price,
                selected_source=result.selected_source,
                conflict_severity=result.conflict_severity,
                notes=result.notes,
                commit=False
            )
            
            # Mark staging records as reconciled
            self.staging_repo.mark_reconciled(result.staging_ids, result.notes, commit=False)
            
            return True
            
        except Exception as e:
            self.logger.error(
                f"Failed to apply reconciliation for {result.stock_code} "
                f"on {result.price_date}: {e}"
            )
            return False
    
    def _calculate_variance(self, prices: List[Decimal]) -> Decimal:
        """
        Calculate percentage variance between prices.
        
        Args:
            prices: List of prices
            
        Returns:
            Maximum percentage variance
        """
        if len(prices) <= 1:
            return Decimal('0.0')
        
        min_price = min(prices)
        max_price = max(prices)
        
        if min_price == 0:
            return Decimal('100.0')  # Avoid division by zero
        
        variance = ((max_price - min_price) / min_price) * 100
        return variance
    
    def _average_prices(self, prices: List[Decimal]) -> Decimal:
        """Calculate average of prices."""
        return sum(prices) / len(prices)
    
    def _prefer_source(
        self,
        staging_records: List[StagingDailyPrice],
        preferred_source: str
    ) -> Tuple[Decimal, str]:
        """
        Select price from preferred source.
        
        Args:
            staging_records: List of staging records
            preferred_source: Preferred source name
            
        Returns:
            Tuple of (selected_price, source_name)
        """
        # Try to find preferred source
        for record in staging_records:
            if record.source == preferred_source:
                return (record.close_price, record.source)
        
        # Fallback to first record if preferred not found
        return (staging_records[0].close_price, staging_records[0].source)
    
    def _get_single_source_records(self, price_date: date) -> List[StagingDailyPrice]:
        """Get staging records with only one source (no conflicts)."""
        unreconciled = self.staging_repo.get_unreconciled(price_date=price_date)
        
        # Group by stock_code
        grouped: Dict[str, List[StagingDailyPrice]] = {}
        for record in unreconciled:
            if record.stock_code not in grouped:
                grouped[record.stock_code] = []
            grouped[record.stock_code].append(record)
        
        # Return only single-source records
        single_source = []
        for stock_code, records in grouped.items():
            if len(records) == 1:
                single_source.append(records[0])
        
        return single_source
    
    def _process_single_source(
        self,
        staging_record: StagingDailyPrice
    ) -> Optional[ReconciliationResult]:
        """
        Process a staging record with only one source (no conflict).
        
        Args:
            staging_record: Single staging record
            
        Returns:
            ReconciliationResult
        """
        result = ReconciliationResult(
            stock_code=staging_record.stock_code,
            price_date=staging_record.price_date,
            sources=[staging_record.source],
            prices=[staging_record.close_price],
            selected_price=staging_record.close_price,
            selected_source=staging_record.source,
            resolution_method='single_source',
            conflict_severity='low',
            notes=f"Single source: {staging_record.source}",
            staging_ids=[staging_record.staging_id]
        )
        
        return result
    
    def get_reconciliation_stats(self, price_date: date) -> Dict[str, any]:
        """
        Get reconciliation statistics for a date.
        
        Args:
            price_date: Date to analyze
            
        Returns:
            Dictionary with statistics
        """
        results = self.reconcile_date(price_date)
        
        total = len(results)
        by_severity = {
            'low': len([r for r in results if r.conflict_severity == 'low']),
            'medium': len([r for r in results if r.conflict_severity == 'medium']),
            'high': len([r for r in results if r.conflict_severity == 'high']),
            'critical': len([r for r in results if r.conflict_severity == 'critical'])
        }
        
        by_method = {
            'single_source': len([r for r in results if r.resolution_method == 'single_source']),
            'average': len([r for r in results if r.resolution_method == 'average']),
            'prefer_source': len([r for r in results if r.resolution_method == 'prefer_source']),
            'manual': len([r for r in results if r.resolution_method == 'manual'])
        }
        
        return {
            'date': price_date,
            'total_reconciled': total,
            'by_severity': by_severity,
            'by_method': by_method,
            'high_variance_count': by_severity['high'] + by_severity.get('critical', 0)
        }
