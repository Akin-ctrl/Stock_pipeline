"""Integration tests for StagingRepository."""

import pytest
from datetime import date, timedelta
from decimal import Decimal
from sqlalchemy.orm import Session

from app.models.staging import StagingDailyPrice
from app.repositories.staging_repository import StagingRepository


@pytest.fixture
def staging_data(db_session: Session):
    """Create sample staging data with multiple dates and sources."""
    staging_records = [
        # Date 1: NGX only
        StagingDailyPrice(
            stock_code="DANGCEM",
            price_date=date(2026, 2, 10),
            source="ngx",
            close_price=Decimal("450.00"),
            reconciled=False
        ),
        StagingDailyPrice(
            stock_code="ZENITH",
            price_date=date(2026, 2, 10),
            source="ngx",
            close_price=Decimal("35.50"),
            reconciled=False
        ),
        # Date 2: Afrimarket only
        StagingDailyPrice(
            stock_code="DANGCEM",
            price_date=date(2026, 2, 9),
            source="afrimarket",
            close_price=Decimal("448.00"),
            reconciled=False
        ),
        StagingDailyPrice(
            stock_code="ZENITH",
            price_date=date(2026, 2, 9),
            source="afrimarket",
            close_price=Decimal("35.20"),
            reconciled=False
        ),
        # Date 3: Both sources (conflict)
        StagingDailyPrice(
            stock_code="DANGCEM",
            price_date=date(2026, 2, 8),
            source="ngx",
            close_price=Decimal("445.00"),
            reconciled=False
        ),
        StagingDailyPrice(
            stock_code="DANGCEM",
            price_date=date(2026, 2, 8),
            source="afrimarket",
            close_price=Decimal("446.00"),
            reconciled=False
        ),
        # Reconciled record (should be excluded from unreconciled queries)
        StagingDailyPrice(
            stock_code="ZENITH",
            price_date=date(2026, 2, 7),
            source="ngx",
            close_price=Decimal("34.80"),
            reconciled=True
        ),
    ]
    
    db_session.add_all(staging_records)
    db_session.commit()
    
    return staging_records


@pytest.mark.integration
@pytest.mark.database
class TestStagingRepository:
    """Test StagingRepository operations."""
    
    def test_get_unreconciled_dates(self, db_session: Session, staging_data):
        """Test retrieving all distinct dates with unreconciled records."""
        repo = StagingRepository(db_session)
        dates = repo.get_unreconciled_dates()
        
        # Should return 3 dates (Feb 8, 9, 10) - NOT Feb 7 (reconciled)
        assert len(dates) == 3
        assert date(2026, 2, 10) in dates
        assert date(2026, 2, 9) in dates
        assert date(2026, 2, 8) in dates
        assert date(2026, 2, 7) not in dates  # Reconciled, should be excluded
        
        # Should be sorted ascending
        assert dates == sorted(dates)
    
    def test_get_unreconciled_count_all(self, db_session: Session, staging_data):
        """Test counting all unreconciled records."""
        repo = StagingRepository(db_session)
        count = repo.get_unreconciled_count()
        
        # 6 unreconciled records (excluding the 1 reconciled)
        assert count == 6
    
    def test_get_unreconciled_count_by_date(self, db_session: Session, staging_data):
        """Test counting unreconciled records for specific date."""
        repo = StagingRepository(db_session)
        
        # Feb 10: 2 records (NGX only)
        count_feb10 = repo.get_unreconciled_count(price_date=date(2026, 2, 10))
        assert count_feb10 == 2
        
        # Feb 8: 2 records (both sources - conflict)
        count_feb8 = repo.get_unreconciled_count(price_date=date(2026, 2, 8))
        assert count_feb8 == 2
        
        # Feb 7: 0 unreconciled (1 reconciled exists)
        count_feb7 = repo.get_unreconciled_count(price_date=date(2026, 2, 7))
        assert count_feb7 == 0
    
    def test_get_conflicts(self, db_session: Session, staging_data):
        """Test identifying conflicts (multiple sources for same stock/date)."""
        repo = StagingRepository(db_session)
        
        # Feb 8: DANGCEM has conflict (both sources)
        conflicts = repo.get_conflicts(price_date=date(2026, 2, 8))
        assert len(conflicts) == 1
        
        stock_code, dt, records = conflicts[0]
        assert stock_code == "DANGCEM"
        assert dt == date(2026, 2, 8)
        assert len(records) == 2
        
        sources = {r.source for r in records}
        assert sources == {"ngx", "afrimarket"}
    
    def test_get_conflicts_no_conflicts(self, db_session: Session, staging_data):
        """Test get_conflicts returns empty for dates with no conflicts."""
        repo = StagingRepository(db_session)
        
        # Feb 10: No conflicts (NGX only)
        conflicts = repo.get_conflicts(price_date=date(2026, 2, 10))
        assert len(conflicts) == 0
        
        # Feb 9: No conflicts (Afrimarket only)
        conflicts = repo.get_conflicts(price_date=date(2026, 2, 9))
        assert len(conflicts) == 0
    
    def test_get_all_reconciled(self, db_session: Session, staging_data):
        """Test retrieving all reconciled records."""
        repo = StagingRepository(db_session)
        
        records = repo.get_all_reconciled()
        
        # Only 1 reconciled record
        assert len(records) == 1
        assert records[0].stock_code == "ZENITH"
        assert records[0].price_date == date(2026, 2, 7)
        assert records[0].reconciled is True
    
    def test_mark_reconciled(self, db_session: Session, staging_data):
        """Test marking records as reconciled."""
        repo = StagingRepository(db_session)
        
        # Get unreconciled records for Feb 10
        unreconciled = repo.get_all(
            price_date=date(2026, 2, 10),
            reconciled_only=False
        )
        unreconciled_ids = [r.staging_id for r in unreconciled if not r.reconciled]
        
        # Mark as reconciled
        updated = repo.mark_reconciled(
            staging_ids=unreconciled_ids,
            reconciliation_notes="Test reconciliation"
        )
        
        assert updated == 2  # 2 records marked
        
        # Verify they're now reconciled
        count = repo.get_unreconciled_count(price_date=date(2026, 2, 10))
        assert count == 0
    
    def test_bulk_insert_staging(self, db_session: Session):
        """Test bulk inserting staging records."""
        repo = StagingRepository(db_session)
        
        records = [
            {
                'stock_code': 'TESTSTOCK',
                'price_date': date(2026, 2, 10),
                'close_price': Decimal("100.00"),
                'source': 'ngx'
            },
            {
                'stock_code': 'TESTSTOCK2',
                'price_date': date(2026, 2, 10),
                'close_price': Decimal("200.00"),
                'source': 'afrimarket'
            }
        ]
        
        inserted = repo.bulk_insert_staging(records, source='test')
        
        assert inserted == 2
        
        # Verify inserted
        all_records = repo.get_all(price_date=date(2026, 2, 10), reconciled_only=False)
        test_stocks = [r for r in all_records if r.stock_code in ['TESTSTOCK', 'TESTSTOCK2']]
        assert len(test_stocks) == 2
