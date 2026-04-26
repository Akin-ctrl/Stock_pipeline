"""
Integration test for staging ingestion workflow.

Tests the complete production sequence:
1. Fetch from NGX (if available)
2. Fetch from Afrimarket
3. Load to staging with source tags
4. Verify staging data integrity
"""

import pytest
from datetime import date, datetime
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session

from app.services.data_sources import NGXDataSource
from app.services.data_sources.afrimarket_source import AfrimarketDataSource
from app.repositories.staging_repository import StagingRepository
from app.config.database import get_db
from app.utils.logger import get_logger

logger = get_logger(__name__)


@pytest.mark.integration
class TestStagingIngestionWorkflow:
    """Test complete staging ingestion workflow in production sequence."""
    
    def test_complete_ingestion_workflow(self, db_session: Session):
        """
        Test complete production workflow:
        1. Fetch NGX data
        2. Fetch Afrimarket data  
        3. Tag with source
        4. Load to staging
        5. Verify data integrity
        """
        logger.info("=" * 80)
        logger.info("TESTING STAGING INGESTION WORKFLOW")
        logger.info("=" * 80)
        
        execution_date = date.today()
        staging_repo = StagingRepository(db_session)
        
        # STEP 1: Fetch from NGX
        logger.info("\n[STEP 1] Fetching from NGX...")
        ngx_source = NGXDataSource()
        ngx_data = pd.DataFrame()
        
        try:
            ngx_data = ngx_source.fetch(execution_date)
            if not ngx_data.empty:
                logger.info(f"✅ NGX fetched {len(ngx_data)} records")
                logger.info(f"   Columns: {list(ngx_data.columns)}")
                logger.info(f"   Sample stock codes: {ngx_data['stock_code'].head(3).tolist()}")
                
                # Verify NGX data structure
                assert 'stock_code' in ngx_data.columns
                assert 'price_date' in ngx_data.columns
                assert 'close_price' in ngx_data.columns
                assert 'source' in ngx_data.columns
                assert (ngx_data['source'] == 'ngx').all(), "All NGX records must have source='ngx'"
                logger.info(f"   Price dates: {ngx_data['price_date'].unique()}")
            else:
                logger.warning("⚠️  NGX returned no data (may be blocked or no market data)")
        except Exception as e:
            logger.warning(f"⚠️  NGX fetch failed: {e}")
        
        # STEP 2: Fetch from Afrimarket
        logger.info("\n[STEP 2] Fetching from Afrimarket...")
        afm_source = AfrimarketDataSource()
        afm_data = pd.DataFrame()
        
        try:
            afm_data = afm_source.fetch()
            if not afm_data.empty:
                # Tag with execution_date (simulating orchestrator behavior)
                afm_data['price_date'] = execution_date
                
                logger.info(f"✅ Afrimarket fetched {len(afm_data)} records")
                logger.info(f"   Columns: {list(afm_data.columns)}")
                logger.info(f"   Sample stock codes: {afm_data['stock_code'].head(3).tolist()}")
                
                # Verify Afrimarket data structure
                assert 'stock_code' in afm_data.columns
                assert 'price_date' in afm_data.columns
                assert 'close_price' in afm_data.columns
                assert 'source' in afm_data.columns
                assert (afm_data['source'] == 'afrimarket').all(), "All Afrimarket records must have source='afrimarket'"
                logger.info(f"   Price dates: {afm_data['price_date'].unique()}")
        except Exception as e:
            logger.error(f"❌ Afrimarket fetch failed: {e}")
            pytest.fail(f"Afrimarket fetch failed: {e}")
        
        # STEP 3: Combine data
        logger.info("\n[STEP 3] Combining data from sources...")
        all_data = []
        if not ngx_data.empty:
            all_data.append(ngx_data)
        if not afm_data.empty:
            all_data.append(afm_data)
        
        assert len(all_data) > 0, "Must have data from at least one source"
        
        combined = pd.concat(all_data, ignore_index=True)
        logger.info(f"✅ Combined {len(combined)} total records")
        logger.info(f"   Sources: {combined['source'].value_counts().to_dict()}")
        
        # STEP 4: Load to staging
        logger.info("\n[STEP 4] Loading to staging...")
        
        # Clear existing staging data for this test
        db_session.execute(text("DELETE FROM staging_daily_prices"))
        db_session.commit()
        
        # Load NGX data
        ngx_loaded = 0
        if not ngx_data.empty:
            ngx_loaded = staging_repo.bulk_insert_staging(ngx_data, source='ngx')
            logger.info(f"✅ Loaded {ngx_loaded} NGX records to staging")
        
        # Load Afrimarket data
        afm_loaded = 0
        if not afm_data.empty:
            afm_loaded = staging_repo.bulk_insert_staging(afm_data, source='afrimarket')
            logger.info(f"✅ Loaded {afm_loaded} Afrimarket records to staging")
        
        total_loaded = ngx_loaded + afm_loaded
        logger.info(f"   Total loaded: {total_loaded}")
        
        assert total_loaded > 0, "Must load at least some records to staging"
        
        # STEP 5: Verify staging data
        logger.info("\n[STEP 5] Verifying staging data integrity...")
        
        # Check source distribution
        result = db_session.execute(text("""
            SELECT source, COUNT(*) as count
            FROM staging_daily_prices
            GROUP BY source
            ORDER BY source
        """))
        source_counts = {row[0]: row[1] for row in result}
        logger.info(f"✅ Source distribution: {source_counts}")
        
        # Verify NO NULL sources
        result = db_session.execute(text("""
            SELECT COUNT(*) FROM staging_daily_prices WHERE source IS NULL
        """))
        null_sources = result.scalar()
        assert null_sources == 0, f"Found {null_sources} records with NULL source!"
        logger.info("✅ No NULL source values")
        
        # Check date distribution
        result = db_session.execute(text("""
            SELECT price_date, source, COUNT(*) as count
            FROM staging_daily_prices
            GROUP BY price_date, source
            ORDER BY price_date DESC, source
        """))
        date_dist = [(row[0], row[1], row[2]) for row in result]
        logger.info("✅ Date distribution:")
        for price_date, source, count in date_dist:
            logger.info(f"   {price_date} | {source:12} | {count} records")
        
        # Verify all records are unreconciled
        result = db_session.execute(text("""
            SELECT COUNT(*) FROM staging_daily_prices WHERE reconciled = true
        """))
        reconciled = result.scalar()
        assert reconciled == 0, "All new records should be unreconciled"
        logger.info("✅ All records unreconciled (ready for reconciliation)")
        
        # STEP 6: Test get_unreconciled_dates
        logger.info("\n[STEP 6] Testing get_unreconciled_dates()...")
        unreconciled_dates = staging_repo.get_unreconciled_dates()
        logger.info(f"✅ Unreconciled dates: {unreconciled_dates}")
        assert len(unreconciled_dates) > 0, "Should have unreconciled dates"
        
        logger.info("\n" + "=" * 80)
        logger.info("✅ STAGING INGESTION WORKFLOW COMPLETE")
        logger.info("=" * 80)
        logger.info(f"Total records staged: {total_loaded}")
        logger.info(f"Unreconciled dates: {len(unreconciled_dates)}")
        logger.info(f"Ready for reconciliation: {total_loaded} records")
