"""
Integration test for the current staging ingestion workflow.

This test reflects the current source strategy:
1. Fetch from Afrimarket
2. Load Afrimarket rows into staging
3. Verify staging integrity and source tagging
"""

from datetime import date

import pandas as pd
import pytest
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.repositories.staging_repository import StagingRepository
from app.services.data_sources.afrimarket_source import AfrimarketDataSource
from app.utils.logger import get_logger

logger = get_logger(__name__)


@pytest.mark.integration
class TestStagingIngestionWorkflow:
    """Test the current Afrimarket-backed staging ingestion workflow."""

    def test_complete_ingestion_workflow(self, db_session: Session, monkeypatch):
        """
        Test current production-shaped workflow:
        1. Fetch Afrimarket data
        2. Tag with execution date
        3. Load to staging
        4. Verify data integrity
        """
        logger.info("=" * 80)
        logger.info("TESTING STAGING INGESTION WORKFLOW")
        logger.info("=" * 80)

        execution_date = date.today()
        staging_repo = StagingRepository(db_session)

        afrimarket_rows = pd.DataFrame(
            [
                {
                    "stock_code": "GTCO",
                    "company_name": "Guaranty Trust Holding Company Plc",
                    "exchange": "NGX",
                    "price_date": execution_date,
                    "close_price": 52.10,
                    "source": "afrimarket",
                    "volume": 1_250_000,
                    "price_change_amount": 0.8,
                },
                {
                    "stock_code": "ZENITHBANK",
                    "company_name": "Zenith Bank Plc",
                    "exchange": "NGX",
                    "price_date": execution_date,
                    "close_price": 49.75,
                    "source": "afrimarket",
                    "volume": 980_000,
                    "price_change_amount": -0.4,
                },
            ]
        )

        monkeypatch.setattr(
            AfrimarketDataSource,
            "fetch",
            lambda self, start_date=None, end_date=None: afrimarket_rows.copy(),
        )

        logger.info("\n[STEP 1] Fetching from Afrimarket...")
        afm_source = AfrimarketDataSource()
        afm_data = afm_source.fetch()

        assert not afm_data.empty
        assert "stock_code" in afm_data.columns
        assert "price_date" in afm_data.columns
        assert "close_price" in afm_data.columns
        assert "source" in afm_data.columns
        assert (afm_data["source"] == "afrimarket").all()
        logger.info(f"✅ Afrimarket fetched {len(afm_data)} records")

        logger.info("\n[STEP 2] Loading to staging...")
        db_session.execute(text("DELETE FROM staging_daily_prices"))
        db_session.commit()

        loaded = staging_repo.bulk_insert_staging(afm_data, source="afrimarket")
        assert loaded > 0
        logger.info(f"✅ Loaded {loaded} Afrimarket records to staging")

        logger.info("\n[STEP 3] Verifying staging data integrity...")
        result = db_session.execute(
            text(
                """
                SELECT source, COUNT(*) as count
                FROM staging_daily_prices
                GROUP BY source
                ORDER BY source
                """
            )
        )
        source_counts = {row[0]: row[1] for row in result}
        assert source_counts == {"afrimarket": len(afrimarket_rows)}
        logger.info(f"✅ Source distribution: {source_counts}")

        result = db_session.execute(
            text("SELECT COUNT(*) FROM staging_daily_prices WHERE source IS NULL")
        )
        assert result.scalar() == 0
        logger.info("✅ No NULL source values")

        result = db_session.execute(
            text(
                """
                SELECT COUNT(*) FROM staging_daily_prices
                WHERE reconciled = true
                """
            )
        )
        assert result.scalar() == 0
        logger.info("✅ All records unreconciled (ready for reconciliation)")

        logger.info("\n[STEP 4] Testing get_unreconciled_dates()...")
        unreconciled_dates = staging_repo.get_unreconciled_dates()
        assert execution_date in unreconciled_dates
        logger.info(f"✅ Unreconciled dates: {unreconciled_dates}")

        logger.info("\n" + "=" * 80)
        logger.info("✅ STAGING INGESTION WORKFLOW COMPLETE")
        logger.info("=" * 80)
        logger.info(f"Total records staged: {loaded}")
        logger.info(f"Ready for reconciliation: {loaded} records")
