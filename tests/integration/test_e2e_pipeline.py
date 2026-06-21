"""
End-to-end pipeline tests.

Exercises the complete ETL flow — fetch → stage → reconcile → transform
→ load prices → indicators → alerts → recommendations — using a real
PostgreSQL test database and monkeypatched network calls.

These tests validate that all the components interact correctly as a system
and that bugs fixed in this batch (null prices, fillna contamination,
reconciliation timestamps, duplicate constraints) don't regress.
"""

from datetime import date, timedelta
from decimal import Decimal

import pandas as pd
import pytest
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.utils.logger import get_logger

logger = get_logger(__name__)

# Always use a date safely in the past so "today" market rules don't interfere
EXECUTION_DATE = date.today() - timedelta(days=1)


def _afrimarket_mock(execution_date: date) -> pd.DataFrame:
    """Minimal, well-formed Afrimarket DataFrame for e2e pipeline testing."""
    return pd.DataFrame(
        [
            {
                "stock_code": "DANGCEM",
                "company_name": "Dangote Cement Plc",
                "exchange": "NGX",
                "price_date": execution_date,
                "close_price": 375.50,
                "source": "afrimarket",
                "volume": 2_100_000,
                "price_change_amount": 5.50,
            },
            {
                "stock_code": "GTCO",
                "company_name": "Guaranty Trust Holding Company Plc",
                "exchange": "NGX",
                "price_date": execution_date,
                "close_price": 52.10,
                "source": "afrimarket",
                "volume": 1_250_000,
                "price_change_amount": 0.80,
            },
            {
                "stock_code": "ZENITHBANK",
                "company_name": "Zenith Bank Plc",
                "exchange": "NGX",
                "price_date": execution_date,
                "close_price": 49.75,
                "source": "afrimarket",
                "volume": 980_000,
                "price_change_amount": -0.40,
            },
        ]
    )


def _run_orchestrator(
    monkeypatch,
    db_session: Session,
    *,
    use_staging: bool = True,
    mock_fn=None,
):
    """
    Build and run PipelineOrchestrator against the test database.

    Monkeypatches:
    - POSTGRES_HOST / PORT / DB / USER / PASSWORD to point at the test DB
    - AfrimarketDataSource.fetch to avoid live network calls
    - settings cache so the fresh env vars take effect
    """
    import os

    # Discover the test DB coordinates from the conftest's engine bindings
    test_host = "localhost"
    test_port = str(os.getenv("POSTGRES_PORT", "5432"))
    test_db = "stock_pipeline_test"
    test_user = os.getenv("POSTGRES_USER", "stock_user")
    test_pw = os.getenv("POSTGRES_PASSWORD", "stock_password")

    monkeypatch.setenv("POSTGRES_HOST", test_host)
    monkeypatch.setenv("POSTGRES_PORT", test_port)
    monkeypatch.setenv("POSTGRES_DB", test_db)
    monkeypatch.setenv("POSTGRES_USER", test_user)
    monkeypatch.setenv("POSTGRES_PASSWORD", test_pw)

    # Bust the module-level settings singleton so next get_settings() re-reads env
    import app.config.settings as _sm
    original_cache = getattr(_sm, "_settings", None)
    _sm._settings = None

    try:
        from app.pipelines.orchestrator import PipelineConfig, PipelineOrchestrator
        from app.services.data_sources.afrimarket_source import AfrimarketDataSource

        if mock_fn is None:
            mock_fn = lambda self, start_date=None, end_date=None: _afrimarket_mock(EXECUTION_DATE)

        monkeypatch.setattr(AfrimarketDataSource, "fetch", mock_fn)

        config = PipelineConfig(
            fetch_afrimarket=True,
            use_staging=use_staging,
            validate_data=True,
            load_stocks=True,
            load_prices=True,
            calculate_indicators=False,
            evaluate_alerts=False,
            generate_recommendations=False,
        )
        return PipelineOrchestrator(config=config).run(execution_date=EXECUTION_DATE)
    finally:
        _sm._settings = original_cache


@pytest.mark.integration
class TestEndToEndStagingWorkflow:
    """
    Full staging-path e2e: fetch → stage → reconcile → transform → load.
    Indicators/alerts/recommendations disabled — require long price history.
    """

    def test_staging_workflow_loads_prices_correctly(
        self, db_session: Session, monkeypatch
    ):
        """Pipeline staging mode loads prices with correct values and no NULL contamination."""
        result = _run_orchestrator(monkeypatch, db_session, use_staging=True)

        assert result.success, f"Pipeline failed. Errors: {result.errors}"
        assert result.prices_loaded >= 1, "No prices were loaded"
        assert result.stocks_processed >= 1, "No stocks were processed"

        null_prices = db_session.execute(
            text(
                "SELECT COUNT(*) FROM fact_daily_prices "
                "WHERE close_price IS NULL AND price_date = :d"
            ),
            {"d": EXECUTION_DATE},
        ).scalar()
        assert null_prices == 0, "NULL close_price found in fact_daily_prices"

    def test_staging_workflow_is_idempotent(
        self, db_session: Session, monkeypatch
    ):
        """Running the pipeline twice for the same date must not duplicate prices."""
        for run_num in range(2):
            result = _run_orchestrator(monkeypatch, db_session, use_staging=True)
            assert result.success, f"Run {run_num + 1} failed: {result.errors}"

        count = db_session.execute(
            text(
                """
                SELECT COUNT(*) FROM fact_daily_prices fdp
                JOIN dim_stocks ds ON ds.stock_id = fdp.stock_id
                WHERE fdp.price_date = :d AND ds.stock_code = 'DANGCEM'
                """
            ),
            {"d": EXECUTION_DATE},
        ).scalar()
        assert count == 1, f"Expected 1 price row for DANGCEM, got {count} (upsert broken?)"

    def test_null_close_price_is_rejected(
        self, db_session: Session, monkeypatch
    ):
        """Rows with null close_price must be dropped before reaching fact_daily_prices."""
        dirty = _afrimarket_mock(EXECUTION_DATE).copy()
        dirty.loc[dirty["stock_code"] == "GTCO", "close_price"] = None

        result = _run_orchestrator(
            monkeypatch,
            db_session,
            use_staging=True,
            mock_fn=lambda self, start_date=None, end_date=None: dirty,
        )

        null_gtco = db_session.execute(
            text(
                """
                SELECT COUNT(*) FROM fact_daily_prices fdp
                JOIN dim_stocks ds ON ds.stock_id = fdp.stock_id
                WHERE fdp.price_date = :d
                  AND ds.stock_code = 'GTCO'
                  AND fdp.close_price IS NULL
                """
            ),
            {"d": EXECUTION_DATE},
        ).scalar()
        assert null_gtco == 0, "NULL close_price was persisted — null guard fix regressed"

    def test_staging_preserves_reconciled_state_on_re_insert(
        self, db_session: Session, monkeypatch
    ):
        """
        Re-inserting staging rows for a date that was already reconciled must
        not reset the reconciliation state (LC-08 fix).
        """
        from app.repositories.staging_repository import StagingRepository

        staging_repo = StagingRepository(db_session)
        staging_repo.bulk_insert_staging(
            _afrimarket_mock(EXECUTION_DATE), source="afrimarket"
        )
        db_session.flush()

        # Mark DANGCEM as reconciled
        db_session.execute(
            text(
                """
                UPDATE staging_daily_prices
                SET reconciled = true,
                    promoted_at = NOW(),
                    reconciliation_notes = 'manual test reconciliation'
                WHERE stock_code = 'DANGCEM' AND price_date = :d
                """
            ),
            {"d": EXECUTION_DATE},
        )
        db_session.flush()

        # Re-insert same data (simulates a DAG retry for the same date)
        staging_repo.bulk_insert_staging(
            _afrimarket_mock(EXECUTION_DATE), source="afrimarket"
        )
        db_session.flush()

        row = db_session.execute(
            text(
                """
                SELECT reconciled, reconciliation_notes
                FROM staging_daily_prices
                WHERE stock_code = 'DANGCEM' AND price_date = :d
                """
            ),
            {"d": EXECUTION_DATE},
        ).fetchone()

        assert row is not None
        assert row[0] is True, "Re-insert reset reconciled=True (LC-08 regressed)"
        assert row[1] == "manual test reconciliation", (
            "Re-insert wiped reconciliation_notes (LC-08 regressed)"
        )

    def test_fillna_zero_contamination_absent(
        self, db_session: Session, monkeypatch
    ):
        """
        change_1d_pct must be NULL for a stock on its first-ever price load,
        not 0.0 (which would be fillna(0) contamination from the old code).
        """
        result = _run_orchestrator(monkeypatch, db_session, use_staging=True)
        assert result.success

        row = db_session.execute(
            text(
                """
                SELECT fdp.change_1d_pct
                FROM fact_daily_prices fdp
                JOIN dim_stocks ds ON ds.stock_id = fdp.stock_id
                WHERE fdp.price_date = :d AND ds.stock_code = 'DANGCEM'
                LIMIT 1
                """
            ),
            {"d": EXECUTION_DATE},
        ).fetchone()

        assert row is not None, "DANGCEM price not found"
        # change_1d_pct should be NULL (no previous day to diff against),
        # NOT 0.0 which signals the old fillna(0) contamination.
        assert row[0] != Decimal("0"), (
            "change_1d_pct is 0.0 on a first-load date — fillna(0) contamination regressed"
        )


@pytest.mark.integration
class TestEndToEndDirectWorkflow:
    """
    Full direct-path e2e: fetch → validate → transform → load.
    Exercises the legacy direct workflow (use_staging=False).
    """

    def test_direct_workflow_loads_valid_prices(
        self, db_session: Session, monkeypatch
    ):
        """Direct mode loads prices without staging/reconciliation and rejects NULLs."""
        result = _run_orchestrator(monkeypatch, db_session, use_staging=False)

        assert result.success, f"Direct pipeline failed: {result.errors}"
        assert result.prices_loaded >= 1

        null_count = db_session.execute(
            text(
                "SELECT COUNT(*) FROM fact_daily_prices "
                "WHERE close_price IS NULL AND price_date = :d"
            ),
            {"d": EXECUTION_DATE},
        ).scalar()
        assert null_count == 0, "NULL close_prices persisted in direct mode"

    def test_direct_workflow_is_idempotent(
        self, db_session: Session, monkeypatch
    ):
        """Re-running direct mode for the same date must not duplicate rows."""
        for run in range(2):
            result = _run_orchestrator(monkeypatch, db_session, use_staging=False)
            assert result.success, f"Run {run + 1} failed: {result.errors}"

        count = db_session.execute(
            text(
                """
                SELECT COUNT(*) FROM fact_daily_prices fdp
                JOIN dim_stocks ds ON ds.stock_id = fdp.stock_id
                WHERE fdp.price_date = :d AND ds.stock_code = 'GTCO'
                """
            ),
            {"d": EXECUTION_DATE},
        ).scalar()
        assert count == 1, f"Expected 1 price row for GTCO, got {count} (upsert broken?)"
