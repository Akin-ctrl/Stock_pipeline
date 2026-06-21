"""
Staging and reconciliation manager.

Owns all staging-table operations: loading raw data, tracking unreconciled
dates, running reconciliation, and producing the fact-sync DataFrame that
drives price promotion.

Kept separate from PipelineOrchestrator so staging concerns have a single
authoritative home and can be tested without loading the full orchestrator.
"""

from datetime import date, datetime
from typing import Any, Dict, List, Optional

import pandas as pd

from app.repositories import StockRepository
from app.repositories.staging_repository import StagingRepository
from app.utils import get_logger


class StagingManager:
    """
    Manages the staging layer of the ETL pipeline.

    Responsibilities:
    - Load raw fetched data into staging_daily_prices
    - Query and track unreconciled dates
    - Drive the reconciliation engine across all pending dates
    - Produce the canonical fact-sync DataFrame for downstream promotion

    Errors and warnings are appended to the shared lists supplied at
    construction time so the owning orchestrator can collect them centrally.
    """

    def __init__(
        self,
        db,
        reconciliation_engine,
        errors: List[str],
        warnings: List[str],
        stage_times: Dict[str, float],
    ):
        self.db = db
        self.reconciliation_engine = reconciliation_engine
        self.logger = get_logger("staging_manager")
        self.errors = errors
        self.warnings = warnings
        self.stage_times = stage_times

        # Metrics exposed to the orchestrator after each run
        self.staging_loaded: int = 0
        self.reconciled_count: int = 0
        self.conflicts_flagged: int = 0
        self.avg_price_variance: float = 0.0
        self.reconciliation_window_start: Optional[datetime] = None

    def reset(self) -> None:
        """Reset per-run metrics."""
        self.staging_loaded = 0
        self.reconciled_count = 0
        self.conflicts_flagged = 0
        self.avg_price_variance = 0.0
        self.reconciliation_window_start = None

    # ------------------------------------------------------------------
    # Public API (called by PipelineOrchestrator and Airflow tasks)
    # ------------------------------------------------------------------

    def load_to_staging(self, data: pd.DataFrame, execution_date: date) -> int:
        """Load raw fetched data to staging_daily_prices.

        Args:
            data: DataFrame produced by the data source fetch step.
            execution_date: The pipeline's logical execution date.

        Returns:
            Number of rows inserted.
        """
        stage_start = datetime.now()
        self.logger.info("Stage 3: Loading data to staging")

        try:
            with self.db.get_session() as session:
                staging_repo = StagingRepository(session)

                staging_data = data.copy()
                if staging_data.empty:
                    self.logger.warning("No records to load to staging")
                    return 0

                if 'price_date' not in staging_data.columns:
                    staging_data['price_date'] = execution_date

                total_loaded = 0
                for source in staging_data['source'].unique():
                    source_data = staging_data[staging_data['source'] == source]
                    loaded = staging_repo.bulk_insert_staging(
                        df=source_data,
                        source=str(source),
                    )
                    total_loaded += loaded

                session.commit()

                self.staging_loaded = total_loaded
                self.logger.info(
                    f"Loaded {total_loaded} records to staging",
                    extra={"loaded": total_loaded},
                )
                self.stage_times['load_staging'] = (datetime.now() - stage_start).total_seconds()
                return total_loaded

        except Exception as exc:
            self.errors.append(f"Staging load error: {str(exc)}")
            self.logger.error("Staging load failed", error=exc)
            raise

    def get_unreconciled_dates(self) -> List[date]:
        """Return all distinct dates that have unreconciled staging records."""
        try:
            with self.db.get_session() as session:
                return StagingRepository(session).get_unreconciled_dates()
        except Exception as exc:
            self.logger.error(f"Failed to get unreconciled dates: {exc}")
            return []

    def get_staging_count(self, price_date: date, source: str) -> int:
        """Return the number of staging records for a given date and source."""
        try:
            with self.db.get_session() as session:
                return StagingRepository(session).count_by_date_source(price_date, source)
        except Exception as exc:
            self.logger.error(f"Failed to get staging count: {exc}")
            return 0

    def reconcile_all_staging(self, dates: List[date]) -> None:
        """Run the reconciliation engine over every supplied date.

        Sets ``reconciliation_window_start`` before reconciliation begins so
        that ``get_fact_sync_data`` can use it as the lower-bound timestamp
        for the recently-reconciled query.

        Args:
            dates: Dates with unreconciled staging records.
        """
        stage_start = datetime.now()
        self.logger.info(f"Stage 4: Reconciling staged prices for {len(dates)} dates")

        try:
            with self.db.get_session() as session:
                staging_repo = StagingRepository(session)
                self.reconciliation_engine.staging_repo = staging_repo
                self.reconciliation_engine.stock_repo = StockRepository(session)

                total_reconciled = 0
                total_applied = 0

                for price_date in dates:
                    self.logger.info(f"Reconciling data for {price_date}")
                    results = self.reconciliation_engine.reconcile_date(price_date)

                    for result in results:
                        if self.reconciliation_engine.apply_reconciliation(result):
                            total_applied += 1
                        else:
                            self.logger.warning(
                                f"Failed to apply reconciliation for "
                                f"{result.stock_code} on {result.price_date}"
                            )

                    total_reconciled += len(results)
                    self.logger.info(
                        f"Reconciled {len(results)} records for {price_date} "
                        f"({total_applied}/{total_reconciled} applied)"
                    )

                session.commit()

                # Capture the window-start from the DB server clock immediately
                # after the commit so that promoted_at timestamps (also written by
                # the DB server) are guaranteed to precede this boundary.
                from sqlalchemy import text as _text
                self.reconciliation_window_start = session.execute(
                    _text("SELECT NOW()")
                ).scalar()

                self.reconciled_count = total_applied
                self.logger.info(
                    f"Total reconciled records across all dates: {self.reconciled_count}",
                    extra={"reconciled_records": self.reconciled_count},
                )

                if total_reconciled > 0 and total_applied == 0:
                    raise RuntimeError(
                        f"Reconciliation failed: {total_reconciled} records processed "
                        f"but none applied successfully"
                    )

        except Exception as exc:
            error_msg = f"Staging reconciliation failed: {str(exc)}"
            self.logger.error(error_msg, error=exc)
            self.errors.append(error_msg)
            self.reconciled_count = 0
            raise

        self.stage_times['reconcile_staging'] = (datetime.now() - stage_start).total_seconds()

    def get_fact_sync_data(
        self,
        promoted_after: Optional[datetime] = None,
        price_dates: Optional[List[date]] = None,
    ) -> pd.DataFrame:
        """Return staging rows that should be upserted into fact_daily_prices.

        Combines two sets:
        1. Rows reconciled during the current run (refreshes existing fact rows).
        2. Older reconciled rows still absent from fact_daily_prices (rescues
           stranded backfill records).

        Args:
            promoted_after: Override the reconciliation window start timestamp.
            price_dates: Restrict the pending-promotion query to these dates.

        Returns:
            Deduplicated, sorted DataFrame ready for fact promotion.
        """
        recent_df = self._get_recently_reconciled_data(promoted_after=promoted_after)
        pending_df = self._get_pending_fact_promotion_data(price_dates=price_dates)

        frames = [df for df in (pending_df, recent_df) if not df.empty]
        if not frames:
            self.logger.warning("No reconciled staging rows available for fact sync")
            return pd.DataFrame()

        combined = pd.concat(frames, ignore_index=True)
        combined = combined.drop_duplicates(
            subset=['stock_code', 'price_date'],
            keep='last',
        )
        combined = combined.sort_values(['price_date', 'stock_code']).reset_index(drop=True)

        self.logger.info(
            f"Prepared {len(combined)} reconciled rows for fact sync",
            extra={"records": len(combined)},
        )
        return combined

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _rows_to_dataframe(self, rows: List[Any]) -> pd.DataFrame:
        """Convert staging ORM rows to the narrow fact-sync DataFrame shape.

        All callers in this codebase supply ORM model instances, not plain
        dicts. The attribute access path is the canonical one; the dict path
        is kept only as a documented fallback for test fixtures that supply
        synthetic data as dicts.
        """
        if not rows:
            return pd.DataFrame()

        data = []
        for row in rows:
            if isinstance(row, dict):
                # Test-fixture path: synthetic rows supplied as plain dicts.
                r = row
                data.append({
                    'stock_code': r.get('stock_code'),
                    'source': r.get('source'),
                    'price_date': r.get('price_date'),
                    'close_price': float(r['close_price']) if r.get('close_price') is not None else None,
                    'change_1d_pct': float(r['change_1d_pct']) if r.get('change_1d_pct') is not None else None,
                    'change_ytd_pct': float(r['change_ytd_pct']) if r.get('change_ytd_pct') is not None else None,
                    'volume': int(r['volume']) if r.get('volume') is not None else None,
                })
            else:
                # Production path: SQLAlchemy ORM instances.
                data.append({
                    'stock_code': row.stock_code,
                    'source': row.source,
                    'price_date': row.price_date,
                    'close_price': float(row.close_price) if row.close_price is not None else None,
                    'change_1d_pct': float(row.change_1d_pct) if row.change_1d_pct is not None else None,
                    'change_ytd_pct': float(row.change_ytd_pct) if row.change_ytd_pct is not None else None,
                    'volume': int(row.volume) if row.volume is not None else None,
                })

        return pd.DataFrame(data)

    def _get_recently_reconciled_data(
        self,
        promoted_after: Optional[datetime] = None,
    ) -> pd.DataFrame:
        """Pull staging rows promoted during the current pipeline run."""
        stage_start = datetime.now()
        self.logger.info("Stage 5: Pulling recently reconciled data from staging")

        effective_start = promoted_after or self.reconciliation_window_start
        if effective_start is None:
            self.logger.warning(
                "Reconciliation window start not set; no recently-reconciled data to pull"
            )
            return pd.DataFrame()

        try:
            with self.db.get_session() as session:
                rows = StagingRepository(session).get_canonical_reconciled_for_fact_sync(
                    promoted_after=effective_start,
                )
                if not rows:
                    self.logger.warning("No recently reconciled records in staging")
                    return pd.DataFrame()

                df = self._rows_to_dataframe(rows)
                self.logger.info(
                    f"Pulled {len(df)} recently reconciled records from staging",
                    extra={"records": len(df)},
                )
                self.stage_times['get_reconciled_data'] = (datetime.now() - stage_start).total_seconds()
                return df

        except Exception as exc:
            self.errors.append(f"Get recent reconciled data error: {str(exc)}")
            self.logger.error("Failed to get recently reconciled data", error=exc)
            raise

    def _get_pending_fact_promotion_data(
        self,
        promoted_after: Optional[datetime] = None,
        price_dates: Optional[List[date]] = None,
    ) -> pd.DataFrame:
        """Pull reconciled rows whose stock/date pair is absent from fact_daily_prices."""
        stage_start = datetime.now()
        self.logger.info("Stage 5: Pulling reconciled rows pending fact promotion")

        try:
            with self.db.get_session() as session:
                rows = StagingRepository(session).get_canonical_reconciled_for_fact_sync(
                    promoted_after=promoted_after,
                    price_dates=price_dates,
                    only_missing_from_fact=True,
                )
                if not rows:
                    self.logger.warning("No reconciled staging rows are pending fact promotion")
                    return pd.DataFrame()

                df = self._rows_to_dataframe(rows)
                self.logger.info(
                    f"Pulled {len(df)} reconciled rows pending fact promotion",
                    extra={"records": len(df)},
                )
                self.stage_times['get_reconciled_data'] = (datetime.now() - stage_start).total_seconds()
                return df

        except Exception as exc:
            self.errors.append(f"Get pending promotion data error: {str(exc)}")
            self.logger.error("Failed to get pending promotion data", error=exc)
            raise
