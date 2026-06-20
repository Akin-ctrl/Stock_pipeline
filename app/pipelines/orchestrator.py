"""
Pipeline orchestrator.

Coordinates the complete ETL workflow:
1. Fetch data from Afrimarket API
2. Stage raw data with source tracking
3. Reconcile multi-source conflicts
4. Transform and standardize data
5. Load into database (stocks, prices)
6. Calculate technical indicators
7. Evaluate alert rules
8. Generate pipeline summary report
"""

from typing import Dict, List, Optional
from datetime import date, datetime
from dataclasses import dataclass

import pandas as pd

from app.config.database import get_db
from app.config.settings import Settings
from app.repositories import (
    StockRepository, PriceRepository,
    IndicatorRepository, AlertRepository, RecommendationRepository,
)
from app.repositories.staging_repository import StagingRepository
from app.models import DimSector, DimStock
from app.services.data_sources import AfrimarketDataSource
from app.services.processors import DataValidator, DataTransformer
from app.services.processors.reconciliation import ReconciliationEngine
from app.services.indicators import IndicatorCalculator
from app.services.alerts import AlertEvaluator, AlertNotifier
from app.services.reference_data import (
    UNKNOWN_SECTOR_NAME,
    choose_sector_name,
    is_unknown_sector,
    load_stock_sector_map,
)
from app.utils import get_logger
from app.utils.exceptions import DataValidationError
from app.pipelines.staging_manager import StagingManager
from app.pipelines.price_loader import PriceLoader


@dataclass
class PipelineConfig:
    """
    Pipeline execution configuration.

    Attributes:
        fetch_afrimarket: Whether to fetch data from Afrimarket source
        use_staging: Whether to use staging workflow with reconciliation
        validate_data: Whether to run validation
        load_stocks: Whether to load/update stocks
        calculate_indicators: Whether to calculate indicators
        evaluate_alerts: Whether to evaluate alert rules
        generate_recommendations: Whether to generate investment recommendations
        recommendation_profile: Recommendation style (steady_20p_10d)
        batch_size: Batch size for processing
        max_errors: Maximum errors before aborting
        lookback_days: Days of historical data to fetch
    """
    fetch_afrimarket: bool = True
    use_staging: bool = True  # True = staging mode with reconciliation (recommended)
    validate_data: bool = True
    load_stocks: bool = True
    load_prices: bool = True
    calculate_indicators: bool = True
    evaluate_alerts: bool = True
    generate_recommendations: bool = True
    recommendation_profile: str = 'steady_20p_10d'
    recommendation_profiles: Optional[List[str]] = None
    batch_size: int = 50
    max_errors: int = 10
    lookback_days: int = 30


@dataclass
class PipelineResult:
    """
    Result of pipeline execution.

    Attributes:
        success: Whether pipeline completed successfully
        execution_time: Total execution time in seconds
        stocks_processed: Number of stocks processed
        prices_loaded: Number of prices loaded (v1) or promoted (v2)
        indicators_calculated: Number of indicators calculated
        alerts_generated: Number of alerts generated
        recommendations_generated: Number of recommendations generated
        errors: List of error messages
        warnings: List of warning messages
        stage_times: Dict mapping stage name to execution time
        staging_loaded: Number of records loaded to staging (v2)
        reconciled_count: Number of records reconciled (v2)
        conflicts_flagged: Number of conflicts flagged for review (v2)
        avg_price_variance: Average price variance % (v2)
    """
    success: bool
    execution_time: float
    stocks_processed: int
    prices_loaded: int
    indicators_calculated: int
    alerts_generated: int
    recommendations_generated: int
    errors: List[str]
    warnings: List[str]
    stage_times: Dict[str, float]
    # Staging workflow metrics
    staging_loaded: int = 0
    reconciled_count: int = 0
    conflicts_flagged: int = 0
    avg_price_variance: float = 0.0


class PipelineOrchestrator:
    """
    Orchestrates the complete ETL pipeline.

    Coordinates data flow through all stages:
    - Data fetching from Afrimarket
    - Data validation and quality checks
    - Data transformation and standardization
    - Database loading (stocks and prices) via PriceLoader
    - Staging and reconciliation via StagingManager
    - Technical indicator calculation
    - Alert rule evaluation
    - Investment recommendation generation

    Provides transaction management, error handling, and detailed logging.
    """

    def __init__(self, config: Optional[PipelineConfig] = None):
        """
        Initialize orchestrator.

        Args:
            config: Pipeline configuration (default: PipelineConfig())
        """
        self.config = config or PipelineConfig()
        self.logger = get_logger("pipeline_orchestrator")

        self.settings = Settings.load()
        self.db = get_db()

        if AfrimarketDataSource is None:
            raise ImportError(
                "Afrimarket package not found. Install with: pip install afrimarket"
            )
        self.afrimarket_source = AfrimarketDataSource()

        self.validator = None  # Initialized with sectors on first use
        self.transformer = DataTransformer()
        self.indicator_calculator = IndicatorCalculator()

        # Shared mutable collections — passed by reference to sub-managers so
        # all components append to the same lists without extra wiring.
        self.errors: List[str] = []
        self.warnings: List[str] = []
        self.stage_times: Dict[str, float] = {}

        reconciliation_engine = None
        if self.config.use_staging:
            reconciliation_engine = ReconciliationEngine(
                low_variance_threshold=1.0,
                medium_variance_threshold=3.0,
                preferred_source='afrimarket',
            )

        self.staging_manager: Optional[StagingManager] = (
            StagingManager(
                db=self.db,
                reconciliation_engine=reconciliation_engine,
                errors=self.errors,
                warnings=self.warnings,
                stage_times=self.stage_times,
            )
            if self.config.use_staging
            else None
        )

        self.price_loader = PriceLoader(
            db=self.db,
            batch_size=self.config.batch_size,
            use_staging=self.config.use_staging,
            errors=self.errors,
            warnings=self.warnings,
            stage_times=self.stage_times,
        )

        self.alert_notifier = AlertNotifier() if (
            self.settings.notifications.email_enabled or
            self.settings.notifications.slack_enabled
        ) else None

    def run(
        self,
        execution_date: Optional[date] = None,
        stock_codes: Optional[List[str]] = None,
    ) -> PipelineResult:
        """
        Execute the complete pipeline.

        Args:
            execution_date: Date to run pipeline for (default: today)
            stock_codes: Specific stocks to process (default: all active)

        Returns:
            PipelineResult with execution summary
        """
        start_time = datetime.now()

        if execution_date is None:
            execution_date = date.today()

        self.logger.info(
            f"Starting pipeline execution for {execution_date}",
            extra={"execution_date": str(execution_date)},
        )

        # Use .clear() rather than reassignment so the shared references held
        # by StagingManager and PriceLoader continue to point at the same lists.
        self.errors.clear()
        self.warnings.clear()
        self.stage_times.clear()

        if self.staging_manager:
            self.staging_manager.reset()

        try:
            if self.config.use_staging:
                result = self._run_staging_workflow(execution_date, stock_codes)
            else:
                result = self._run_direct_workflow(execution_date, stock_codes)

            self._log_summary(result)
            return result

        except Exception as e:
            self.logger.error(
                "Pipeline execution failed",
                error=e,
                extra={"error": str(e)},
            )
            self.errors.append(f"Pipeline failure: {str(e)}")
            return self._build_result(start_time, False, 0, 0, 0, 0)

    def _run_direct_workflow(
        self,
        execution_date: date,
        stock_codes: Optional[List[str]],
    ) -> PipelineResult:
        """
        Direct workflow is disabled.

        This path bypasses staging and reconciliation, writing prices with
        bar_status='OBSERVED'. Trusted filters in PriceRepository require
        RECONCILED or OFFICIAL, so OBSERVED data is silently excluded from
        every downstream query — indicators, alerts, and recommendations see
        nothing for the day while the pipeline reports success.

        Always run with use_staging=True (the default).
        """
        raise RuntimeError(
            "Direct (non-staging) workflow is disabled. "
            "Set PipelineConfig.use_staging=True (the default) to use the "
            "reconciled staging path. Direct mode writes bar_status=OBSERVED "
            "data that is excluded from indicators, alerts, and recommendations."
        )

        # --- dead code below preserved as reference for the staging equivalent ---
        start_time = datetime.now()

        stocks_processed = 0
        prices_loaded = 0
        indicators_calculated = 0
        alerts_generated = 0
        recommendations_generated = 0
        analysis_date = execution_date

        try:
            raw_data = self.fetch_data(execution_date, stock_codes)
            self.logger.info(f"Fetched {len(raw_data)} rows from NGX")
            if raw_data.empty:
                self.logger.warning("No data fetched from NGX")
                self.warnings.append("No data fetched from NGX")
                return self._build_result(start_time, False, 0, 0, 0, 0)

            if self.config.validate_data:
                validated_data = self._validate_data(raw_data)
                if validated_data.empty:
                    self.logger.error("All data failed validation")
                    return self._build_result(start_time, False, 0, 0, 0, 0)
            else:
                validated_data = raw_data

            transformed_data = self.transform_data(validated_data) if not validated_data.empty else pd.DataFrame()

            if self.config.load_stocks and not transformed_data.empty:
                stocks_processed = self.load_stocks(transformed_data)

            if self.config.load_prices and not transformed_data.empty:
                prices_loaded = self.load_prices(transformed_data)
                analysis_date = pd.to_datetime(transformed_data['price_date']).dt.date.max()

            if self.config.calculate_indicators:
                indicators_calculated = self.calculate_indicators(analysis_date, stock_codes)

            if self.config.evaluate_alerts:
                alerts_generated = self.evaluate_alerts(analysis_date)

            if self.config.generate_recommendations:
                recommendations_generated = self.generate_recommendations(analysis_date, stock_codes)

            return self._build_result(
                start_time,
                len(self.errors) == 0,
                stocks_processed,
                prices_loaded,
                indicators_calculated,
                alerts_generated,
                recommendations_generated,
            )

        except Exception as e:
            self.errors.append(f"Direct workflow error: {str(e)}")
            self.logger.error("Direct workflow failed", error=e)
            return self._build_result(
                start_time, False, stocks_processed, prices_loaded,
                indicators_calculated, alerts_generated, recommendations_generated,
            )

    def _run_staging_workflow(
        self,
        execution_date: date,
        stock_codes: Optional[List[str]],
    ) -> PipelineResult:
        """
        Staging workflow: Fetch → stage raw → reconcile → validate → transform → load.

        This workflow:
        1. Fetches RAW data from Afrimarket
        2. Loads RAW data to staging tables (no processing)
        3. Pulls from staging and reconciles price differences between sources
        4. Transforms reconciled data (standardize, calculate fields)
        5. Loads transformed data to production
        6. Calculates indicators and alerts on production data
        """
        start_time = datetime.now()

        stocks_processed = 0
        prices_loaded = 0
        indicators_calculated = 0
        alerts_generated = 0
        recommendations_generated = 0
        analysis_date = execution_date

        try:
            raw_data = self.fetch_data(execution_date, stock_codes)
            self.logger.info(f"Fetched {len(raw_data)} rows from Afrimarket")

            if raw_data.empty:
                self.logger.warning("No data fetched from Afrimarket")
                self.warnings.append("No data fetched from Afrimarket")
                if not self.config.fetch_afrimarket:
                    return self._build_result(start_time, False, 0, 0, 0, 0)
                self.logger.info("Continuing with empty dataset - will handle downstream")

            if self.config.load_stocks and not raw_data.empty:
                stocks_processed = self.load_stocks(raw_data)

            if not raw_data.empty:
                # Validate against raw_data (which carries company_name / exchange from
                # the scraper). Reconciled staging output lacks those columns so
                # validation must happen here, before records enter staging.
                raw_data = self._validate_data(raw_data)
                loaded = self.staging_manager.load_to_staging(raw_data, execution_date)
                self.logger.info(f"Loaded {loaded} raw records to staging")

            unreconciled_dates = self.staging_manager.get_unreconciled_dates()
            if unreconciled_dates:
                self.logger.info(
                    f"Found unreconciled data for {len(unreconciled_dates)} dates: "
                    f"{unreconciled_dates}"
                )
                self.staging_manager.reconcile_all_staging(unreconciled_dates)
            else:
                self.logger.info("No unreconciled records found in staging")

            # Stage 5a: Reconciled staging data lacks company_name/exchange.
            # Raw data was already validated before load_to_staging above.
            reconciled_data = self.staging_manager.get_fact_sync_data()
            if not reconciled_data.empty:
                transformed_data = self.transform_data(reconciled_data)
            else:
                transformed_data = pd.DataFrame()

            if self.config.load_prices and not transformed_data.empty:
                prices_loaded = self.load_prices(transformed_data)
                analysis_date = pd.to_datetime(transformed_data['price_date']).dt.date.max()

            if self.config.calculate_indicators and prices_loaded > 0:
                indicators_calculated = self.calculate_indicators(analysis_date, stock_codes)

            if self.config.evaluate_alerts:
                alerts_generated = self.evaluate_alerts(analysis_date)

            if self.config.generate_recommendations:
                recommendations_generated = self.generate_recommendations(analysis_date, stock_codes)

            return self._build_result(
                start_time,
                len(self.errors) == 0,
                stocks_processed,
                prices_loaded,
                indicators_calculated,
                alerts_generated,
                recommendations_generated,
            )

        except Exception as e:
            self.logger.error(
                "Staging workflow failed",
                error=e,
                extra={"error": str(e)},
            )
            self.errors.append(f"Staging workflow failure: {str(e)}")
            return self._build_result(start_time, False, 0, 0, 0, 0)

    # ------------------------------------------------------------------
    # Fetch
    # ------------------------------------------------------------------

    def fetch_data(
        self,
        execution_date: date,
        stock_codes: Optional[List[str]],
    ) -> pd.DataFrame:
        """Fetch data from all configured sources."""
        stage_start = datetime.now()

        self.logger.info("Stage 1: Fetching data from sources")
        self.logger.info(
            f"Sources configured: Afrimarket={self.config.fetch_afrimarket} "
            f"(available={self.afrimarket_source is not None})"
        )

        all_data = []

        self.logger.info(
            f"Afrimarket fetch check: fetch_afrimarket={self.config.fetch_afrimarket}, "
            f"afrimarket_source={'initialized' if self.afrimarket_source else 'None'}"
        )
        if self.config.fetch_afrimarket and self.afrimarket_source:
            try:
                self.logger.info("Fetching data from Afrimarket")
                afm_data = self.afrimarket_source.fetch()

                if not afm_data.empty:
                    afm_data['price_date'] = execution_date
                    all_data.append(afm_data)
                    self.logger.info(
                        f"Fetched {len(afm_data)} records from Afrimarket",
                        extra={"records": len(afm_data), "source": "afrimarket"},
                    )
                else:
                    self.warnings.append("No data from Afrimarket")

            except Exception as e:
                error_msg = f"Afrimarket fetch failed: {str(e)}"
                self.logger.error(error_msg)
                self.errors.append(error_msg)

        if all_data:
            combined = pd.concat(all_data, ignore_index=True)
            self.logger.info(
                f"Total records fetched: {len(combined)}",
                extra={"total_records": len(combined)},
            )
        else:
            combined = pd.DataFrame()
            self.logger.warning("No data from any source - all_data list is empty")

        self.stage_times['fetch_data'] = (datetime.now() - stage_start).total_seconds()
        return combined

    # ------------------------------------------------------------------
    # Staging delegation (public API preserved for Airflow DAG tasks)
    # ------------------------------------------------------------------

    def load_to_staging(self, data: pd.DataFrame, execution_date: date) -> int:
        """Load data to staging tables."""
        return self.staging_manager.load_to_staging(data, execution_date)

    def get_unreconciled_dates(self) -> List[date]:
        """Get all distinct dates with unreconciled staging records."""
        return self.staging_manager.get_unreconciled_dates()

    def get_staging_count(self, price_date: date, source: str) -> int:
        """Get count of staging records for a date and source."""
        return self.staging_manager.get_staging_count(price_date, source)

    def reconcile_all_staging(self, dates: List[date]) -> None:
        """Reconcile prices for multiple dates in staging."""
        self.staging_manager.reconcile_all_staging(dates)

    def get_fact_sync_data(
        self,
        promoted_after: Optional[datetime] = None,
        price_dates: Optional[List[date]] = None,
    ) -> pd.DataFrame:
        """Pull staging rows that should be upserted into fact_daily_prices."""
        return self.staging_manager.get_fact_sync_data(
            promoted_after=promoted_after,
            price_dates=price_dates,
        )

    # ------------------------------------------------------------------
    # Price/stock loading delegation (public API preserved for DAG tasks)
    # ------------------------------------------------------------------

    def load_stocks(self, data: pd.DataFrame) -> int:
        """Load/update stock dimension records."""
        return self.price_loader.load_stocks(data)

    def load_prices(self, data: pd.DataFrame) -> int:
        """Load price records into fact_daily_prices."""
        return self.price_loader.load_prices(data)

    # ------------------------------------------------------------------
    # Validation and transformation (stays in orchestrator — pipeline-level)
    # ------------------------------------------------------------------

    def _validate_data(self, data: pd.DataFrame) -> pd.DataFrame:
        """Validate data quality."""
        stage_start = datetime.now()

        self.logger.info("Stage 2: Validating data quality")

        with self.db.get_session() as session:
            sectors = session.query(DimSector).all()
            valid_sectors = [s.sector_name for s in sectors]

        self.validator = DataValidator(valid_sectors=valid_sectors)

        try:
            cleaned_data, result = self.validator.validate(data)

            self.logger.info(
                f"Validation complete: {result.valid_count} valid, "
                f"{result.suspicious_count} suspicious, {result.invalid_count} invalid",
                extra={
                    "valid": result.valid_count,
                    "suspicious": result.suspicious_count,
                    "invalid": result.invalid_count,
                },
            )

            if result.warnings:
                for warning in result.warnings[:10]:
                    self.warnings.append(f"Validation: {warning.get('warning', 'Unknown')}")

            if result.errors:
                for error in result.errors[:10]:
                    self.errors.append(f"Validation: {error.get('error', 'Unknown')}")

            if not result.is_valid:
                self.logger.warning(
                    f"Validation found {result.invalid_count} invalid records",
                    extra={"invalid_count": result.invalid_count},
                )

            self.stage_times['validate_data'] = (datetime.now() - stage_start).total_seconds()
            return cleaned_data

        except DataValidationError as e:
            self.errors.append(f"Validation error: {str(e)}")
            self.logger.error(f"Validation failed: {str(e)}")
            return pd.DataFrame()

    def transform_data(self, data: pd.DataFrame) -> pd.DataFrame:
        """Transform and standardize data."""
        stage_start = datetime.now()

        self.logger.info("Stage 3: Transforming data")

        try:
            # Pass 'unknown' as the fallback source; per-row source values already
            # present in the DataFrame are preserved by the transformer.
            transformed = self.transformer.transform(data, source='unknown')
            transformed = self.transformer.deduplicate(transformed, keep='last')

            self.logger.info(
                f"Transformation complete: {len(transformed)} records",
                extra={"records": len(transformed)},
            )
            self.stage_times['transform_data'] = (datetime.now() - stage_start).total_seconds()
            return transformed

        except Exception as e:
            self.errors.append(f"Transformation error: {str(e)}")
            self.logger.error("Transformation failed", error=e)
            raise

    # ------------------------------------------------------------------
    # Indicators, alerts, recommendations
    # ------------------------------------------------------------------

    def calculate_indicators(
        self,
        execution_date: date,
        stock_codes: Optional[List[str]],
    ) -> int:
        """Calculate technical indicators."""
        stage_start = datetime.now()

        self.logger.info("Stage 6: Calculating technical indicators")

        calculated_count = 0

        try:
            with self.db.get_session() as session:
                stock_repo = StockRepository(session)
                price_repo = PriceRepository(session)
                indicator_repo = IndicatorRepository(session)

                if stock_codes:
                    stocks = [stock_repo.get_by_code(code) for code in stock_codes]
                    stocks = [s for s in stocks if s is not None]
                else:
                    stocks = stock_repo.get_all_active()

                self.logger.info(
                    f"Calculating indicators for {len(stocks)} stocks",
                    extra={"stocks": len(stocks)},
                )

                indicator_errors = 0
                max_indicator_error_rate = 0.3

                for stock in stocks:
                    try:
                        required_history = self.indicator_calculator.minimum_history_required()
                        prices = price_repo.get_trusted_price_history(
                            stock.stock_id,
                            start_date=None,
                            end_date=execution_date,
                            limit=required_history + 60,
                        )

                        if len(prices) < required_history:
                            self.logger.debug(
                                f"Skipping indicators for {stock.stock_code}: "
                                f"{len(prices)} trusted prices available, need {required_history}"
                            )
                            continue

                        if len(prices) < 2:
                            continue

                        price_data = [{
                            'price_date': p.price_date,
                            'close_price': float(p.close_price),
                        } for p in prices]

                        # Verify that the series has no large calendar gaps.
                        # RSI, MACD, and MAs assume daily continuity; a gap of
                        # more than 5 calendar days (accounting for weekends +
                        # one holiday) indicates missing data that would corrupt
                        # the indicators for the affected stock.
                        from datetime import timedelta
                        dates_sorted = sorted(p.price_date for p in prices)
                        max_gap = max(
                            (dates_sorted[i + 1] - dates_sorted[i]).days
                            for i in range(len(dates_sorted) - 1)
                        ) if len(dates_sorted) > 1 else 0
                        if max_gap > 5:
                            self.logger.warning(
                                f"Skipping indicators for {stock.stock_code}: "
                                f"price history has a {max_gap}-calendar-day gap "
                                f"(max allowed 5) — indicators would be unreliable"
                            )
                            continue

                        indicators = self.indicator_calculator.calculate_for_stock(
                            stock_id=stock.stock_id,
                            stock_code=stock.stock_code,
                            price_history=price_data,
                        )

                        # Always upsert computed rows so reruns repair stale
                        # indicator values instead of preserving old calculations.
                        for indicator in indicators:
                            calculation_date = indicator['calculation_date']
                            values = {
                                k: v for k, v in indicator.items()
                                if k not in ('stock_id', 'calculation_date')
                            }
                            indicator_repo.save_indicators(
                                stock_id=stock.stock_id,
                                calculation_date=calculation_date,
                                indicators=values,
                            )
                            calculated_count += 1

                        session.commit()

                    except Exception as e:
                        indicator_errors += 1
                        error_msg = f"Indicator calculation failed for {stock.stock_code}: {str(e)}"
                        self.logger.warning(error_msg)
                        self.warnings.append(error_msg)
                        session.rollback()

                if stocks and indicator_errors / len(stocks) > max_indicator_error_rate:
                    raise RuntimeError(
                        f"Indicator calculation failed for {indicator_errors}/{len(stocks)} stocks "
                        f"({indicator_errors/len(stocks)*100:.0f}%) — exceeds "
                        f"{max_indicator_error_rate*100:.0f}% error threshold"
                    )

            self.logger.info(
                f"Calculated {calculated_count} indicators",
                extra={"calculated": calculated_count},
            )
            self.stage_times['calculate_indicators'] = (datetime.now() - stage_start).total_seconds()
            return calculated_count

        except Exception as e:
            self.errors.append(f"Indicator calculation error: {str(e)}")
            self.logger.error(f"Indicator calculation failed: {str(e)}")
            return 0

    def evaluate_alerts(self, execution_date: date) -> int:
        """Evaluate alert rules and send notifications."""
        stage_start = datetime.now()

        self.logger.info("Stage 7: Evaluating alert rules")

        try:
            evaluator = AlertEvaluator()
            result = evaluator.evaluate_all_rules(evaluation_date=execution_date)

            if result.alerts:
                saved = evaluator.save_alerts(result.alerts)
                self.logger.info(
                    f"Generated and saved {saved} alerts",
                    extra={"alerts": saved},
                )
                if self.alert_notifier and result.alerts:
                    try:
                        self._send_alert_notifications(result.alerts, execution_date)
                    except Exception as e:
                        self.warnings.append(f"Notification system error: {str(e)}")
                        self.logger.warning(
                            f"Notification failed but pipeline continues: {str(e)}"
                        )
            else:
                self.logger.info("No alerts triggered")

            evaluator.close()
            self.stage_times['evaluate_alerts'] = (datetime.now() - stage_start).total_seconds()
            return result.alerts_generated

        except Exception as e:
            self.errors.append(f"Alert evaluation error: {str(e)}")
            self.logger.error(f"Alert evaluation failed: {str(e)}")
            return 0

    def _send_alert_notifications(self, alerts: List, execution_date: date):
        """
        Send notifications for generated alerts.

        Designed to never fail the pipeline — all notification errors are
        caught, logged, and added to warnings only.
        """
        try:
            channels = []
            if self.settings.notifications.email_enabled:
                channels.append('email')
            if self.settings.notifications.slack_enabled:
                channels.append('slack')

            if not channels:
                self.logger.info("No notification channels enabled - skipping notifications")
                return

            critical_count = 0
            for alert in alerts:
                if alert.severity == 'CRITICAL':
                    try:
                        result = self.alert_notifier.send_alert(alert, channels=channels)
                        if result.success:
                            critical_count += 1
                        else:
                            self.warnings.append(
                                f"Notification failed for alert {alert.alert_id}: "
                                f"{', '.join(result.errors)}"
                            )
                            self.logger.warning(
                                f"Failed to send alert notification: {', '.join(result.errors)}",
                                extra={"alert_id": alert.alert_id},
                            )
                    except Exception as e:
                        self.warnings.append(
                            f"Notification exception for alert {alert.alert_id}: {str(e)}"
                        )
                        self.logger.warning(
                            f"Exception sending alert notification: {str(e)}",
                            extra={"alert_id": alert.alert_id},
                        )

            if critical_count > 0:
                self.logger.info(
                    f"Sent {critical_count} critical alert notifications",
                    extra={"critical_alerts": critical_count},
                )

            warning_alerts = [a for a in alerts if a.severity == 'WARNING']
            if self.settings.notifications.email_enabled and warning_alerts:
                try:
                    digest_result = self.alert_notifier.send_daily_digest(alerts, execution_date)
                    if digest_result.success:
                        self.logger.info(
                            f"Sent daily digest with {len(alerts)} alerts",
                            extra={"total_alerts": len(alerts)},
                        )
                    else:
                        self.warnings.append(
                            f"Daily digest failed: {', '.join(digest_result.errors)}"
                        )
                        self.logger.warning(
                            f"Failed to send daily digest: {', '.join(digest_result.errors)}"
                        )
                except Exception as e:
                    self.warnings.append(f"Daily digest exception: {str(e)}")
                    self.logger.warning(f"Exception sending daily digest: {str(e)}")

        except Exception as e:
            self.warnings.append(f"Notification setup error: {str(e)}")
            self.logger.warning(f"Notification system error (pipeline continues): {str(e)}")

    def generate_recommendations(
        self,
        execution_date: date,
        stock_codes: Optional[List[str]],
    ) -> int:
        """Generate investment recommendations."""
        stage_start = datetime.now()

        if self.config.recommendation_profiles:
            profiles = self.config.recommendation_profiles
        elif self.config.recommendation_profile:
            profiles = [self.config.recommendation_profile]
        else:
            profiles = ['steady_20p_10d', 'steady_20p_20d']

        total_saved = 0
        from app.services.advisory import ProductionPortfolioPolicy, StockScreener

        try:
            with self.db.get_session() as session:
                for profile in profiles:
                    self.logger.info(f"Running recommendation screening for profile: {profile}")
                    screener = StockScreener(session, strategy_profile=profile)
                    rec_repo = RecommendationRepository(session)
                    portfolio_policy = ProductionPortfolioPolicy()

                    recommendations = screener.generate_recommendations(
                        recommendation_date=execution_date,
                        stock_codes=stock_codes,
                        strategy_profile=profile,
                        capture_audit=True,
                    )
                    open_positions = portfolio_policy.count_open_positions(
                        session,
                        recommendation_date=execution_date,
                        profile=profile,
                    )
                    recommendations = portfolio_policy.apply(
                        recommendations,
                        existing_open_positions=open_positions,
                    )
                    screener.apply_portfolio_audit(recommendations)

                    audit_rows = rec_repo.replace_audit_entries(
                        recommendation_date=execution_date,
                        profile=profile,
                        audit_entries=screener.last_audit_entries,
                        full_snapshot=stock_codes is None,
                    )
                    self.logger.info(
                        f"Persisted {audit_rows} recommendation audit rows for {profile}",
                        extra={
                            "recommendation_audit_rows": audit_rows,
                            "recommendation_date": str(execution_date),
                            "profile": profile,
                        },
                    )

                    if stock_codes is None:
                        deleted = rec_repo.delete_recommendations_for_date_profile(
                            recommendation_date=execution_date,
                            profile=profile,
                        )
                        if deleted:
                            self.logger.info(
                                f"Deleted {deleted} existing recommendation rows "
                                f"for {execution_date} and profile {profile} before full regeneration",
                                extra={
                                    "deleted_recommendations": deleted,
                                    "recommendation_date": str(execution_date),
                                    "profile": profile,
                                },
                            )

                    if recommendations:
                        saved = rec_repo.create_recommendations_bulk(recommendations)
                        approved = sum(1 for rec in recommendations if rec.portfolio_approved)
                        total_saved += saved

                        self.logger.info(
                            f"Generated {saved} screening signals for {profile}; "
                            f"{approved} portfolio-approved",
                            extra={
                                "recommendations": saved,
                                "portfolio_approved": approved,
                                "open_positions_before": open_positions,
                                "profile": profile,
                            },
                        )

                        buy_picks = [
                            r for r in recommendations
                            if r.portfolio_approved
                            and r.action_type.value in ('BUY', 'STRONG_BUY')
                        ]
                        if buy_picks:
                            top_3 = buy_picks[:3]
                            self.logger.info(
                                f"Top {len(top_3)} buy signals for {profile}:",
                                extra={"top_picks": len(top_3), "profile": profile},
                            )
                            for i, rec in enumerate(top_3, 1):
                                self.logger.info(
                                    f"  {i}. {rec.stock_code} - "
                                    f"Score: {rec.score:.1f}, "
                                    f"Action: {rec.action_type.value}, "
                                    f"Technical Signal: {rec.signal_type.value}, "
                                    f"Signal Agreement: {rec.signal_agreement*100:.0f}%"
                                )
                    else:
                        self.logger.info(f"No signals generated for profile {profile} (filters applied)")

                    screener.close()

                session.commit()
                self.stage_times['generate_recommendations'] = (
                    datetime.now() - stage_start
                ).total_seconds()
                return total_saved

        except Exception as e:
            self.errors.append(f"Recommendation generation error: {str(e)}")
            self.logger.error("Recommendation generation failed", error=e)
            return 0

    # ------------------------------------------------------------------
    # Result building and logging
    # ------------------------------------------------------------------

    def _build_result(
        self,
        start_time: datetime,
        success: bool,
        stocks: int,
        prices: int,
        indicators: int,
        alerts: int,
        recommendations: int = 0,
    ) -> PipelineResult:
        """Build pipeline result, reading staging metrics from StagingManager."""
        execution_time = (datetime.now() - start_time).total_seconds()
        sm = self.staging_manager

        return PipelineResult(
            success=success,
            execution_time=execution_time,
            stocks_processed=stocks,
            prices_loaded=prices,
            indicators_calculated=indicators,
            alerts_generated=alerts,
            recommendations_generated=recommendations,
            errors=list(self.errors),
            warnings=list(self.warnings),
            stage_times=dict(self.stage_times),
            staging_loaded=sm.staging_loaded if sm else 0,
            reconciled_count=sm.reconciled_count if sm else 0,
            conflicts_flagged=sm.conflicts_flagged if sm else 0,
            avg_price_variance=sm.avg_price_variance if sm else 0.0,
        )

    def _log_summary(self, result: PipelineResult):
        """Log execution summary."""
        self.logger.info("=" * 80)
        self.logger.info("Pipeline Execution Summary")
        self.logger.info("=" * 80)
        self.logger.info(f"Status: {'SUCCESS' if result.success else 'FAILED'}")
        self.logger.info(f"Execution time: {result.execution_time:.2f}s")
        self.logger.info(f"Stocks processed: {result.stocks_processed}")
        self.logger.info(f"Prices loaded: {result.prices_loaded}")
        self.logger.info(f"Indicators calculated: {result.indicators_calculated}")
        self.logger.info(f"Alerts generated: {result.alerts_generated}")
        self.logger.info(f"Recommendations generated: {result.recommendations_generated}")

        if result.staging_loaded > 0:
            self.logger.info(f"\nReconciliation Metrics:")
            self.logger.info(f"  Staging loaded: {result.staging_loaded}")
            self.logger.info(f"  Reconciled: {result.reconciled_count}")
            self.logger.info(f"  Conflicts flagged: {result.conflicts_flagged}")
            self.logger.info(f"  Avg variance: {result.avg_price_variance:.2f}%")

        if result.stage_times:
            self.logger.info("\nStage execution times:")
            for stage, time in result.stage_times.items():
                self.logger.info(f"  {stage}: {time:.2f}s")

        if result.errors:
            self.logger.info(f"\nErrors ({len(result.errors)}):")
            for error in result.errors[:10]:
                self.logger.info(f"  - {error}")

        if result.warnings:
            self.logger.info(f"\nWarnings ({len(result.warnings)}):")
            for warning in result.warnings[:10]:
                self.logger.info(f"  - {warning}")

        self.logger.info("=" * 80)
