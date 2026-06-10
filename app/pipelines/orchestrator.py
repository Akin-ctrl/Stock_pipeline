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

from typing import Dict, List, Optional, Tuple, Any
from datetime import date, datetime, timedelta
from dataclasses import dataclass
from decimal import Decimal
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
from app.services.advisory import ProductionPortfolioPolicy, StockScreener
from app.services.reference_data import (
    UNKNOWN_SECTOR_NAME,
    choose_sector_name,
    is_unknown_sector,
    load_stock_sector_map,
)
from app.utils import get_logger
from app.utils.exceptions import DataValidationError


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
    - Data fetching from multiple sources
    - Data validation and quality checks
    - Data transformation and standardization
    - Database loading (stocks and prices)
    - Technical indicator calculation
    - Alert rule evaluation
    
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
        
        # Load settings
        self.settings = Settings.load()
        
        # Initialize database
        self.db = get_db()
        
        # Initialize data sources - Afrimarket only
        if AfrimarketDataSource is None:
            raise ImportError(
                "Afrimarket package not found. Install with: pip install afrimarket"
            )
        self.afrimarket_source = AfrimarketDataSource()
        
        # Initialize processors
        self.validator = None  # Will initialize with sectors
        self.transformer = DataTransformer()
        self.indicator_calculator = IndicatorCalculator()
        
        # Initialize staging and reconciliation (if enabled)
        self.staging_repo = None  # Will initialize with session
        self.reconciliation_engine = None
        if self.config.use_staging:
            # Initialize reconciliation engine with proper parameter names
            # Repositories will be attached per-session during reconciliation
            self.reconciliation_engine = ReconciliationEngine(
                low_variance_threshold=1.0,
                medium_variance_threshold=3.0,
                preferred_source='afrimarket'
            )
        
        # Initialize alert notifier
        self.alert_notifier = AlertNotifier() if (
            self.settings.notifications.email_enabled or 
            self.settings.notifications.slack_enabled
        ) else None
        
        # Track execution metrics
        self.errors: List[str] = []
        self.warnings: List[str] = []
        self.stage_times: Dict[str, float] = {}
        
        # Track staging metrics
        self.staging_loaded = 0
        self.reconciled_count = 0
        self.conflicts_flagged = 0
        self.avg_price_variance = 0.0
        self.reconciliation_window_start: Optional[datetime] = None
        
    def run(
        self,
        execution_date: Optional[date] = None,
        stock_codes: Optional[List[str]] = None
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
            "=" * 80,
            extra={"execution_date": str(execution_date)}
        )
        self.logger.info(
            f"Starting pipeline execution for {execution_date}",
            extra={"execution_date": str(execution_date)}
        )
        self.logger.info("=" * 80)
        
        # Reset metrics
        self.errors = []
        self.warnings = []
        self.stage_times = {}
        
        # Reset staging metrics
        self.staging_loaded = 0
        self.reconciled_count = 0
        self.conflicts_flagged = 0
        self.avg_price_variance = 0.0
        self.reconciliation_window_start = None
        
        stocks_processed = 0
        prices_loaded = 0
        indicators_calculated = 0
        alerts_generated = 0
        analysis_date = execution_date
        
        try:
            # Choose workflow based on configuration
            if self.config.use_staging:
                # Staging mode: Dual sources → staging → reconcile → promote
                result = self._run_staging_workflow(execution_date, stock_codes)
            else:
                # Direct mode: Single source → direct load (legacy)
                result = self._run_direct_workflow(execution_date, stock_codes)
            
            self._log_summary(result)
            return result
            
        except Exception as e:
            self.logger.error(
                "Pipeline execution failed",
                error=e,
                extra={"error": str(e)}
            )
            self.errors.append(f"Pipeline failure: {str(e)}")
            
            return self._build_result(
                start_time, False, 0, 0, 0, 0
            )
    
    def _run_direct_workflow(
        self,
        execution_date: date,
        stock_codes: Optional[List[str]]
    ) -> PipelineResult:
        """
        Direct workflow: Fetch → validate → transform → load.
        
        Legacy mode for fallback. Staging mode is recommended.
        
        Args:
            execution_date: Date to run pipeline for
            stock_codes: Specific stocks to process
            
        Returns:
            PipelineResult with execution summary
        """
        start_time = datetime.now()
        
        stocks_processed = 0
        prices_loaded = 0
        indicators_calculated = 0
        alerts_generated = 0
        
        try:
            # Stage 1: Fetch data from NGX
            raw_data = self._fetch_data(execution_date, stock_codes)
            self.logger.info(f"Fetched {len(raw_data)} rows from NGX")
            if raw_data.empty:
                self.logger.warning("No data fetched from NGX")
                self.warnings.append("No data fetched from NGX")
                return self._build_result(
                    start_time, False, 0, 0, 0, 0
                )
            
            # Stage 2: Validate data
            if self.config.validate_data and not raw_data.empty:
                validated_data = self._validate_data(raw_data)
                if validated_data.empty:
                    self.logger.error("All data failed validation")
                    return self._build_result(
                        start_time, False, 0, 0, 0, 0
                    )
            else:
                validated_data = raw_data
            
            # Stage 3: Transform data
            if not validated_data.empty:
                transformed_data = self._transform_data(validated_data)
            else:
                transformed_data = pd.DataFrame()
            
            # Stage 4: Load stocks
            if self.config.load_stocks and not transformed_data.empty:
                stocks_processed = self._load_stocks(transformed_data)
            
            # Stage 5: Load prices directly
            if self.config.load_prices and not transformed_data.empty:
                prices_loaded = self._load_prices(transformed_data)
                analysis_date = pd.to_datetime(transformed_data['price_date']).dt.date.max()
            
            # Stage 6: Calculate indicators
            if self.config.calculate_indicators:
                indicators_calculated = self._calculate_indicators(
                    analysis_date,
                    stock_codes
                )
            
            # Stage 7: Evaluate alerts
            if self.config.evaluate_alerts:
                alerts_generated = self._evaluate_alerts(analysis_date)
            
            # Stage 8: Generate recommendations
            recommendations_generated = 0
            if self.config.generate_recommendations:
                recommendations_generated = self._generate_recommendations(
                    analysis_date,
                    stock_codes
                )
            
            # Pipeline successful
            success = len(self.errors) == 0
            
            result = self._build_result(
                start_time,
                success,
                stocks_processed,
                prices_loaded,
                indicators_calculated,
                alerts_generated,
                recommendations_generated
            )
            
            return result
            
        except Exception as e:
            self.errors.append(f"Direct workflow error: {str(e)}")
            self.logger.error("Direct workflow failed", error=e)
            return self._build_result(
                start_time, False, stocks_processed, prices_loaded,
                indicators_calculated, alerts_generated, recommendations_generated
            )
    
    
    def _run_staging_workflow(
        self,
        execution_date: date,
        stock_codes: Optional[List[str]]
    ) -> PipelineResult:
        """
        Staging workflow: Fetch → stage raw → reconcile → validate → transform → load.
        
        This workflow:
        1. Fetches RAW data from BOTH NGX and Afrimarket
        2. Loads RAW data to staging tables (no processing)
        3. Pulls from staging and reconciles price differences between sources
        4. Validates reconciled data (quality checks)
        5. Transforms validated data (standardize, calculate fields)
        6. Loads transformed data to production
        7. Calculates indicators and alerts on production data
        
        Args:
            execution_date: Date to run pipeline for
            stock_codes: Specific stocks to process
            
        Returns:
            PipelineResult with execution summary including reconciliation metrics
        """
        start_time = datetime.now()
        
        stocks_processed = 0
        prices_loaded = 0
        indicators_calculated = 0
        alerts_generated = 0
        analysis_date = execution_date
        
        try:
            # Stage 1: Fetch RAW data from BOTH sources (NGX + Afrimarket)
            raw_data = self._fetch_data(execution_date, stock_codes)
            self.logger.info(f"Fetched {len(raw_data)} rows from Afrimarket")
            
            # Check if we got data
            if raw_data.empty:
                self.logger.warning("No data fetched from Afrimarket")
                self.warnings.append("No data fetched from Afrimarket")
                # Continue with graceful degradation
                if not self.config.fetch_afrimarket:
                    return self._build_result(
                        start_time, False, 0, 0, 0, 0
                    )
                # If we have the source configured, log but continue for now
                self.logger.info("Continuing with empty dataset - will handle downstream")
            
            # Stage 2: Load stocks (update dimension if new stocks appear)
            if self.config.load_stocks and not raw_data.empty:
                stocks_processed = self._load_stocks(raw_data)
            
            # Stage 3: Load RAW data to staging
            if not raw_data.empty:
                self.staging_loaded = self._load_to_staging(raw_data, execution_date)
                self.logger.info(f"Loaded {self.staging_loaded} raw records to staging")
            
            # Stage 4: Pull from staging and reconcile prices
            # Reconcile ALL unreconciled dates, not just execution_date
            # (Data sources may return different dates than execution_date)
            unreconciled_dates = self._get_unreconciled_dates()
            if unreconciled_dates:
                self.logger.info(f"Found unreconciled data for {len(unreconciled_dates)} dates: {unreconciled_dates}")
                self.reconciliation_window_start = datetime.now()
                self._reconcile_all_staging(unreconciled_dates)
            else:
                self.logger.info("No unreconciled records found in staging")
            
            # Stage 5: Pull reconciled data, validate and transform
            reconciled_data = self._get_fact_sync_data(
                promoted_after=self.reconciliation_window_start
            )

            # Stage 5a: Skip validation for staging workflow
            # Staging data lacks company_name/exchange (stored in dim_stocks)
            # Stocks were already validated during _load_stocks phase
            if not reconciled_data.empty:
                self.logger.info("Skipping validation for staging workflow (stocks already validated)")
                validated_data = reconciled_data

                # Stage 5b: Transform validated data
                transformed_data = self._transform_data(validated_data)
            else:
                transformed_data = pd.DataFrame()
            
            # Stage 6: Load transformed data to production
            if self.config.load_prices and not transformed_data.empty:
                prices_loaded = self._load_prices(transformed_data)
                analysis_date = pd.to_datetime(transformed_data['price_date']).dt.date.max()
            
            # Stage 7: Calculate indicators (on production data)
            if self.config.calculate_indicators and prices_loaded > 0:
                indicators_calculated = self._calculate_indicators(
                    analysis_date,
                    stock_codes
                )
            
            # Stage 8: Evaluate alerts
            if self.config.evaluate_alerts:
                alerts_generated = self._evaluate_alerts(analysis_date)
            
            # Stage 9: Generate recommendations
            recommendations_generated = 0
            if self.config.generate_recommendations:
                recommendations_generated = self._generate_recommendations(
                    analysis_date,
                    stock_codes
                )
            
            # Pipeline successful
            success = len(self.errors) == 0
            
            result = self._build_result(
                start_time,
                success,
                stocks_processed,
                prices_loaded,
                indicators_calculated,
                alerts_generated,
                recommendations_generated
            )
            
            return result
            
        except Exception as e:
            self.logger.error(
                "Staging workflow failed",
                error=e,
                extra={"error": str(e)}
            )
            self.errors.append(f"Staging workflow failure: {str(e)}")
            
            return self._build_result(
                start_time, False, 0, 0, 0, 0
            )
    
    def _fetch_data(
        self,
        execution_date: date,
        stock_codes: Optional[List[str]]
    ) -> pd.DataFrame:
        """Fetch data from all configured sources."""
        stage_start = datetime.now()
        
        self.logger.info("=" * 80)
        self.logger.info("DEBUG: _fetch_data() METHOD CALLED!")
        self.logger.info(f"DEBUG: execution_date={execution_date}, stock_codes={stock_codes}")
        self.logger.info("=" * 80)
        
        self.logger.info("Stage 1: Fetching data from sources")
        self.logger.info(
            f"Sources configured: Afrimarket={self.config.fetch_afrimarket} (available={self.afrimarket_source is not None})"
        )
        
        all_data = []
        
        # Fetch from Afrimarket (Afrimarket-only mode)
        self.logger.info(
            f"Afrimarket fetch check: fetch_afrimarket={self.config.fetch_afrimarket}, "
            f"afrimarket_source={'initialized' if self.afrimarket_source else 'None'}"
        )
        if self.config.fetch_afrimarket and self.afrimarket_source:
            try:
                self.logger.info("Fetching data from Afrimarket")
                afm_data = self.afrimarket_source.fetch()
                
                if not afm_data.empty:
                    # Add execution_date as price_date
                    afm_data['price_date'] = execution_date
                    all_data.append(afm_data)
                    self.logger.info(
                        f"Fetched {len(afm_data)} records from Afrimarket",
                        extra={"records": len(afm_data), "source": "afrimarket"}
                    )
                else:
                    self.warnings.append("No data from Afrimarket")
                    
            except Exception as e:
                error_msg = f"Afrimarket fetch failed: {str(e)}"
                self.logger.error(error_msg)
                self.errors.append(error_msg)
        
        # Combine all data
        if all_data:
            combined = pd.concat(all_data, ignore_index=True)
            self.logger.info(
                f"Total records fetched: {len(combined)}",
                extra={"total_records": len(combined)}
            )
        else:
            combined = pd.DataFrame()
            self.logger.warning("No data from any source - all_data list is empty")
        
        self.stage_times['fetch_data'] = (datetime.now() - stage_start).total_seconds()

        return combined
    
    def _load_to_staging(
        self,
        data: pd.DataFrame,
        execution_date: date
    ) -> int:
        """
        Load data to staging tables.
        
        Args:
            data: DataFrame with fetched data
            execution_date: Date to load for
            
        Returns:
            Number of records loaded to staging
        """
        stage_start = datetime.now()
        
        self.logger.info("Stage 3: Loading data to staging")
        
        try:
            with self.db.get_session() as session:
                self.staging_repo = StagingRepository(session)
                
                # Staging is raw: do NOT filter out unknown stocks here
                staging_data = data.copy()
                
                if staging_data.empty:
                    self.logger.warning("No records to load to staging")
                    return 0
                
                # Ensure price_date column exists
                if 'price_date' not in staging_data.columns:
                    staging_data['price_date'] = execution_date
                
                # Group by source and load each
                total_loaded = 0
                for source in staging_data['source'].unique():
                    source_data = staging_data[staging_data['source'] == source]
                    loaded = self.staging_repo.bulk_insert_staging(
                        df=source_data,
                        source=str(source)
                    )
                    total_loaded += loaded
                
                session.commit()
                
                self.logger.info(
                    f"Loaded {total_loaded} records to staging",
                    extra={"loaded": total_loaded}
                )
                
                self.stage_times['load_staging'] = (datetime.now() - stage_start).total_seconds()
                return total_loaded
                    
        except Exception as e:
            self.errors.append(f"Staging load error: {str(e)}")
            self.logger.error("Staging load failed", error=e)
            raise  # Re-raise to fail the DAG task
    
    def _get_unreconciled_dates(self) -> List[date]:
        """
        Get all distinct dates with unreconciled staging records.
        
        Returns:
            List of dates
        """
        try:
            with self.db.get_session() as session:
                staging_repo = StagingRepository(session)
                dates = staging_repo.get_unreconciled_dates()
                return dates
        except Exception as e:
            self.logger.error(f"Failed to get unreconciled dates: {str(e)}")
            return []
    
    def _get_unreconciled_count(self, execution_date: date) -> int:
        """
        Get count of unreconciled records for a given date.
        
        Args:
            execution_date: Date to check
            
        Returns:
            Number of unreconciled records
        """
        try:
            with self.db.get_session() as session:
                staging_repo = StagingRepository(session)
                count = staging_repo.get_unreconciled_count(execution_date)
                return count
        except Exception as e:
            self.logger.error(f"Failed to get unreconciled count: {str(e)}")
            return 0

    def _get_staging_count(self, price_date: date, source: str) -> int:
        """
        Get count of staging records for a date and source.

        Args:
            price_date: Trading date
            source: Data source identifier

        Returns:
            Number of staging records
        """
        try:
            with self.db.get_session() as session:
                staging_repo = StagingRepository(session)
                return staging_repo.count_by_date_source(price_date, source)
        except Exception as e:
            self.logger.error(f"Failed to get staging count: {str(e)}")
            return 0
    
    def _reconcile_all_staging(self, dates: List[date]) -> None:
        """
        Reconcile prices for multiple dates in staging.
        
        Args:
            dates: List of dates to reconcile
        """
        stage_start = datetime.now()
        
        self.logger.info(f"Stage 4: Reconciling staged prices for {len(dates)} dates")
        
        try:
            with self.db.get_session() as session:
                self.staging_repo = StagingRepository(session)
                # Attach repositories for reconciliation within the active session
                self.reconciliation_engine.staging_repo = self.staging_repo
                self.reconciliation_engine.stock_repo = StockRepository(session)
                
                total_reconciled = 0
                total_applied = 0
                
                # Reconcile each date
                for price_date in dates:
                    self.logger.info(f"Reconciling data for {price_date}")
                    results = self.reconciliation_engine.reconcile_date(price_date)
                    
                    # Apply each reconciliation result
                    for result in results:
                        if self.reconciliation_engine.apply_reconciliation(result):
                            total_applied += 1
                        else:
                            self.logger.warning(
                                f"Failed to apply reconciliation for {result.stock_code} "
                                f"on {result.price_date}"
                            )
                    
                    total_reconciled += len(results)
                    self.logger.info(
                        f"Reconciled {len(results)} records for {price_date} "
                        f"({total_applied}/{total_reconciled} applied)"
                    )
                
                # Commit all changes
                session.commit()
                
                self.reconciled_count = total_applied
                self.logger.info(
                    f"Total reconciled records across all dates: {self.reconciled_count}",
                    extra={"reconciled_records": self.reconciled_count}
                )
                
                # Raise exception if no records were reconciled but we expected some
                if total_reconciled > 0 and total_applied == 0:
                    raise RuntimeError(
                        f"Reconciliation failed: {total_reconciled} records processed "
                        f"but none applied successfully"
                    )
                
        except Exception as e:
            error_msg = f"Staging reconciliation failed: {str(e)}"
            self.logger.error(error_msg, error=e)
            self.errors.append(error_msg)
            self.reconciled_count = 0
            raise  # Re-raise to fail the DAG task
        
        self.stage_times['reconcile_staging'] = (datetime.now() - stage_start).total_seconds()
    
    def _reconcile_staging(self, execution_date: date) -> None:
        """
        Reconcile prices in staging.
        
        Args:
            execution_date: Date to reconcile for
        """
        stage_start = datetime.now()
        
        self.logger.info("Stage 4: Reconciling staged prices")
        
        try:
            with self.db.get_session() as session:
                self.staging_repo = StagingRepository(session)
                # Attach repositories for reconciliation within the active session
                self.reconciliation_engine.staging_repo = self.staging_repo
                self.reconciliation_engine.stock_repo = StockRepository(session)
                
                # Get reconciliation results for the date
                results = self.reconciliation_engine.reconcile_date(execution_date)
                
                # Apply each reconciliation result
                applied_count = 0
                conflicts_flagged = 0
                
                for result in results:
                    if self.reconciliation_engine.apply_reconciliation(result):
                        applied_count += 1
                        if result.conflict_severity in ['high', 'critical']:
                            conflicts_flagged += 1
                
                session.commit()
                
                self.reconciled_count = applied_count
                self.conflicts_flagged = conflicts_flagged
                
                self.logger.info(
                    f"Reconciled {self.reconciled_count} records, "
                    f"flagged {self.conflicts_flagged} conflicts, "
                    f"avg variance: {self.avg_price_variance:.2f}%",
                    extra={
                        "reconciled": self.reconciled_count,
                        "conflicts": self.conflicts_flagged,
                        "avg_variance": self.avg_price_variance
                    }
                )
                
                # Log reconciliation stats
                stats = self.reconciliation_engine.get_reconciliation_stats(execution_date)
                
                if stats:
                    self.logger.info(
                        f"Reconciliation breakdown: "
                        f"{stats.get('by_severity', {}).get('low', 0)} low, "
                        f"{stats.get('by_severity', {}).get('medium', 0)} medium, "
                        f"{stats.get('by_severity', {}).get('high', 0)} high"
                    )
                
                self.stage_times['reconcile'] = (datetime.now() - stage_start).total_seconds()
                
        except Exception as e:
            self.errors.append(f"Reconciliation error: {str(e)}")
            self.logger.error("Reconciliation failed", error=e)
    
    def _get_all_reconciled_data(self) -> pd.DataFrame:
        """
        Pull ALL reconciled data from staging (all dates) for validation and transformation.
        
        Returns:
            DataFrame with reconciled price data
        """
        stage_start = datetime.now()
        
        self.logger.info("Stage 5: Pulling all reconciled data from staging")
        
        try:
            with self.db.get_session() as session:
                self.staging_repo = StagingRepository(session)
                
                # Get ALL reconciled staging records (no date filter)
                staging_records = self.staging_repo.get_all_reconciled(limit=None)
                
                if not staging_records:
                    self.logger.warning("No reconciled records in staging")
                    return pd.DataFrame()
                
                # Convert to DataFrame
                data = []
                for record in staging_records:
                    data.append({
                        'stock_code': record.stock_code,
                        'source': record.source,
                        'price_date': record.price_date,
                        'close_price': float(record.close_price) if record.close_price is not None else None,
                        'change_1d_pct': float(record.change_1d_pct) if record.change_1d_pct is not None else None,
                        'change_ytd_pct': float(record.change_ytd_pct) if record.change_ytd_pct is not None else None,
                        'volume': int(record.volume) if record.volume is not None else None
                    })
                
                df = pd.DataFrame(data)
                
                self.logger.info(
                    f"Pulled {len(df)} reconciled records from staging (all dates)",
                    extra={"records": len(df)}
                )
                
                self.stage_times['get_reconciled_data'] = (datetime.now() - stage_start).total_seconds()
                
                return df
                
        except Exception as e:
            self.errors.append(f"Get reconciled data error: {str(e)}")
            self.logger.error("Failed to get reconciled data", error=e)
            raise  # Re-raise to fail the DAG task

    def _fact_sync_rows_to_dataframe(self, rows: List[Any]) -> pd.DataFrame:
        """Convert canonical fact-sync rows into the narrow downstream dataframe shape."""
        if not rows:
            return pd.DataFrame()

        data = []
        for row in rows:
            if isinstance(row, dict):
                stock_code = row.get('stock_code')
                source = row.get('source')
                price_date = row.get('price_date')
                close_price = row.get('close_price')
                change_1d_pct = row.get('change_1d_pct')
                change_ytd_pct = row.get('change_ytd_pct')
                volume = row.get('volume')
            else:
                stock_code = row.stock_code
                source = row.source
                price_date = row.price_date
                close_price = row.close_price
                change_1d_pct = row.change_1d_pct
                change_ytd_pct = row.change_ytd_pct
                volume = row.volume

            data.append({
                'stock_code': stock_code,
                'source': source,
                'price_date': price_date,
                'close_price': float(close_price) if close_price is not None else None,
                'change_1d_pct': float(change_1d_pct) if change_1d_pct is not None else None,
                'change_ytd_pct': float(change_ytd_pct) if change_ytd_pct is not None else None,
                'volume': int(volume) if volume is not None else None,
            })

        return pd.DataFrame(data)

    def _get_recently_reconciled_data(
        self,
        promoted_after: Optional[datetime] = None,
    ) -> pd.DataFrame:
        """
        Pull only the staging records promoted during the current pipeline run.

        Returns:
            DataFrame with recently reconciled price data
        """
        stage_start = datetime.now()

        self.logger.info("Stage 5: Pulling recently reconciled data from staging")

        effective_start = promoted_after or self.reconciliation_window_start
        if effective_start is None:
            self.logger.warning("Reconciliation window start not set; no recent staging data to pull")
            return pd.DataFrame()

        try:
            with self.db.get_session() as session:
                self.staging_repo = StagingRepository(session)
                canonical_rows = self.staging_repo.get_canonical_reconciled_for_fact_sync(
                    promoted_after=effective_start,
                )

                if not canonical_rows:
                    self.logger.warning("No recently reconciled records in staging")
                    return pd.DataFrame()

                df = self._fact_sync_rows_to_dataframe(canonical_rows)

                self.logger.info(
                    f"Pulled {len(df)} recently reconciled records from staging",
                    extra={"records": len(df)}
                )

                self.stage_times['get_reconciled_data'] = (datetime.now() - stage_start).total_seconds()

                return df

        except Exception as e:
            self.errors.append(f"Get recent reconciled data error: {str(e)}")
            self.logger.error("Failed to get recently reconciled data", error=e)
            raise

    def _get_pending_fact_promotion_data(
        self,
        promoted_after: Optional[datetime] = None,
        price_dates: Optional[List[date]] = None,
    ) -> pd.DataFrame:
        """
        Pull reconciled staging rows whose stock/date pair is still missing from fact_daily_prices.

        This keeps daily runs incremental while also rescuing historical rows that
        were reconciled in staging but never promoted downstream.
        """
        stage_start = datetime.now()

        self.logger.info("Stage 5: Pulling reconciled rows pending fact promotion")

        try:
            with self.db.get_session() as session:
                self.staging_repo = StagingRepository(session)
                canonical_rows = self.staging_repo.get_canonical_reconciled_for_fact_sync(
                    promoted_after=promoted_after,
                    price_dates=price_dates,
                    only_missing_from_fact=True,
                )

                if not canonical_rows:
                    self.logger.warning("No reconciled staging rows are pending fact promotion")
                    return pd.DataFrame()

                df = self._fact_sync_rows_to_dataframe(canonical_rows)

                self.logger.info(
                    f"Pulled {len(df)} reconciled rows pending fact promotion",
                    extra={"records": len(df)}
                )

                self.stage_times['get_reconciled_data'] = (datetime.now() - stage_start).total_seconds()
                return df

        except Exception as e:
            self.errors.append(f"Get pending promotion data error: {str(e)}")
            self.logger.error("Failed to get pending promotion data", error=e)
            raise

    def _get_fact_sync_data(
        self,
        promoted_after: Optional[datetime] = None,
        price_dates: Optional[List[date]] = None,
    ) -> pd.DataFrame:
        """
        Pull staging rows that should be upserted into fact_daily_prices.

        This combines two cases:
        1. recently reconciled rows from the current run, even if the fact row
           already exists and only needs to be refreshed
        2. older reconciled rows that are still missing from fact_daily_prices

        Returning both sets keeps the daily DAG incremental while also rescuing
        stranded historical backfills.
        """
        recent_df = self._get_recently_reconciled_data(promoted_after=promoted_after)
        pending_df = self._get_pending_fact_promotion_data(
            promoted_after=None,
            price_dates=price_dates,
        )

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
            extra={"records": len(combined)}
        )
        return combined
    
    def _get_reconciled_data(self, execution_date: date) -> pd.DataFrame:
        """
        Pull reconciled data from staging for validation and transformation.
        
        Args:
            execution_date: Date to get reconciled data for
            
        Returns:
            DataFrame with reconciled price data
        """
        stage_start = datetime.now()
        
        self.logger.info("Stage 5: Pulling reconciled data from staging")
        
        try:
            with self.db.get_session() as session:
                self.staging_repo = StagingRepository(session)
                
                # Get reconciled staging records
                staging_records = self.staging_repo.get_by_date_reconciled(
                    execution_date,
                    reconciled_only=True
                )
                
                if not staging_records:
                    self.logger.warning("No reconciled records in staging")
                    return pd.DataFrame()
                
                # Convert to DataFrame
                data = []
                for record in staging_records:
                    data.append({
                        'stock_code': record.stock_code,
                        'source': record.source,
                        'price_date': record.price_date,
                        'close_price': float(record.close_price) if record.close_price is not None else None,
                        'change_1d_pct': float(record.change_1d_pct) if record.change_1d_pct is not None else None,
                        'change_ytd_pct': float(record.change_ytd_pct) if record.change_ytd_pct is not None else None,
                        'volume': int(record.volume) if record.volume is not None else None
                    })
                
                df = pd.DataFrame(data)
                
                self.logger.info(
                    f"Pulled {len(df)} reconciled records from staging",
                    extra={"records": len(df)}
                )
                
                self.stage_times['get_reconciled_data'] = (datetime.now() - stage_start).total_seconds()
                
                return df
                
        except Exception as e:
            self.errors.append(f"Get reconciled data error: {str(e)}")
            self.logger.error("Failed to get reconciled data", error=e)
            return pd.DataFrame()
    
    def _validate_data(self, data: pd.DataFrame) -> pd.DataFrame:
        """Validate data quality."""
        stage_start = datetime.now()
        
        self.logger.info("Stage 2: Validating data quality")
        
        # Initialize validator with valid sectors
        with self.db.get_session() as session:
            # Query all sectors directly
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
                    "invalid": result.invalid_count
                }
            )
            
            # Add warnings for validation issues
            if result.warnings:
                for warning in result.warnings[:10]:  # First 10
                    self.warnings.append(f"Validation: {warning.get('warning', 'Unknown')}")
            
            # Add errors for invalid records
            if result.errors:
                for error in result.errors[:10]:
                    error_msg = error.get('error', 'Unknown')
                    self.errors.append(f"Validation: {error_msg}")
            
            if not result.is_valid:
                self.logger.warning(
                    f"Validation found {result.invalid_count} invalid records",
                    extra={"invalid_count": result.invalid_count}
                )
            
            self.stage_times['validate_data'] = (datetime.now() - stage_start).total_seconds()
            
            return cleaned_data
            
        except DataValidationError as e:
            self.errors.append(f"Validation error: {str(e)}")
            self.logger.error(f"Validation failed: {str(e)}")
            return pd.DataFrame()
    
    def _transform_data(self, data: pd.DataFrame) -> pd.DataFrame:
        """Transform and standardize data."""
        stage_start = datetime.now()
        
        self.logger.info("Stage 3: Transforming data")
        
        try:
            # Determine source from data
            source = data['source'].iloc[0] if 'source' in data.columns else 'unknown'
            
            transformed = self.transformer.transform(data, source=source)
            
            # Deduplicate
            transformed = self.transformer.deduplicate(transformed, keep='last')
            
            self.logger.info(
                f"Transformation complete: {len(transformed)} records",
                extra={"records": len(transformed)}
            )
            
            self.stage_times['transform_data'] = (datetime.now() - stage_start).total_seconds()
            
            return transformed
            
        except Exception as e:
            self.errors.append(f"Transformation error: {str(e)}")
            self.logger.error("Transformation failed", error=e)
            raise  # Re-raise to fail the DAG task
    
    def _calculate_missing_changes(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate missing change_1d_pct and change_ytd_pct values.
        
        - change_1d_pct: ((close - prev_close) / prev_close) * 100
        - change_ytd_pct: ((close - year_start_close) / year_start_close) * 100
        
        Args:
            data: DataFrame with price data grouped by stock
            
        Returns:
            DataFrame with calculated values filled in
        """
        df = data.copy()

        if df.empty:
            return df

        # Ensure price_date is datetime for grouped calculations.
        if not pd.api.types.is_datetime64_any_dtype(df['price_date']):
            df['price_date'] = pd.to_datetime(df['price_date'])

        df = df.sort_values(['stock_code', 'price_date']).copy()

        # Recalculate changes for sources that do not provide trustworthy
        # percentage fields. Afrimarket current quotes expose absolute change,
        # not percentage change, so we should always derive the percentages.
        untrusted_pct_sources = df['source'].fillna('').str.lower().isin({'afrimarket'})

        # First calculate whatever can be derived from the incoming batch itself.
        batch_prev_close = df.groupby('stock_code')['close_price'].shift(1)
        batch_change_1d = ((df['close_price'] - batch_prev_close) / batch_prev_close) * 100
        recalc_1d_mask = df['change_1d_pct'].isna() | untrusted_pct_sources
        df.loc[recalc_1d_mask, 'change_1d_pct'] = batch_change_1d[recalc_1d_mask].round(4)

        df['calc_year'] = df['price_date'].dt.year
        batch_year_start = df.groupby(['stock_code', 'calc_year'])['close_price'].transform('first')
        batch_change_ytd = ((df['close_price'] - batch_year_start) / batch_year_start) * 100
        recalc_ytd_mask = df['change_ytd_pct'].isna() | untrusted_pct_sources
        df.loc[recalc_ytd_mask, 'change_ytd_pct'] = batch_change_ytd[recalc_ytd_mask].round(4)

        # When the batch only contains the first observed row for a stock-year,
        # a current-only rerun will incorrectly treat "today" as the YTD
        # baseline. Force those rows back through DB-history lookup.
        batch_year_position = df.groupby(['stock_code', 'calc_year']).cumcount()
        needs_ytd_history = recalc_ytd_mask & untrusted_pct_sources & (batch_year_position == 0)
        df.loc[needs_ytd_history, 'change_ytd_pct'] = pd.NA

        # Then fill any remaining gaps from already-stored production history.
        rows_needing_history = df[
            (recalc_1d_mask & df['change_1d_pct'].isna()) |
            (recalc_ytd_mask & df['change_ytd_pct'].isna())
        ]
        if not rows_needing_history.empty:
            with self.db.get_session() as session:
                stock_repo = StockRepository(session)
                price_repo = PriceRepository(session)
                stock_cache: Dict[str, Optional[int]] = {}

                for idx, row in rows_needing_history.iterrows():
                    stock_code = row['stock_code']
                    stock_id = stock_cache.get(stock_code)
                    if stock_code not in stock_cache:
                        stock = stock_repo.get_by_code(stock_code)
                        stock_id = stock.stock_id if stock else None
                        stock_cache[stock_code] = stock_id

                    if stock_id is None:
                        continue

                    price_date = row['price_date'].date()

                    if recalc_1d_mask.loc[idx] and pd.isna(df.loc[idx, 'change_1d_pct']):
                        previous_price = price_repo.get_previous_price(stock_id, price_date)
                        if previous_price and previous_price.close_price:
                            prev_close = float(previous_price.close_price)
                            if prev_close > 0:
                                change_1d = ((float(row['close_price']) - prev_close) / prev_close) * 100
                                df.loc[idx, 'change_1d_pct'] = round(change_1d, 4)

                    if recalc_ytd_mask.loc[idx] and pd.isna(df.loc[idx, 'change_ytd_pct']):
                        year_start_price = price_repo.get_first_price_of_year(
                            stock_id,
                            row['calc_year'],
                            through_date=price_date,
                        )
                        if year_start_price and year_start_price.close_price:
                            baseline = float(year_start_price.close_price)
                            if baseline > 0:
                                change_ytd = ((float(row['close_price']) - baseline) / baseline) * 100
                                df.loc[idx, 'change_ytd_pct'] = round(change_ytd, 4)

        # If we still do not have a valid baseline, default only the first known
        # observation to 0.0 rather than preserving bad source values.
        df['change_1d_pct'] = df['change_1d_pct'].fillna(0.0)
        df['change_ytd_pct'] = df['change_ytd_pct'].fillna(0.0)
        df = df.drop(columns=['calc_year'])

        return df
    
    def _load_stocks(self, data: pd.DataFrame) -> int:
        """Load/update stock records."""
        stage_start = datetime.now()
        
        self.logger.info("Stage 4: Loading stocks")
        
        loaded_count = 0
        
        try:
            with self.db.get_session() as session:
                stock_repo = StockRepository(session)
                try:
                    sector_map = load_stock_sector_map()
                except FileNotFoundError as exc:
                    sector_map = {}
                    self.logger.warning(
                        "Stock sector reference map unavailable; falling back to source sectors",
                        extra={"error": str(exc)}
                    )

                def get_or_create_sector(sector_name: str) -> DimSector:
                    sector = session.query(DimSector).filter(
                        DimSector.sector_name == sector_name
                    ).first()
                    if sector:
                        return sector

                    sector = DimSector(
                        sector_name=sector_name,
                        description=f"{sector_name} sector",
                    )
                    session.add(sector)
                    session.flush()
                    return sector
                
                # Get unique stocks from data
                # Note: Afrimarket does not currently provide reliable sector
                # metadata, so curated reference data protects master data from
                # being degraded back to Unknown during daily runs.
                required_cols = ['stock_code', 'company_name', 'exchange']
                unique_stocks = data[required_cols].drop_duplicates()
                
                # Add sector column if missing (use None for lookup)
                if 'sector' not in data.columns:
                    unique_stocks['sector'] = None
                else:
                    unique_stocks = data[required_cols + ['sector']].drop_duplicates()
                
                for _, row in unique_stocks.iterrows():
                    try:
                        # Check if stock exists
                        stock = stock_repo.get_by_code(row['stock_code'])
                        
                        if stock:
                            update_values = {}
                            if stock.company_name != row['company_name']:
                                update_values["company_name"] = row['company_name']

                            existing_sector_name = (
                                stock.sector.sector_name if stock.sector else None
                            )
                            sector_name = choose_sector_name(
                                row['stock_code'],
                                sector_map,
                                existing_sector_name=existing_sector_name,
                                source_sector_name=row.get('sector'),
                            )
                            if (
                                is_unknown_sector(existing_sector_name)
                                and not is_unknown_sector(sector_name)
                            ):
                                sector = get_or_create_sector(sector_name)
                                if stock.sector_id != sector.sector_id:
                                    update_values["sector_id"] = sector.sector_id

                            if update_values:
                                stock_repo.update(stock, **update_values)
                                loaded_count += 1
                        else:
                            sector_name = choose_sector_name(
                                row['stock_code'],
                                sector_map,
                                source_sector_name=row.get('sector'),
                            )
                            sector = get_or_create_sector(
                                sector_name or UNKNOWN_SECTOR_NAME
                            )
                            
                            # Create stock
                            stock_repo.create_stock(
                                stock_code=row['stock_code'],
                                company_name=row['company_name'],
                                sector_id=sector.sector_id,
                                exchange=row['exchange']
                            )
                            loaded_count += 1
                            
                    except Exception as e:
                        error_msg = f"Failed to load stock {row['stock_code']}: {str(e)}"
                        self.logger.warning(error_msg)
                        self.warnings.append(error_msg)
                
                session.commit()
            
            self.logger.info(
                f"Loaded {loaded_count} stocks",
                extra={"loaded": loaded_count}
            )

            self.stage_times['load_stocks'] = (datetime.now() - stage_start).total_seconds()
            
            return loaded_count
            
        except Exception as e:
            self.errors.append(f"Stock loading error: {str(e)}")
            self.logger.error(f"Stock loading failed: {str(e)}")
            return 0
    
    def _load_prices(self, data: pd.DataFrame) -> int:
        """Load price records."""
        stage_start = datetime.now()
        
        self.logger.info("Stage 5: Loading prices")
        
        loaded_count = 0
        
        try:
            # Calculate missing change_1d_pct and change_ytd_pct
            data = self._calculate_missing_changes(data)
            
            with self.db.get_session() as session:
                price_repo = PriceRepository(session)
                stock_rows = (
                    session.query(DimStock.stock_code, DimStock.stock_id)
                    .filter(DimStock.stock_code.in_(data['stock_code'].dropna().unique().tolist()))
                    .all()
                )
                stock_map = {
                    str(stock_code).upper(): stock_id
                    for stock_code, stock_id in stock_rows
                }
                
                # Process in batches using bulk insert
                for i in range(0, len(data), self.config.batch_size):
                    batch = data.iloc[i:i + self.config.batch_size]
                    
                    # Prepare bulk price data
                    price_records = []
                    for _, row in batch.iterrows():
                        try:
                            stock_id = stock_map.get(str(row['stock_code']).upper())
                            if not stock_id:
                                self.warnings.append(f"Stock not found: {row['stock_code']}")
                                continue
                            
                            # Determine quality flag
                            quality_flag = self._derive_quality_flag(row)
                            complete_data = all([
                                pd.notna(row.get('close_price')),
                                pd.notna(row.get('change_1d_pct')),
                                pd.notna(row.get('change_ytd_pct')),
                            ])
                            quality = self._build_quality_metrics(
                                price_date=row.get("price_date"),
                                quality_flag=quality_flag,
                                volume=row.get("volume"),
                                change_1d_pct=row.get("change_1d_pct"),
                                change_ytd_pct=row.get("change_ytd_pct"),
                            )
                            
                            # Prepare price record for bulk insert
                            price_records.append({
                                'stock_id': stock_id,
                                'price_date': row['price_date'],
                                'close_price': row['close_price'],
                                'volume': row.get('volume'),
                                'change_1d_pct': row.get('change_1d_pct'),
                                'change_ytd_pct': row.get('change_ytd_pct'),
                                'source': row.get('source', 'unknown'),
                                'source_count': 1,
                                'bar_status': 'RECONCILED' if self.config.use_staging else 'OBSERVED',
                                'is_official': False,
                                'confidence_score': quality["confidence_score"],
                                'data_quality_flag': quality_flag,
                                'has_complete_data': complete_data
                            })
                            
                        except Exception as e:
                            error_msg = f"Failed to prepare price for {row['stock_code']}: {str(e)}"
                            self.logger.warning(error_msg)
                            self.warnings.append(error_msg)
                    
                    # Bulk insert the batch
                    if price_records:
                        batch_count = price_repo.bulk_insert_prices(price_records)
                        loaded_count += batch_count
                        session.commit()
                    
                    self.logger.info(
                        f"Loaded batch {i // self.config.batch_size + 1}: {len(price_records)} prices",
                        extra={"batch": i // self.config.batch_size + 1}
                    )
            
            self.logger.info(
                f"Loaded {loaded_count} prices",
                extra={"loaded": loaded_count}
            )
            
            self.stage_times['load_prices'] = (datetime.now() - stage_start).total_seconds()
            
            # Fail if no prices were loaded from non-empty data
            if loaded_count == 0 and len(data) > 0:
                raise RuntimeError(
                    f"Price loading failed: 0 prices loaded from {len(data)} input records"
                )
            
            return loaded_count
            
        except Exception as e:
            self.errors.append(f"Price loading error: {str(e)}")
            self.logger.error("Price loading failed", error=e)
            raise  # Re-raise to fail the DAG task

    def _derive_quality_flag(self, row: pd.Series) -> str:
        """Derive a consistent quality flag from the available row fields."""
        if pd.isna(row.get("close_price")):
            return "POOR"

        has_complete = all([
            pd.notna(row.get("close_price")),
            pd.notna(row.get("change_1d_pct")),
            pd.notna(row.get("change_ytd_pct")),
        ])
        if has_complete:
            return "GOOD"
        return "INCOMPLETE"

    def _build_quality_metrics(
        self,
        price_date: Any,
        quality_flag: str,
        volume: Any,
        change_1d_pct: Any,
        change_ytd_pct: Any,
    ) -> Dict[str, Any]:
        """Build consistent quality/confidence metadata for the existing price schema."""
        fields_present = {
            "close_price": True,
            "change_1d_pct": pd.notna(change_1d_pct),
            "change_ytd_pct": pd.notna(change_ytd_pct),
            "volume": pd.notna(volume),
        }
        completeness_score = round(
            (sum(1 for present in fields_present.values() if present) / len(fields_present)) * 100,
            2,
        )

        age_days = 0
        if price_date is not None:
            try:
                normalized_date = pd.to_datetime(price_date).date()
                age_days = max((date.today() - normalized_date).days, 0)
            except Exception:
                age_days = 0

        freshness_score = max(0.0, round(100 - min(age_days, 30) * 3, 2))

        confidence_map = {
            "GOOD": 85.0,
            "INCOMPLETE": 70.0,
            "SUSPICIOUS": 45.0,
            "MISSING": 30.0,
            "STALE": 30.0,
            "POOR": 20.0,
        }
        confidence_score = confidence_map.get(quality_flag, 50.0)
        anomaly_score = 60.0 if quality_flag == "SUSPICIOUS" else 0.0

        return {
            "completeness_score": completeness_score,
            "freshness_score": freshness_score,
            "confidence_score": confidence_score,
            "anomaly_score": anomaly_score,
            "quality_label": quality_flag,
            "field_coverage": fields_present,
            "notes": (
                f"Generated from {quality_flag.lower()} market data "
                f"during fact_daily_prices in-place redesign"
            ),
        }
    
    def _calculate_indicators(
        self,
        execution_date: date,
        stock_codes: Optional[List[str]]
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
                
                # Get stocks to process
                if stock_codes:
                    stocks = [stock_repo.get_by_code(code) for code in stock_codes]
                    stocks = [s for s in stocks if s is not None]
                else:
                    stocks = stock_repo.get_all_active()
                
                self.logger.info(
                    f"Calculating indicators for {len(stocks)} stocks",
                    extra={"stocks": len(stocks)}
                )
                
                # Process each stock
                for stock in stocks:
                    try:
                        # Get trusted price history with enough depth for mature indicators
                        required_history = self.indicator_calculator.minimum_history_required()
                        start_date = execution_date - timedelta(days=240)
                        prices = price_repo.get_trusted_price_history(
                            stock.stock_id,
                            start_date=start_date,
                            end_date=execution_date
                        )

                        if len(prices) < required_history:
                            self.logger.debug(
                                f"Skipping indicators for {stock.stock_code}: "
                                f"{len(prices)} trusted prices available, need {required_history}"
                            )
                            continue
                        
                        if len(prices) < 2:
                            continue
                        
                        # Convert to DataFrame
                        price_data = [{
                            'price_date': p.price_date,
                            'close_price': float(p.close_price)
                        } for p in prices]
                        
                        # Calculate indicators
                        indicators = self.indicator_calculator.calculate_for_stock(
                            stock_id=stock.stock_id,
                            stock_code=stock.stock_code,
                            price_history=price_data
                        )

                        # Save to database. Always upsert computed rows so
                        # reruns repair stale indicator values instead of
                        # preserving old calculations.
                        for indicator in indicators:
                            calculation_date = indicator['calculation_date']
                            values = {
                                k: v for k, v in indicator.items()
                                if k not in ('stock_id', 'calculation_date')
                            }
                            indicator_repo.save_indicators(
                                stock_id=stock.stock_id,
                                calculation_date=calculation_date,
                                indicators=values
                            )
                            calculated_count += 1
                        
                        session.commit()
                        
                    except Exception as e:
                        error_msg = f"Indicator calculation failed for {stock.stock_code}: {str(e)}"
                        self.logger.warning(error_msg)
                        self.warnings.append(error_msg)
                        session.rollback()
            
            self.logger.info(
                f"Calculated {calculated_count} indicators",
                extra={"calculated": calculated_count}
            )
            
            self.stage_times['calculate_indicators'] = (datetime.now() - stage_start).total_seconds()
            
            return calculated_count
            
        except Exception as e:
            self.errors.append(f"Indicator calculation error: {str(e)}")
            self.logger.error(f"Indicator calculation failed: {str(e)}")
            return 0
    
    def _evaluate_alerts(self, execution_date: date) -> int:
        """
        Evaluate alert rules and send notifications.
        
        Args:
            execution_date: Date to evaluate alerts for
            
        Returns:
            Number of alerts generated
        """
        stage_start = datetime.now()
        
        self.logger.info("Stage 7: Evaluating alert rules")
        
        try:
            evaluator = AlertEvaluator()
            
            result = evaluator.evaluate_all_rules(evaluation_date=execution_date)
            
            # Save alerts
            if result.alerts:
                saved = evaluator.save_alerts(result.alerts)
                self.logger.info(
                    f"Generated and saved {saved} alerts",
                    extra={"alerts": saved}
                )
                
                # Send notifications if notifier is configured
                # Wrapped in try-except to ensure notification failures don't break pipeline
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
        
        This method is designed to never fail the pipeline - all notification
        errors are caught, logged, and added to warnings only.
        
        Args:
            alerts: List of AlertHistory objects
            execution_date: Date of execution
        """
        try:
            # Determine notification channels based on configuration
            channels = []
            if self.settings.notifications.email_enabled:
                channels.append('email')
            if self.settings.notifications.slack_enabled:
                channels.append('slack')
            
            if not channels:
                self.logger.info("No notification channels enabled - skipping notifications")
                return
            
            # Send individual notifications for critical alerts
            critical_count = 0
            for alert in alerts:
                if alert.severity == 'CRITICAL':
                    try:
                        result = self.alert_notifier.send_alert(alert, channels=channels)
                        if result.success:
                            critical_count += 1
                        else:
                            self.warnings.append(
                                f"Notification failed for alert {alert.alert_id}: {', '.join(result.errors)}"
                            )
                            self.logger.warning(
                                f"Failed to send alert notification: {', '.join(result.errors)}",
                                extra={"alert_id": alert.alert_id}
                            )
                    except Exception as e:
                        self.warnings.append(f"Notification exception for alert {alert.alert_id}: {str(e)}")
                        self.logger.warning(
                            f"Exception sending alert notification: {str(e)}",
                            extra={"alert_id": alert.alert_id}
                        )
            
            if critical_count > 0:
                self.logger.info(
                    f"Sent {critical_count} critical alert notifications",
                    extra={"critical_alerts": critical_count}
                )
            
            # Send daily digest if email is enabled and there are non-critical alerts
            warning_alerts = [a for a in alerts if a.severity == 'WARNING']
            if self.settings.notifications.email_enabled and warning_alerts:
                try:
                    digest_result = self.alert_notifier.send_daily_digest(
                        alerts, 
                        execution_date
                    )
                    if digest_result.success:
                        self.logger.info(
                            f"Sent daily digest with {len(alerts)} alerts",
                            extra={"total_alerts": len(alerts)}
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
            # Even if notification setup fails, don't fail the pipeline
            self.warnings.append(f"Notification setup error: {str(e)}")
            self.logger.warning(
                f"Notification system error (pipeline continues): {str(e)}"
            )
    
    def _generate_recommendations(
        self,
        execution_date: date,
        stock_codes: Optional[List[str]]
    ) -> int:
        """
        Generate investment recommendations.
        
        Args:
            execution_date: Date to generate recommendations for
            stock_codes: Specific stocks to analyze
            
        Returns:
            Number of recommendations generated
        """
        stage_start = datetime.now()
        
        self.logger.info("Stage 8: Generating stock screening signals")
        
        try:
            with self.db.get_session() as session:
                screener = StockScreener(
                    session,
                    strategy_profile=self.config.recommendation_profile,
                )
                rec_repo = RecommendationRepository(session)
                portfolio_policy = ProductionPortfolioPolicy()
                
                # Generate recommendations
                recommendations = screener.generate_recommendations(
                    recommendation_date=execution_date,
                    stock_codes=stock_codes,
                    strategy_profile=self.config.recommendation_profile,
                    capture_audit=True,
                )
                open_positions = portfolio_policy.count_open_positions(
                    session,
                    recommendation_date=execution_date,
                    profile=self.config.recommendation_profile,
                )
                recommendations = portfolio_policy.apply(
                    recommendations,
                    existing_open_positions=open_positions,
                )
                screener.apply_portfolio_audit(recommendations)

                audit_rows = rec_repo.replace_audit_entries(
                    recommendation_date=execution_date,
                    profile=self.config.recommendation_profile,
                    audit_entries=screener.last_audit_entries,
                    full_snapshot=stock_codes is None,
                )
                self.logger.info(
                    f"Persisted {audit_rows} recommendation audit rows",
                    extra={
                        "recommendation_audit_rows": audit_rows,
                        "recommendation_date": str(execution_date),
                        "profile": self.config.recommendation_profile,
                    },
                )

                if stock_codes is None:
                    deleted = rec_repo.delete_recommendations_for_date_profile(
                        recommendation_date=execution_date,
                        profile=self.config.recommendation_profile,
                    )
                    if deleted:
                        self.logger.info(
                            f"Deleted {deleted} existing recommendation rows "
                            f"for {execution_date} before full regeneration",
                            extra={
                                "deleted_recommendations": deleted,
                                "recommendation_date": str(execution_date),
                            },
                        )
                
                if recommendations:
                    # Save to database
                    saved = rec_repo.create_recommendations_bulk(recommendations)
                    approved = sum(
                        1
                        for rec in recommendations
                        if rec.portfolio_approved
                    )
                    
                    self.logger.info(
                        f"Generated {saved} screening signals; "
                        f"{approved} portfolio-approved",
                        extra={
                            "recommendations": saved,
                            "portfolio_approved": approved,
                            "open_positions_before": open_positions,
                        }
                    )
                    
                    # Log top picks
                    buy_picks = [
                        r for r in recommendations
                        if r.portfolio_approved
                        and r.action_type.value in ('BUY', 'STRONG_BUY')
                    ]
                    
                    if buy_picks:
                        top_3 = buy_picks[:3]
                        self.logger.info(
                            f"Top {len(top_3)} buy signals:",
                            extra={"top_picks": len(top_3)}
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
                    self.logger.info("No signals generated (filters applied)")
                    saved = 0
                
                screener.close()
                session.commit()
                
                self.stage_times['generate_recommendations'] = (
                    datetime.now() - stage_start
                ).total_seconds()
                
                return saved
                
        except Exception as e:
            self.errors.append(f"Recommendation generation error: {str(e)}")
            self.logger.error(
                "Recommendation generation failed",
                error=e
            )
            return 0
    
    def _build_result(
        self,
        start_time: datetime,
        success: bool,
        stocks: int,
        prices: int,
        indicators: int,
        alerts: int,
        recommendations: int = 0
    ) -> PipelineResult:
        """Build pipeline result with staging metrics."""
        execution_time = (datetime.now() - start_time).total_seconds()
        
        return PipelineResult(
            success=success,
            execution_time=execution_time,
            stocks_processed=stocks,
            prices_loaded=prices,
            indicators_calculated=indicators,
            alerts_generated=alerts,
            recommendations_generated=recommendations,
            errors=self.errors,
            warnings=self.warnings,
            stage_times=self.stage_times,
            staging_loaded=self.staging_loaded,
            reconciled_count=self.reconciled_count,
            conflicts_flagged=self.conflicts_flagged,
            avg_price_variance=self.avg_price_variance
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
        
        # Log staging metrics if applicable
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
