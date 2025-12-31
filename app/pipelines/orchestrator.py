"""
Pipeline orchestrator.

Coordinates the complete ETL workflow:
1. Fetch data from sources (NGX, Yahoo Finance)
2. Validate data quality
3. Transform and standardize data
4. Load into database (stocks, prices)
5. Calculate technical indicators
6. Evaluate alert rules
7. Generate pipeline summary report
"""

from typing import Dict, List, Optional, Tuple, Any
from datetime import date, datetime, timedelta
from dataclasses import dataclass
import pandas as pd

from app.config.database import get_db
from app.config.settings import Settings
from app.repositories import (
    StockRepository, PriceRepository,
    IndicatorRepository, AlertRepository, RecommendationRepository
)
from app.models import DimSector
from app.services.data_sources import NGXDataSource
from app.services.processors import DataValidator, DataTransformer
from app.services.indicators import IndicatorCalculator
from app.services.alerts import AlertEvaluator, AlertNotifier
from app.services.advisory import InvestmentAdvisor
from app.utils import get_logger
from app.utils.exceptions import DataValidationError


@dataclass
class PipelineConfig:
    """
    Pipeline execution configuration.
    
    Attributes:
        fetch_ngx: Whether to fetch NGX data
        fetch_yahoo: Whether to fetch Yahoo Finance data
        validate_data: Whether to run validation
        load_stocks: Whether to load/update stocks
        load_prices: Whether to load prices
        calculate_indicators: Whether to calculate indicators
        evaluate_alerts: Whether to evaluate alert rules
        generate_recommendations: Whether to generate investment recommendations
        batch_size: Batch size for processing
        max_errors: Maximum errors before aborting
        lookback_days: Days of historical data to fetch
    """
    fetch_ngx: bool = True
    fetch_yahoo: bool = True
    validate_data: bool = True
    load_stocks: bool = True
    load_prices: bool = True
    calculate_indicators: bool = True
    evaluate_alerts: bool = True
    generate_recommendations: bool = True
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
        prices_loaded: Number of prices loaded
        indicators_calculated: Number of indicators calculated
        alerts_generated: Number of alerts generated
        errors: List of error messages
        warnings: List of warning messages
        stage_times: Dict mapping stage name to execution time
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
        
        # Initialize data sources
        self.ngx_source = NGXDataSource() if self.config.fetch_ngx else None
        
        # Initialize processors
        self.validator = None  # Will initialize with sectors
        self.transformer = DataTransformer()
        self.indicator_calculator = IndicatorCalculator()
        
        # Initialize alert notifier
        self.alert_notifier = AlertNotifier() if (
            self.settings.notifications.email_enabled or 
            self.settings.notifications.slack_enabled
        ) else None
        
        # Track execution metrics
        self.errors: List[str] = []
        self.warnings: List[str] = []
        self.stage_times: Dict[str, float] = {}
        
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
        
        stocks_processed = 0
        prices_loaded = 0
        indicators_calculated = 0
        alerts_generated = 0
        
        try:
            # Stage 1: Fetch data
            if self.config.fetch_ngx or self.config.fetch_yahoo:
                raw_data = self._fetch_data(execution_date, stock_codes)
                self.logger.info(f"DEBUG: Fetched {len(raw_data)} rows from sources")
                if raw_data.empty:
                    self.logger.warning("No data fetched from sources")
                    self.warnings.append("No data fetched from any source")
                    return self._build_result(
                        start_time, False, 0, 0, 0, 0
                    )
            else:
                self.logger.info("Skipping data fetch (disabled in config)")
                raw_data = pd.DataFrame()
            
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
            
            # Stage 5: Load prices
            if self.config.load_prices and not transformed_data.empty:
                prices_loaded = self._load_prices(transformed_data)
            
            # Stage 6: Calculate indicators
            if self.config.calculate_indicators:
                indicators_calculated = self._calculate_indicators(
                    execution_date,
                    stock_codes
                )
            
            # Stage 7: Evaluate alerts
            if self.config.evaluate_alerts:
                alerts_generated = self._evaluate_alerts(execution_date)
            
            # Stage 8: Generate recommendations
            recommendations_generated = 0
            if self.config.generate_recommendations:
                recommendations_generated = self._generate_recommendations(
                    execution_date,
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
            
            self._log_summary(result)
            
            return result
            
        except Exception as e:
            self.logger.error(
                f"Pipeline execution failed: {str(e)}",
                extra={"error": str(e)},
                exc_info=True
            )
            self.errors.append(f"Pipeline failure: {str(e)}")
            
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
        
        self.logger.info("Stage 1: Fetching data from sources")
        
        all_data = []
        
        # Fetch from NGX
        if self.ngx_source:
            try:
                self.logger.info("Fetching data from NGX")
                ngx_data = self.ngx_source.fetch(execution_date)
                
                if not ngx_data.empty:
                    all_data.append(ngx_data)
                    self.logger.info(
                        f"Fetched {len(ngx_data)} records from NGX",
                        extra={"records": len(ngx_data), "source": "NGX"}
                    )
                else:
                    self.warnings.append("No data from NGX")
                    
            except Exception as e:
                error_msg = f"NGX fetch failed: {str(e)}"
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
            
            # Add errors for invalid records (but skip duplicate errors as they're handled by deduplication)
            if result.errors:
                for error in result.errors[:10]:  # First 10
                    error_msg = error.get('error', 'Unknown')
                    # Duplicates are warnings, not errors - they get deduplicated in transform stage
                    if 'Duplicate' in error_msg:
                        self.warnings.append(f"Validation: {error_msg}")
                    else:
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
            self.logger.error(f"Transformation failed: {str(e)}")
            return pd.DataFrame()
    
    def _load_stocks(self, data: pd.DataFrame) -> int:
        """Load/update stock records."""
        stage_start = datetime.now()
        
        self.logger.info("Stage 4: Loading stocks")
        
        loaded_count = 0
        
        try:
            with self.db.get_session() as session:
                stock_repo = StockRepository(session)
                
                # Get unique stocks from data
                unique_stocks = data[['stock_code', 'company_name', 'sector', 'exchange']].drop_duplicates()
                
                for _, row in unique_stocks.iterrows():
                    try:
                        # Check if stock exists
                        stock = stock_repo.get_by_code(row['stock_code'])
                        
                        if stock:
                            # Update if needed
                            if stock.company_name != row['company_name']:
                                stock_repo.update_stock(
                                    stock.stock_id,
                                    company_name=row['company_name']
                                )
                                loaded_count += 1
                        else:
                            # Get or create sector
                            sector = session.query(DimSector).filter(
                                DimSector.sector_name == row['sector']
                            ).first()
                            
                            if not sector:
                                sector = DimSector(
                                    sector_name=row['sector'],
                                    description=f"{row['sector']} sector"
                                )
                                session.add(sector)
                                session.flush()  # Get sector_id
                            
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
            with self.db.get_session() as session:
                stock_repo = StockRepository(session)
                price_repo = PriceRepository(session)
                
                # Process in batches using bulk insert
                for i in range(0, len(data), self.config.batch_size):
                    batch = data.iloc[i:i + self.config.batch_size]
                    
                    # Prepare bulk price data
                    price_records = []
                    for _, row in batch.iterrows():
                        try:
                            # Get stock
                            stock = stock_repo.get_by_code(row['stock_code'])
                            if not stock:
                                self.warnings.append(f"Stock not found: {row['stock_code']}")
                                continue
                            
                            # Calculate data quality based on available NGX fields
                            has_complete = all([
                                pd.notna(row.get('close_price')),
                                pd.notna(row.get('change_1d_pct')),
                                pd.notna(row.get('change_ytd_pct')),
                                pd.notna(row.get('market_cap'))
                            ])
                            
                            # Determine quality flag
                            if has_complete:
                                quality_flag = 'GOOD'
                                complete_data = True
                            elif pd.notna(row['close_price']):
                                quality_flag = 'INCOMPLETE'
                                complete_data = False
                            else:
                                quality_flag = 'POOR'
                                complete_data = False
                            
                            # Prepare price record for bulk insert
                            price_records.append({
                                'stock_id': stock.stock_id,
                                'price_date': row['price_date'],
                                'close_price': row['close_price'],
                                'change_1d_pct': row.get('change_1d_pct'),
                                'change_ytd_pct': row.get('change_ytd_pct'),
                                'market_cap': row.get('market_cap'),
                                'source': row.get('source', 'unknown'),
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
            
            return loaded_count
            
        except Exception as e:
            self.errors.append(f"Price loading error: {str(e)}")
            self.logger.error(f"Price loading failed: {str(e)}")
            return 0
    
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
                        # Get price history (last 100 days for calculations)
                        start_date = execution_date - timedelta(days=100)
                        prices = price_repo.get_price_history(
                            stock.stock_id,
                            start_date=start_date,
                            end_date=execution_date
                        )
                        
                        if len(prices) < 2:
                            continue
                        
                        # Convert to DataFrame
                        price_data = [{
                            'price_date': p.price_date,
                            'close_price': float(p.close_price),
                            'high_price': float(p.high_price) if p.high_price else None,
                            'low_price': float(p.low_price) if p.low_price else None,
                            'volume': int(p.volume) if p.volume else None
                        } for p in prices]
                        
                        # Calculate indicators
                        indicators = self.indicator_calculator.calculate_for_stock(
                            stock_id=stock.stock_id,
                            stock_code=stock.stock_code,
                            price_history=price_data
                        )
                        
                        # Save to database
                        for indicator in indicators:
                            indicator_repo.create_indicator(**indicator)
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
        
        self.logger.info("Stage 8: Generating investment recommendations")
        
        try:
            with self.db.get_session() as session:
                advisor = InvestmentAdvisor(session)
                rec_repo = RecommendationRepository(session)
                
                # Generate recommendations
                recommendations = advisor.generate_recommendations(
                    recommendation_date=execution_date,
                    stock_codes=stock_codes,
                    min_score=40.0,  # Minimum acceptable score
                    min_confidence=0.5  # Minimum confidence
                )
                
                if recommendations:
                    # Save to database
                    saved = rec_repo.create_recommendations_bulk(recommendations)
                    
                    self.logger.info(
                        f"Generated {saved} investment recommendations",
                        extra={"recommendations": saved}
                    )
                    
                    # Log top picks
                    buy_picks = [r for r in recommendations 
                                if r.signal_type.value in ('BUY', 'STRONG_BUY')]
                    
                    if buy_picks:
                        top_3 = buy_picks[:3]
                        self.logger.info(
                            f"Top {len(top_3)} buy recommendations:",
                            extra={"top_picks": len(top_3)}
                        )
                        for i, rec in enumerate(top_3, 1):
                            self.logger.info(
                                f"  {i}. {rec.stock_code} - "
                                f"Score: {rec.score:.1f}, "
                                f"Signal: {rec.signal_type.value}, "
                                f"Confidence: {rec.confidence*100:.0f}%"
                            )
                else:
                    self.logger.info("No recommendations generated (filters applied)")
                    saved = 0
                
                advisor.close()
                session.commit()
                
                self.stage_times['generate_recommendations'] = (
                    datetime.now() - stage_start
                ).total_seconds()
                
                return saved
                
        except Exception as e:
            self.errors.append(f"Recommendation generation error: {str(e)}")
            self.logger.error(
                f"Recommendation generation failed: {str(e)}", 
                exc_info=True
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
        """Build pipeline result."""
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
            stage_times=self.stage_times
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
