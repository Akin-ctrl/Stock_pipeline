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
from app.repositories import (
    StockRepository, PriceRepository,
    IndicatorRepository, AlertRepository
)
from app.models import DimSector
from app.services.data_sources import NGXDataSource, YahooDataSource
from app.services.processors import DataValidator, DataTransformer
from app.services.indicators import IndicatorCalculator
from app.services.alerts import AlertEvaluator
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
        
        # Initialize database
        self.db = get_db()
        
        # Initialize data sources
        self.ngx_source = NGXDataSource() if self.config.fetch_ngx else None
        self.yahoo_source = YahooDataSource() if self.config.fetch_yahoo else None
        
        # Initialize processors
        self.validator = None  # Will initialize with sectors
        self.transformer = DataTransformer()
        self.indicator_calculator = IndicatorCalculator()
        
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
                if raw_data.empty:
                    self.logger.warning("No data fetched from sources")
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
            
            # Pipeline successful
            success = len(self.errors) == 0
            
            result = self._build_result(
                start_time,
                success,
                stocks_processed,
                prices_loaded,
                indicators_calculated,
                alerts_generated
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
                ngx_data = self.ngx_source.fetch_daily_prices(execution_date)
                
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
                self.logger.error(error_msg, exc_info=True)
                self.errors.append(error_msg)
        
        # Fetch from Yahoo Finance
        if self.yahoo_source:
            try:
                self.logger.info("Fetching data from Yahoo Finance")
                
                # Get stock codes to fetch
                with self.db.get_session() as session:
                    stock_repo = StockRepository(session)
                    
                    if stock_codes:
                        # Use provided list
                        codes = stock_codes
                    else:
                        # Get all active stocks
                        stocks = stock_repo.get_all_active()
                        codes = [s.stock_code for s in stocks]
                
                # Fetch for date range
                start_date = execution_date - timedelta(days=self.config.lookback_days)
                yahoo_data = self.yahoo_source.fetch_multiple_stocks(
                    codes,
                    start_date=start_date,
                    end_date=execution_date
                )
                
                if not yahoo_data.empty:
                    all_data.append(yahoo_data)
                    self.logger.info(
                        f"Fetched {len(yahoo_data)} records from Yahoo",
                        extra={"records": len(yahoo_data), "source": "Yahoo"}
                    )
                else:
                    self.warnings.append("No data from Yahoo Finance")
                    
            except Exception as e:
                error_msg = f"Yahoo fetch failed: {str(e)}"
                self.logger.error(error_msg, exc_info=True)
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
            
            # Add errors for invalid records
            if result.errors:
                for error in result.errors[:10]:  # First 10
                    self.errors.append(f"Validation: {error.get('error', 'Unknown')}")
            
            if not result.is_valid:
                self.logger.warning(
                    f"Validation found {result.invalid_count} invalid records",
                    extra={"invalid_count": result.invalid_count}
                )
            
            self.stage_times['validate_data'] = (datetime.now() - stage_start).total_seconds()
            
            return cleaned_data
            
        except DataValidationError as e:
            self.errors.append(f"Validation error: {str(e)}")
            self.logger.error(f"Validation failed: {str(e)}", exc_info=True)
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
            self.logger.error(f"Transformation failed: {str(e)}", exc_info=True)
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
            self.logger.error(f"Stock loading failed: {str(e)}", exc_info=True)
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
                
                # Process in batches
                for i in range(0, len(data), self.config.batch_size):
                    batch = data.iloc[i:i + self.config.batch_size]
                    
                    for _, row in batch.iterrows():
                        try:
                            # Get stock
                            stock = stock_repo.get_by_code(row['stock_code'])
                            if not stock:
                                self.warnings.append(f"Stock not found: {row['stock_code']}")
                                continue
                            
                            # Create price record
                            price_repo.create_price(
                                stock_id=stock.stock_id,
                                price_date=row['price_date'],
                                open_price=row.get('open_price'),
                                high_price=row.get('high_price'),
                                low_price=row.get('low_price'),
                                close_price=row['close_price'],
                                volume=row.get('volume'),
                                change_1d_pct=row.get('change_1d_pct'),
                                source=row['source'],
                                data_quality_flag=row.get('data_quality_flag', 'GOOD'),
                                has_complete_data=row.get('has_complete_data', False)
                            )
                            loaded_count += 1
                            
                        except Exception as e:
                            error_msg = f"Failed to load price for {row['stock_code']}: {str(e)}"
                            self.logger.warning(error_msg)
                            self.warnings.append(error_msg)
                    
                    # Commit batch
                    session.commit()
                    
                    self.logger.info(
                        f"Loaded batch {i // self.config.batch_size + 1}: {len(batch)} prices",
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
            self.logger.error(f"Price loading failed: {str(e)}", exc_info=True)
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
            self.logger.error(f"Indicator calculation failed: {str(e)}", exc_info=True)
            return 0
    
    def _evaluate_alerts(self, execution_date: date) -> int:
        """Evaluate alert rules."""
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
            else:
                self.logger.info("No alerts triggered")
            
            evaluator.close()
            
            self.stage_times['evaluate_alerts'] = (datetime.now() - stage_start).total_seconds()
            
            return result.alerts_generated
            
        except Exception as e:
            self.errors.append(f"Alert evaluation error: {str(e)}")
            self.logger.error(f"Alert evaluation failed: {str(e)}", exc_info=True)
            return 0
    
    def _build_result(
        self,
        start_time: datetime,
        success: bool,
        stocks: int,
        prices: int,
        indicators: int,
        alerts: int
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
