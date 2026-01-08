"""
Test pipeline orchestrator.

Tests ETL workflow coordination and stage execution.
"""

from datetime import date, timedelta
import pandas as pd

from app.pipelines import PipelineOrchestrator
from app.pipelines.orchestrator import PipelineConfig
from app.config.database import get_session
from app.repositories import StockRepository, PriceRepository, IndicatorRepository, AlertRepository
from app.utils import get_logger


logger = get_logger("test_orchestrator")


def test_pipeline_config():
    """Test pipeline configuration."""
    logger.info("=" * 60)
    logger.info("Testing Pipeline Configuration")
    logger.info("=" * 60)
    
    # Default config
    config = PipelineConfig()
    logger.info("Default configuration:")
    logger.info(f"  fetch_ngx: {config.fetch_ngx}")
    logger.info(f"  validate_data: {config.validate_data}")
    logger.info(f"  load_stocks: {config.load_stocks}")
    logger.info(f"  load_prices: {config.load_prices}")
    logger.info(f"  calculate_indicators: {config.calculate_indicators}")
    logger.info(f"  evaluate_alerts: {config.evaluate_alerts}")
    logger.info(f"  batch_size: {config.batch_size}")
    logger.info(f"  max_errors: {config.max_errors}")
    logger.info(f"  lookback_days: {config.lookback_days}")
    
    # Custom config (disable fetching for testing)
    custom_config = PipelineConfig(
        fetch_ngx=False,
        batch_size=10,
        lookback_days=7
    )
    logger.info("\nCustom configuration:")
    logger.info(f"  fetch_ngx: {custom_config.fetch_ngx}")
    logger.info(f"  batch_size: {custom_config.batch_size}")
    logger.info(f"  lookback_days: {custom_config.lookback_days}")
    
    logger.info("✓ Configuration test completed")


def test_orchestrator_initialization():
    """Test orchestrator initialization."""
    logger.info("\n" + "=" * 60)
    logger.info("Testing Orchestrator Initialization")
    logger.info("=" * 60)
    
    # Test with default config
    orchestrator = PipelineOrchestrator()
    logger.info("Orchestrator initialized with default config")
    logger.info(f"  Config: {orchestrator.config}")
    logger.info(f"  Database: {orchestrator.db is not None}")
    logger.info(f"  Transformer: {orchestrator.transformer is not None}")
    logger.info(f"  Indicator calculator: {orchestrator.indicator_calculator is not None}")
    
    # Test with custom config
    custom_config = PipelineConfig(
        fetch_ngx=False,
        calculate_indicators=False
    )
    orchestrator2 = PipelineOrchestrator(config=custom_config)
    logger.info("\nOrchestrator initialized with custom config")
    logger.info(f"  NGX source: {orchestrator2.ngx_source}")
    
    logger.info("✓ Initialization test completed")


def test_pipeline_with_existing_data():
    """Test pipeline execution with existing database data."""
    logger.info("\n" + "=" * 60)
    logger.info("Testing Pipeline with Existing Data")
    logger.info("=" * 60)
    
    # Create config that only calculates indicators and alerts
    # (skip fetching since we have data in DB)
    config = PipelineConfig(
        fetch_ngx=False,
        load_stocks=False,
        load_prices=False,
        calculate_indicators=True,
        evaluate_alerts=True
    )
    
    orchestrator = PipelineOrchestrator(config=config)
    
    # Check what stocks we have
    session = get_session()
    stock_repo = StockRepository(session)
    stocks = stock_repo.get_all_active()
    
    if not stocks:
        logger.info("No stocks in database - skipping test")
        session.close()
        return
    
    logger.info(f"Found {len(stocks)} active stocks in database")
    logger.info(f"Sample stocks: {[s.stock_code for s in stocks[:3]]}")
    
    # Get a stock with price data
    price_repo = PriceRepository(session)
    test_stock = None
    for stock in stocks:
        prices = price_repo.get_price_history(
            stock.stock_id,
            start_date=date.today() - timedelta(days=30),
            end_date=date.today()
        )
        if len(prices) > 0:
            test_stock = stock
            logger.info(f"Using stock {stock.stock_code} with {len(prices)} price records")
            break
    
    session.close()
    
    if not test_stock:
        logger.info("No stocks with price data - skipping test")
        return
    
    # Run pipeline for this specific stock
    logger.info(f"\nRunning pipeline for stock: {test_stock.stock_code}")
    result = orchestrator.run(
        execution_date=date.today(),
        stock_codes=[test_stock.stock_code]
    )
    
    logger.info("\nPipeline execution completed")
    logger.info(f"  Success: {result.success}")
    logger.info(f"  Execution time: {result.execution_time:.2f}s")
    logger.info(f"  Indicators calculated: {result.indicators_calculated}")
    logger.info(f"  Alerts generated: {result.alerts_generated}")
    logger.info(f"  Errors: {len(result.errors)}")
    logger.info(f"  Warnings: {len(result.warnings)}")
    
    if result.errors:
        logger.info("\nErrors:")
        for error in result.errors:
            logger.info(f"  - {error}")
    
    if result.warnings:
        logger.info("\nWarnings (first 5):")
        for warning in result.warnings[:5]:
            logger.info(f"  - {warning}")
    
    if result.stage_times:
        logger.info("\nStage execution times:")
        for stage, time in result.stage_times.items():
            logger.info(f"  {stage}: {time:.2f}s")
    
    logger.info("✓ Pipeline execution test completed")


def test_pipeline_result_structure():
    """Test pipeline result structure."""
    logger.info("\n" + "=" * 60)
    logger.info("Testing Pipeline Result Structure")
    logger.info("=" * 60)
    
    # Run minimal pipeline
    config = PipelineConfig(
        fetch_ngx=False,
        load_stocks=False,
        load_prices=False,
        calculate_indicators=False,
        evaluate_alerts=False
    )
    
    orchestrator = PipelineOrchestrator(config=config)
    result = orchestrator.run(execution_date=date.today())
    
    logger.info("Pipeline result structure:")
    logger.info(f"  success: {result.success} (type: {type(result.success).__name__})")
    logger.info(f"  execution_time: {result.execution_time} (type: {type(result.execution_time).__name__})")
    logger.info(f"  stocks_processed: {result.stocks_processed}")
    logger.info(f"  prices_loaded: {result.prices_loaded}")
    logger.info(f"  indicators_calculated: {result.indicators_calculated}")
    logger.info(f"  alerts_generated: {result.alerts_generated}")
    logger.info(f"  errors: {result.errors} (length: {len(result.errors)})")
    logger.info(f"  warnings: {result.warnings} (length: {len(result.warnings)})")
    logger.info(f"  stage_times: {result.stage_times} (keys: {list(result.stage_times.keys())})")
    
    # Verify result attributes
    assert isinstance(result.success, bool), "success should be bool"
    assert isinstance(result.execution_time, float), "execution_time should be float"
    assert isinstance(result.stocks_processed, int), "stocks_processed should be int"
    assert isinstance(result.prices_loaded, int), "prices_loaded should be int"
    assert isinstance(result.indicators_calculated, int), "indicators_calculated should be int"
    assert isinstance(result.alerts_generated, int), "alerts_generated should be int"
    assert isinstance(result.errors, list), "errors should be list"
    assert isinstance(result.warnings, list), "warnings should be list"
    assert isinstance(result.stage_times, dict), "stage_times should be dict"
    
    logger.info("✓ Result structure test completed")


def test_database_state():
    """Check current database state."""
    logger.info("\n" + "=" * 60)
    logger.info("Testing Database State")
    logger.info("=" * 60)
    
    session = get_session()
    
    try:
        # Check stocks
        stock_repo = StockRepository(session)
        stocks = stock_repo.get_all_active()
        logger.info(f"Active stocks: {len(stocks)}")
        if stocks:
            logger.info(f"  Sample: {stocks[0].stock_code} - {stocks[0].company_name}")
        
        # Check prices
        price_repo = PriceRepository(session)
        if stocks:
            recent_prices = price_repo.get_price_history(
                stocks[0].stock_id,
                start_date=date.today() - timedelta(days=7),
                end_date=date.today()
            )
            logger.info(f"Recent prices for {stocks[0].stock_code}: {len(recent_prices)}")
        
        # Check indicators
        indicator_repo = IndicatorRepository(session)
        if stocks:
            recent_indicators = indicator_repo.get_indicator_history(
                stocks[0].stock_id,
                start_date=date.today() - timedelta(days=7),
                end_date=date.today()
            )
            logger.info(f"Recent indicators for {stocks[0].stock_code}: {len(recent_indicators)}")
        
        # Check alerts
        alert_repo = AlertRepository(session)
        active_alerts = alert_repo.get_active_alerts()
        logger.info(f"Active alerts: {len(active_alerts)}")
        
        active_rules = alert_repo.get_all_rules(active_only=True)
        logger.info(f"Active alert rules: {len(active_rules)}")
        if active_rules:
            logger.info(f"  Sample rule: {active_rules[0].rule_name} ({active_rules[0].rule_type})")
        
        logger.info("✓ Database state check completed")
        
    finally:
        session.close()


if __name__ == "__main__":
    try:
        test_pipeline_config()
        test_orchestrator_initialization()
        test_database_state()
        test_pipeline_result_structure()
        test_pipeline_with_existing_data()
        
        logger.info("\n" + "=" * 60)
        logger.info("✓ All orchestrator tests completed!")
        logger.info("=" * 60)
        
    except Exception as e:
        logger.error(f"Test failed: {e}")
        import traceback
        traceback.print_exc()
        raise
