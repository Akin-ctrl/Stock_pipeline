"""
Integration tests for pipeline orchestrator.

Tests end-to-end workflow from data fetch to alert generation.
"""

import pytest
from datetime import date, timedelta
from unittest.mock import Mock, patch
import pandas as pd
from decimal import Decimal

from app.pipelines.orchestrator import (
    PipelineOrchestrator,
    PipelineConfig,
    PipelineResult
)
from app.repositories import StockRepository, PriceRepository, IndicatorRepository, AlertRepository
from app.models import DimSector, AlertRule


@pytest.mark.integration
@pytest.mark.database
class TestPipelineOrchestrator:
    """Test complete pipeline orchestration."""
    
    def test_pipeline_initialization(self):
        """Test pipeline initializes with default config."""
        orchestrator = PipelineOrchestrator()
        
        assert orchestrator.config is not None
        assert orchestrator.config.fetch_yahoo is True
        assert orchestrator.config.load_stocks is True
        assert orchestrator.config.calculate_indicators is True
    
    def test_pipeline_with_custom_config(self):
        """Test pipeline with custom configuration."""
        config = PipelineConfig(
            fetch_ngx=False,
            fetch_yahoo=True,
            calculate_indicators=False,
            evaluate_alerts=False
        )
        
        orchestrator = PipelineOrchestrator(config)
        
        assert orchestrator.config.fetch_ngx is False
        assert orchestrator.config.calculate_indicators is False
    
    @pytest.mark.external
    def test_pipeline_run_with_real_data(self, db_session):
        """Test pipeline execution with real data fetch."""
        config = PipelineConfig(
            fetch_ngx=False,  # Skip NGX to avoid external dependency
            fetch_yahoo=True,
            lookback_days=7
        )
        
        orchestrator = PipelineOrchestrator(config)
        
        # Run pipeline
        result = orchestrator.run(
            execution_date=date.today() - timedelta(days=1)
        )
        
        # Should complete (may have warnings but should not error)
        assert isinstance(result, PipelineResult)
        assert result.execution_time > 0
        
        # Check that some data was processed
        if result.success:
            assert result.stocks_processed >= 0
            assert result.prices_loaded >= 0
    
    def test_pipeline_with_mocked_data(self, db_session, sample_sectors):
        """Test pipeline with mocked data sources."""
        # Create mock data
        mock_yahoo_data = pd.DataFrame({
            'stock_code': ['GTCO.L', 'SEPL.L'],
            'company_name': ['Guaranty Trust', 'Seplat Energy'],
            'sector': ['Financials', 'Oil & Gas'],
            'exchange': ['LSE', 'LSE'],
            'price_date': [date.today(), date.today()],
            'open_price': [100.0, 200.0],
            'high_price': [105.0, 210.0],
            'low_price': [99.0, 198.0],
            'close_price': [103.0, 205.0],
            'volume': [1000000, 500000],
            'change_1d_pct': [3.0, 2.5]
        })
        
        config = PipelineConfig(
            fetch_ngx=False,
            fetch_yahoo=True
        )
        
        orchestrator = PipelineOrchestrator(config)
        
        # Mock the Yahoo data source
        with patch.object(orchestrator.yahoo_source, 'fetch', return_value=mock_yahoo_data):
            result = orchestrator.run(execution_date=date.today())
        
        assert result.success is True
        assert result.stocks_processed >= 2
        assert result.prices_loaded >= 2
    
    def test_pipeline_handles_empty_data(self, db_session):
        """Test pipeline handles empty data gracefully."""
        config = PipelineConfig(
            fetch_ngx=False,
            fetch_yahoo=True
        )
        
        orchestrator = PipelineOrchestrator(config)
        
        # Mock empty data
        with patch.object(orchestrator.yahoo_source, 'fetch', return_value=pd.DataFrame()):
            result = orchestrator.run(execution_date=date.today())
        
        # Should complete without errors even with no data
        assert isinstance(result, PipelineResult)
        assert result.stocks_processed == 0
        assert result.prices_loaded == 0
    
    def test_pipeline_validation_stage(self, db_session):
        """Test data validation stage."""
        # Invalid data (missing required columns)
        invalid_data = pd.DataFrame({
            'stock_code': ['GTCO'],
            'price_date': [date.today()]
            # Missing close_price
        })
        
        config = PipelineConfig(
            fetch_yahoo=True,
            validate_data=True
        )
        
        orchestrator = PipelineOrchestrator(config)
        
        with patch.object(orchestrator.yahoo_source, 'fetch', return_value=invalid_data):
            result = orchestrator.run(execution_date=date.today())
        
        # Should fail validation
        assert result.success is False or len(result.errors) > 0
    
    def test_pipeline_transformation_stage(self, db_session):
        """Test data transformation stage."""
        # Data with formatting issues
        messy_data = pd.DataFrame({
            'stock_code': ['  gtco.l  ', 'SEPL.L'],
            'company_name': ['  Guaranty   Trust  ', 'SEPLAT ENERGY'],
            'sector': ['Financials', 'Oil & Gas'],
            'exchange': ['LSE', 'LSE'],
            'price_date': ['2025-01-15', '2025-01-15'],
            'open_price': [100.0, 200.0],
            'high_price': [105.0, 210.0],
            'low_price': [99.0, 198.0],
            'close_price': [103.0, 205.0],
            'volume': [1000000, 500000]
        })
        
        config = PipelineConfig(
            fetch_yahoo=True,
            validate_data=False  # Skip validation to test transformation
        )
        
        orchestrator = PipelineOrchestrator(config)
        
        with patch.object(orchestrator.yahoo_source, 'fetch', return_value=messy_data):
            result = orchestrator.run(execution_date=date.today())
        
        # Transformation should clean the data
        assert result.stocks_processed > 0
        
        # Verify data was cleaned properly
        stock_repo = StockRepository(db_session)
        stocks = stock_repo.get_all()
        
        if stocks:
            # Stock codes should be uppercase and trimmed
            stock_codes = [s.stock_code for s in stocks]
            assert all(code == code.strip().upper() for code in stock_codes)
    
    def test_pipeline_indicator_calculation(
        self, db_session, sample_sectors, sample_stocks, sample_prices
    ):
        """Test indicator calculation stage."""
        stock_repo = StockRepository(db_session)
        price_repo = PriceRepository(db_session)
        
        # Create stock and historical prices
        stock_id = stock_repo.create_stock(sample_stocks[0])
        db_session.commit()
        
        # Create 60 days of price data for indicator calculations
        for i in range(60):
            price_data = {
                'stock_id': stock_id,
                'price_date': date.today() - timedelta(days=60-i),
                'close_price': Decimal('100.0') + Decimal(str(i * 0.5)),
                'volume': 1000000,
                'source': 'test'
            }
            price_repo.upsert_price(price_data)
        db_session.commit()
        
        # Run pipeline with only indicator calculation
        config = PipelineConfig(
            fetch_ngx=False,
            fetch_yahoo=False,
            load_stocks=False,
            load_prices=False,
            calculate_indicators=True,
            evaluate_alerts=False
        )
        
        orchestrator = PipelineOrchestrator(config)
        result = orchestrator.run(execution_date=date.today())
        
        # Should calculate indicators
        assert result.indicators_calculated > 0
        
        # Verify indicators were saved
        indicator_repo = IndicatorRepository(db_session)
        latest_indicators = indicator_repo.get_latest(stock_id)
        
        assert latest_indicators is not None
        assert hasattr(latest_indicators, 'rsi_14') or hasattr(latest_indicators, 'sma_20')
    
    def test_pipeline_alert_evaluation(
        self, db_session, sample_sectors, sample_stocks, sample_prices, sample_alert_rules
    ):
        """Test alert evaluation stage."""
        stock_repo = StockRepository(db_session)
        price_repo = PriceRepository(db_session)
        alert_repo = AlertRepository(db_session)
        
        # Create stock
        stock_id = stock_repo.create_stock(sample_stocks[0])
        db_session.commit()
        
        # Create price with significant movement
        prices = [
            {
                'stock_id': stock_id,
                'price_date': date.today() - timedelta(days=1),
                'close_price': Decimal('100.00'),
                'volume': 1000000,
                'source': 'test'
            },
            {
                'stock_id': stock_id,
                'price_date': date.today(),
                'close_price': Decimal('110.00'),  # 10% increase
                'volume': 1000000,
                'source': 'test'
            }
        ]
        
        for price_data in prices:
            price_repo.upsert_price(price_data)
        db_session.commit()
        
        # Create alert rule
        rule = AlertRule(
            rule_name='Price Alert',
            rule_type='PRICE_MOVEMENT',
            threshold=5.0,
            is_active=True
        )
        alert_repo.create_rule(rule)
        db_session.commit()
        
        # Run pipeline with only alert evaluation
        config = PipelineConfig(
            fetch_ngx=False,
            fetch_yahoo=False,
            load_stocks=False,
            load_prices=False,
            calculate_indicators=False,
            evaluate_alerts=True
        )
        
        orchestrator = PipelineOrchestrator(config)
        result = orchestrator.run(execution_date=date.today())
        
        # Should generate alerts
        assert result.alerts_generated >= 1
    
    def test_pipeline_error_handling(self, db_session):
        """Test pipeline error handling."""
        config = PipelineConfig(fetch_yahoo=True)
        orchestrator = PipelineOrchestrator(config)
        
        # Mock data source to raise exception
        with patch.object(orchestrator.yahoo_source, 'fetch', side_effect=Exception('API Error')):
            result = orchestrator.run(execution_date=date.today())
        
        # Should capture error and not crash
        assert isinstance(result, PipelineResult)
        assert len(result.errors) > 0
    
    def test_pipeline_stage_timing(self, db_session):
        """Test that pipeline tracks stage execution times."""
        config = PipelineConfig(
            fetch_yahoo=True,
            lookback_days=5
        )
        
        orchestrator = PipelineOrchestrator(config)
        
        # Mock quick data fetch
        mock_data = pd.DataFrame({
            'stock_code': ['GTCO.L'],
            'company_name': ['Guaranty Trust'],
            'sector': ['Financials'],
            'exchange': ['LSE'],
            'price_date': [date.today()],
            'open_price': [100.0],
            'high_price': [105.0],
            'low_price': [99.0],
            'close_price': [103.0],
            'volume': [1000000]
        })
        
        with patch.object(orchestrator.yahoo_source, 'fetch', return_value=mock_data):
            result = orchestrator.run(execution_date=date.today())
        
        # Should track timing
        assert result.execution_time > 0
        assert isinstance(result.stage_times, dict)


@pytest.mark.integration
@pytest.mark.external
class TestEndToEndPipeline:
    """Test complete end-to-end pipeline flow."""
    
    def test_full_pipeline_workflow(self, db_session):
        """Test complete workflow from fetch to alerts."""
        # This test runs the full pipeline with all stages enabled
        config = PipelineConfig(
            fetch_ngx=False,  # Skip to avoid external dependency
            fetch_yahoo=True,
            validate_data=True,
            load_stocks=True,
            load_prices=True,
            calculate_indicators=True,
            evaluate_alerts=True,
            lookback_days=7
        )
        
        orchestrator = PipelineOrchestrator(config)
        
        # Create an alert rule before running pipeline
        alert_repo = AlertRepository(db_session)
        rule = AlertRule(
            rule_name='Test Alert',
            rule_type='PRICE_MOVEMENT',
            threshold=5.0,
            is_active=True
        )
        alert_repo.create_rule(rule)
        db_session.commit()
        
        # Run full pipeline
        result = orchestrator.run(execution_date=date.today() - timedelta(days=1))
        
        # Verify result
        assert isinstance(result, PipelineResult)
        assert result.execution_time > 0
        
        # If successful, verify data flow
        if result.success:
            # Check stocks were loaded
            stock_repo = StockRepository(db_session)
            stocks = stock_repo.get_all()
            assert len(stocks) > 0
            
            # Check prices were loaded
            price_repo = PriceRepository(db_session)
            for stock in stocks:
                latest_price = price_repo.get_latest(stock.stock_id)
                if latest_price:
                    assert latest_price.close_price > 0
