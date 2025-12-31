"""
Integration tests for data processors (transformer, validator).

Tests data transformation, validation, and indicator calculations.
"""

import pytest
from datetime import date, datetime, timedelta
import pandas as pd
import numpy as np

from app.services.processors import DataTransformer, DataValidator
from app.services.indicators import IndicatorCalculator
from app.utils.exceptions import DataValidationError


@pytest.mark.integration
class TestDataTransformer:
    """Test data transformation pipeline."""
    
    def test_transform_basic_data(self):
        """Test basic data transformation."""
        transformer = DataTransformer()
        
        # Create sample raw data
        raw_data = pd.DataFrame({
            'stock_code': ['  gtco.l  ', 'SEPL.L', 'dangcem'],
            'company_name': ['  Guaranty   Trust  ', 'SEPLAT ENERGY', 'dangote cement'],
            'price_date': ['2025-01-15', '2025-01-15', '2025-01-15'],
            'close_price': [100.5, 200.3, 300.0],
            'volume': [1000000, 500000, 750000]
        })
        
        transformed = transformer.transform(raw_data, source='yahoo')
        
        # Check standardization
        assert transformed['stock_code'].iloc[0] == 'GTCOL', "Stock code should be uppercase and trimmed"
        assert transformed['stock_code'].iloc[2] == 'DANGCEM', "Stock code should be uppercase"
        
        # Check company name cleaning
        assert 'Guaranty Trust' in transformed['company_name'].iloc[0]
        assert transformed['company_name'].iloc[2] == 'Dangote Cement'
        
        # Check metadata
        assert (transformed['source'] == 'yahoo').all()
        assert 'has_complete_data' in transformed.columns
        assert 'ingestion_timestamp' in transformed.columns
    
    def test_transform_handles_missing_values(self):
        """Test handling of missing values."""
        transformer = DataTransformer()
        
        raw_data = pd.DataFrame({
            'stock_code': ['GTCO', 'SEPL', 'DANG'],
            'company_name': ['Guaranty Trust', 'Seplat', 'Dangote'],
            'price_date': ['2025-01-15', '2025-01-15', '2025-01-15'],
            'close_price': [100.5, None, 300.0],
            'volume': [1000000, 500000, None]
        })
        
        transformed = transformer.transform(raw_data, source='ngx')
        
        # Should fill missing volume with 0
        assert transformed['volume'].notna().all()
        
        # Check completeness flag
        assert transformed['has_complete_data'].iloc[0] == True
        assert transformed['has_complete_data'].iloc[1] == False  # Missing close_price
    
    def test_transform_date_standardization(self):
        """Test date standardization."""
        transformer = DataTransformer()
        
        # Mix of datetime and string dates
        raw_data = pd.DataFrame({
            'stock_code': ['GTCO', 'SEPL'],
            'company_name': ['Guaranty Trust', 'Seplat'],
            'price_date': [datetime(2025, 1, 15), '2025-01-16'],
            'close_price': [100.5, 200.3],
            'volume': [1000000, 500000]
        })
        
        transformed = transformer.transform(raw_data, source='yahoo')
        
        # All dates should be date objects
        assert all(isinstance(d, date) for d in transformed['price_date'])
    
    def test_transform_empty_dataframe(self):
        """Test transformation of empty DataFrame."""
        transformer = DataTransformer()
        
        empty_df = pd.DataFrame()
        result = transformer.transform(empty_df, source='yahoo')
        
        assert result.empty
        assert isinstance(result, pd.DataFrame)


@pytest.mark.integration
class TestDataValidator:
    """Test data validation pipeline."""
    
    def test_validate_valid_data(self):
        """Test validation of correct data."""
        validator = DataValidator()
        
        valid_data = pd.DataFrame({
            'stock_code': ['GTCO', 'SEPL'],
            'company_name': ['Guaranty Trust', 'Seplat Energy'],
            'price_date': [date(2025, 1, 15), date(2025, 1, 15)],
            'close_price': [100.5, 200.3],
            'volume': [1000000, 500000],
            'open_price': [99.0, 198.0],
            'high_price': [101.0, 202.0],
            'low_price': [98.5, 197.5]
        })
        
        # Should not raise any exceptions
        validator.validate(valid_data)
    
    def test_validate_detects_missing_required_columns(self):
        """Test detection of missing required columns."""
        validator = DataValidator()
        
        invalid_data = pd.DataFrame({
            'stock_code': ['GTCO'],
            'price_date': [date(2025, 1, 15)]
            # Missing close_price
        })
        
        with pytest.raises(DataValidationError) as exc_info:
            validator.validate(invalid_data)
        
        assert 'close_price' in str(exc_info.value).lower()
    
    def test_validate_detects_negative_prices(self):
        """Test detection of negative prices."""
        validator = DataValidator()
        
        invalid_data = pd.DataFrame({
            'stock_code': ['GTCO'],
            'company_name': ['Guaranty Trust'],
            'price_date': [date(2025, 1, 15)],
            'close_price': [-100.5],  # Invalid negative price
            'volume': [1000000]
        })
        
        with pytest.raises(DataValidationError) as exc_info:
            validator.validate(invalid_data)
        
        assert 'negative' in str(exc_info.value).lower() or 'price' in str(exc_info.value).lower()
    
    def test_validate_detects_future_dates(self):
        """Test detection of future dates."""
        validator = DataValidator()
        
        future_date = date.today() + timedelta(days=30)
        invalid_data = pd.DataFrame({
            'stock_code': ['GTCO'],
            'company_name': ['Guaranty Trust'],
            'price_date': [future_date],  # Future date
            'close_price': [100.5],
            'volume': [1000000]
        })
        
        with pytest.raises(DataValidationError) as exc_info:
            validator.validate(invalid_data)
        
        assert 'future' in str(exc_info.value).lower() or 'date' in str(exc_info.value).lower()
    
    def test_validate_empty_dataframe(self):
        """Test validation of empty DataFrame."""
        validator = DataValidator()
        
        empty_df = pd.DataFrame()
        
        # Empty DataFrame should raise validation error
        with pytest.raises(DataValidationError) as exc_info:
            validator.validate(empty_df)
        
        assert 'empty' in str(exc_info.value).lower()


@pytest.mark.integration
@pytest.mark.database
class TestIndicatorCalculator:
    """Test technical indicator calculations."""
    
    def test_calculate_sma(self):
        """Test Simple Moving Average calculation."""
        calculator = IndicatorCalculator()
        
        # Create sample price data
        prices = pd.DataFrame({
            'price_date': pd.date_range(start='2025-01-01', periods=50),
            'close_price': np.random.uniform(90, 110, 50)
        })
        
        indicators = calculator.calculate_sma(prices, periods=[20, 50])
        
        assert 'sma_20' in indicators.columns
        assert 'sma_50' in indicators.columns
        
        # First 19 values should be NaN for SMA 20
        assert indicators['sma_20'].iloc[:19].isna().all()
        # 20th value onwards should have SMA
        assert indicators['sma_20'].iloc[19:].notna().all()
    
    def test_calculate_ema(self):
        """Test Exponential Moving Average calculation."""
        calculator = IndicatorCalculator()
        
        prices = pd.DataFrame({
            'price_date': pd.date_range(start='2025-01-01', periods=50),
            'close_price': np.random.uniform(90, 110, 50)
        })
        
        indicators = calculator.calculate_ema(prices, periods=[12, 26])
        
        assert 'ema_12' in indicators.columns
        assert 'ema_26' in indicators.columns
        assert indicators['ema_12'].notna().sum() > 0
    
    def test_calculate_rsi(self):
        """Test RSI calculation."""
        calculator = IndicatorCalculator()
        
        # Create trending price data
        prices = pd.DataFrame({
            'price_date': pd.date_range(start='2025-01-01', periods=30),
            'close_price': np.linspace(90, 110, 30)  # Upward trend
        })
        
        indicators = calculator.calculate_rsi(prices, period=14)
        
        assert 'rsi_14' in indicators.columns
        
        # RSI should be between 0 and 100
        rsi_values = indicators['rsi_14'].dropna()
        assert (rsi_values >= 0).all() and (rsi_values <= 100).all()
    
    def test_calculate_macd(self):
        """Test MACD calculation."""
        calculator = IndicatorCalculator()
        
        prices = pd.DataFrame({
            'price_date': pd.date_range(start='2025-01-01', periods=50),
            'close_price': np.random.uniform(90, 110, 50)
        })
        
        indicators = calculator.calculate_macd(prices)
        
        assert 'macd' in indicators.columns
        assert 'macd_signal' in indicators.columns
        assert 'macd_histogram' in indicators.columns
    
    def test_calculate_bollinger_bands(self):
        """Test Bollinger Bands calculation."""
        calculator = IndicatorCalculator()
        
        prices = pd.DataFrame({
            'price_date': pd.date_range(start='2025-01-01', periods=30),
            'close_price': np.random.uniform(90, 110, 30)
        })
        
        indicators = calculator.calculate_bollinger_bands(prices, period=20, std_dev=2)
        
        assert 'bb_upper' in indicators.columns
        assert 'bb_middle' in indicators.columns
        assert 'bb_lower' in indicators.columns
        
        # Upper should be > Middle > Lower
        valid_rows = indicators.dropna()
        if len(valid_rows) > 0:
            assert (valid_rows['bb_upper'] >= valid_rows['bb_middle']).all()
            assert (valid_rows['bb_middle'] >= valid_rows['bb_lower']).all()
    
    def test_calculate_all_indicators(self):
        """Test calculating all indicators together."""
        calculator = IndicatorCalculator()
        
        prices = pd.DataFrame({
            'price_date': pd.date_range(start='2025-01-01', periods=60),
            'close_price': np.random.uniform(90, 110, 60),
            'volume': np.random.uniform(100000, 1000000, 60)
        })
        
        indicators = calculator.calculate_all(prices)
        
        # Should have multiple indicator columns
        assert len(indicators.columns) > len(prices.columns)
        assert 'sma_20' in indicators.columns
        assert 'rsi_14' in indicators.columns
        assert 'macd' in indicators.columns


@pytest.mark.integration
class TestProcessorIntegration:
    """Test integration between transformer and validator."""
    
    def test_transform_then_validate_pipeline(self):
        """Test complete transformation and validation pipeline."""
        transformer = DataTransformer()
        validator = DataValidator()
        
        # Raw data with issues
        raw_data = pd.DataFrame({
            'stock_code': ['  gtco  ', 'SEPL'],
            'company_name': ['  Guaranty   Trust  ', 'SEPLAT ENERGY'],
            'price_date': ['2025-01-15', '2025-01-15'],
            'close_price': [100.5, 200.3],
            'volume': [1000000, 500000],
            'open_price': [99.0, 198.0],
            'high_price': [101.0, 202.0],
            'low_price': [98.5, 197.5]
        })
        
        # Transform
        transformed = transformer.transform(raw_data, source='yahoo')
        
        # Validate - should pass
        validator.validate(transformed)
        
        # Verify transformation worked
        assert transformed['stock_code'].iloc[0] == 'GTCO'
        assert 'source' in transformed.columns
