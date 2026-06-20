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
        assert transformed['stock_code'].iloc[0] == 'GTCO.L', "Stock code should be uppercase and trimmed"
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
        
        transformed = transformer.transform(raw_data, source='afrimarket')
        
        # Missing volume remains None (SQL NULL)
        assert transformed['volume'].iloc[2] is None
        
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
        validator = DataValidator(valid_sectors=[])
        
        valid_data = pd.DataFrame({
            'stock_code': ['GTCO', 'SEPL'],
            'company_name': ['Guaranty Trust', 'Seplat Energy'],
            'exchange': ['NGX', 'NGX'],
            'price_date': [date(2025, 1, 15), date(2025, 1, 15)],
            'close_price': [100.5, 200.3],
            'volume': [1000000, 500000]
        })
        
        cleaned_df, result = validator.validate(valid_data)
        assert result.is_valid
    
    def test_validate_detects_missing_required_columns(self):
        """Test detection of missing required columns."""
        validator = DataValidator(valid_sectors=[])
        
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
        validator = DataValidator(valid_sectors=[])
        
        invalid_data = pd.DataFrame({
            'stock_code': ['GTCO'],
            'company_name': ['Guaranty Trust'],
            'exchange': ['NGX'],
            'price_date': [date(2025, 1, 15)],
            'close_price': [-100.5],  # Invalid negative price
            'volume': [1000000]
        })
        
        cleaned_df, result = validator.validate(invalid_data)
        assert result.suspicious_count > 0
    
    def test_validate_empty_dataframe(self):
        """Test validation of empty DataFrame."""
        validator = DataValidator(valid_sectors=[])
        
        empty_df = pd.DataFrame()
        cleaned_df, result = validator.validate(empty_df)
        
        assert cleaned_df.empty
        assert result.total_count == 0


@pytest.mark.integration
@pytest.mark.database
class TestIndicatorCalculator:
    """Test technical indicator calculations."""
    
    def test_calculate_all_indicators(self):
        """Test calculating all indicators together."""
        calculator = IndicatorCalculator()
        
        prices = pd.DataFrame({
            'price_date': pd.date_range(start='2025-01-01', periods=100),
            'close_price': np.random.uniform(90, 110, 100),
            'volume': np.random.uniform(100000, 1000000, 100)
        })
        
        indicators = calculator.calculate_all(prices)
        
        # Should have multiple indicator columns
        assert len(indicators.columns) > len(prices.columns)
        assert 'ma_7' in indicators.columns
        assert 'ma_30' in indicators.columns
        assert 'ma_90' in indicators.columns
        assert 'rsi' in indicators.columns
        assert 'macd_line' in indicators.columns
        assert 'macd_signal' in indicators.columns
        assert 'bb_upper' in indicators.columns
        assert 'bb_lower' in indicators.columns
        assert 'volatility_30' in indicators.columns
        assert 'ma_crossover_signal' in indicators.columns


@pytest.mark.integration
class TestProcessorIntegration:
    """Test integration between transformer and validator."""
    
    def test_transform_then_validate_pipeline(self):
        """Test complete transformation and validation pipeline."""
        transformer = DataTransformer()
        validator = DataValidator(valid_sectors=[])
        
        # Raw data with issues
        raw_data = pd.DataFrame({
            'stock_code': ['  gtco  ', 'SEPL'],
            'company_name': ['  Guaranty   Trust  ', 'SEPLAT ENERGY'],
            'price_date': ['2025-01-15', '2025-01-15'],
            'close_price': [100.5, 200.3],
            'volume': [1000000, 500000]
        })
        
        # Transform
        transformed = transformer.transform(raw_data, source='yahoo')
        
        # Add exchange since validator expects it
        transformed['exchange'] = 'NGX'
        
        # Validate - should pass
        cleaned_df, result = validator.validate(transformed)
        assert result.is_valid
        
        # Verify transformation worked
        assert cleaned_df['stock_code'].iloc[0] == 'GTCO'
        assert 'source' in cleaned_df.columns

