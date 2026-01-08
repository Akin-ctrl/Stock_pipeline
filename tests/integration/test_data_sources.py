"""
Integration tests for data sources (NGX).

Tests data fetching, parsing, error handling, and data quality.
"""

import pytest
from datetime import date, timedelta
import pandas as pd
from unittest.mock import Mock, patch

from app.services.data_sources import NGXDataSource
from app.utils.exceptions import DataFetchError


@pytest.mark.integration
@pytest.mark.external
class TestNGXDataSource:
    """Test NGX data source integration."""
    
    def test_fetch_with_default_dates(self):
        """Test fetching NGX data with default dates."""
        source = NGXDataSource()
        df = source.fetch()
        
        # NGX data might be empty if API is down or no new data
        # but it should return a DataFrame
        assert isinstance(df, pd.DataFrame), "Should return DataFrame"
        
        if not df.empty:
            assert 'stock_code' in df.columns
            assert 'company_name' in df.columns
            assert 'price_date' in df.columns
            assert df['exchange'].iloc[0] == 'NGX'
    
    def test_fetch_includes_required_columns_when_data_available(self):
        """Test required columns when data is available."""
        source = NGXDataSource()
        df = source.fetch()
        
        if not df.empty:
            required_columns = [
                'stock_code', 'company_name', 'sector', 'exchange',
                'price_date', 'close_price'
            ]
            
            for col in required_columns:
                assert col in df.columns, f"Missing required column: {col}"
