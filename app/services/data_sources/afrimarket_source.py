"""
Data source for African stocks using the afrimarket Python API.

Uses the afrimarket package to fetch historical and current price data
for Nigerian stocks from the NGXGROUP exchange.
"""

import time
from typing import Optional, List
from datetime import date, datetime, timedelta
from decimal import Decimal
import pandas as pd

from app.services.data_sources.base import DataSource
from app.utils import retry
from app.utils.exceptions import DataFetchError, DataParseError
from app.utils.logger import get_logger

# Import afrimarket
try:
    import afrimarket as afm
except ImportError as e:
    raise ImportError(
        "afrimarket package not found. Install with: pip install afrimarket"
    ) from e


logger = get_logger(__name__)


class AfrimarketDataSource(DataSource):
    """
    Data source for African stocks using the afrimarket API.
    
    Provides historical price data for Nigerian stocks from NGXGROUP exchange.
    
    Features:
        - Historical data (10+ years, verified 2014-2026)
        - Current/latest prices
        - Bulk fetching for multiple stocks
        - Retry logic with exponential backoff
    
    API Limitations (verified):
        - Historical data: Only Date + Price columns
        - No volume, change_1d_pct, change_ytd_pct in historical
        - Current prices: May have additional fields
    
    Usage:
        >>> source = AfrimarketDataSource()
        >>> df = source.fetch_historical(['DANGCEM', 'ZENITH'], start_date, end_date)
        >>> df = source.fetch()  # Current prices for all stocks
    """
    
    def __init__(self):
        """Initialize Afrimarket data source."""
        super().__init__("afrimarket")
        self.market = 'ngx'
        self.exchange = afm.Exchange(market=self.market)
        self.logger = get_logger("datasource.afrimarket")
        
    @retry(max_attempts=3, delay=2.0)
    def fetch(
        self,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None
    ) -> pd.DataFrame:
        """
        Fetch current/latest stock prices for all available Nigerian stocks.
        
        Uses afrimarket's Exchange.get_securities() for current prices.
        
        Args:
            start_date: Ignored for current prices
            end_date: Ignored for current prices
            
        Returns:
            DataFrame with columns:
                - stock_code: Ticker symbol
                - company_name: Company name
                - sector: Sector (if available)
                - exchange: 'NGX'
                - price_date: Current date
                - close_price: Latest price
                - source: 'afrimarket'
                
        Raises:
            DataFetchError: If fetching data fails
        """
        start_time = time.time()
        
        try:
            self.logger.info("Fetching current prices from afrimarket API")
            
            # Get listed companies (has current prices)
            df = self.exchange.get_listed_companies()
            
            if df is None or df.empty:
                raise DataFetchError("No data returned from afrimarket API")
            
            # Standardize column names based on actual API
            # Columns: ['Ticker', 'Name', 'Volume', 'Price', 'Change']
            df = df.rename(columns={
                'Ticker': 'stock_code',
                'Name': 'company_name',
                'Price': 'close_price',
                'Volume': 'volume',
                'Change': 'change_1d_pct'
            })
            
            # Add metadata
            df['exchange'] = 'NGX'
            df['price_date'] = date.today()
            df['source'] = 'afrimarket'
            
            # Keep all available API columns + required metadata
            keep_cols = [
                'stock_code',
                'company_name',
                'exchange',
                'price_date',
                'close_price',
                'source',
                'volume',
                'change_1d_pct'
            ]
            df = df[keep_cols]
            
            # Filter out invalid records
            df = df.dropna(subset=['stock_code', 'close_price'])
            
            # Clean up stock codes
            df['stock_code'] = df['stock_code'].str.upper().str.strip()

            # Normalize numeric fields
            df['volume'] = pd.to_numeric(df['volume'], errors='coerce')
            df['change_1d_pct'] = pd.to_numeric(df['change_1d_pct'], errors='coerce')
            
            # Validate
            self.validate_dataframe(df)
            
            # Log summary
            duration = time.time() - start_time
            self.log_fetch_summary(df, duration)
            
            return df
            
        except Exception as e:
            self.logger.error(f"Failed to fetch afrimarket current prices: {str(e)}")
            raise DataFetchError(f"Afrimarket fetch failed: {str(e)}") from e
    
    @retry(max_attempts=3, delay=2.0)
    def fetch_historical(
        self,
        stock_codes: List[str],
        start_date: Optional[date] = None,
        end_date: Optional[date] = None
    ) -> pd.DataFrame:
        """
        Fetch historical price data for specified stocks.
        
        Uses afrimarket's Stock.get_price() for historical data.
        
        Args:
            stock_codes: List of ticker symbols (e.g., ['DANGCEM', 'ZENITH'])
            start_date: Start date (default: 10 years ago)
            end_date: End date (default: today)
            
        Returns:
            DataFrame with columns:
                - stock_code: Ticker symbol
                - price_date: Trading date
                - close_price: Closing price
                - source: 'afrimarket'
                
        Note:
            - Historical data contains ONLY Date + Price columns (verified)
            - change_1d_pct, change_ytd_pct, volume must be calculated separately
            
        Raises:
            DataFetchError: If fetching data fails
        """
        start_time = time.time()
        
        # Set default date range (10 years)
        if not start_date:
            start_date = date.today() - timedelta(days=3650)
        if not end_date:
            end_date = date.today()
        
        try:
            self.logger.info(
                f"Fetching historical data for {len(stock_codes)} stocks "
                f"from {start_date} to {end_date}"
            )
            
            all_records = []
            
            for i, stock_code in enumerate(stock_codes, 1):
                try:
                    self.logger.debug(f"Fetching {stock_code} ({i}/{len(stock_codes)})")
                    
                    # Create Stock instance (use lowercase ticker)
                    stock = afm.Stock(ticker=stock_code.lower(), market=self.market)
                    
                    # Fetch historical prices
                    # Returns DataFrame with ['Date', 'Price'] columns (verified)
                    df_price = stock.get_price()
                    
                    if df_price is None or df_price.empty:
                        self.logger.warning(f"No historical data for {stock_code}")
                        continue
                    
                    # Standardize columns
                    df_price = df_price.rename(columns={
                        'Date': 'price_date',
                        'Price': 'close_price'
                    })
                    
                    # Add stock code and source
                    df_price['stock_code'] = stock_code
                    df_price['source'] = 'afrimarket'
                    
                    # Convert date to datetime.date
                    df_price['price_date'] = pd.to_datetime(df_price['price_date']).dt.date
                    
                    # Filter by date range if needed
                    if start_date or end_date:
                        if start_date:
                            df_price = df_price[df_price['price_date'] >= start_date]
                        if end_date:
                            df_price = df_price[df_price['price_date'] <= end_date]
                    
                    # Convert price to Decimal (for consistency with DB)
                    df_price['close_price'] = df_price['close_price'].astype(float)
                    
                    all_records.append(df_price)
                    
                    # Small delay to avoid rate limiting
                    if i < len(stock_codes):
                        time.sleep(0.1)
                    
                except Exception as e:
                    self.logger.error(f"Failed to fetch {stock_code}: {str(e)}")
                    continue
            
            if not all_records:
                raise DataFetchError("No historical data fetched for any stock")
            
            # Combine all stock data
            df = pd.concat(all_records, ignore_index=True)
            
            # Sort by date and stock
            df = df.sort_values(['stock_code', 'price_date'])
            
            # Remove duplicates (keep last)
            df = df.drop_duplicates(subset=['stock_code', 'price_date'], keep='last')
            
            # Reset index
            df = df.reset_index(drop=True)
            
            # Log summary
            duration = time.time() - start_time
            self.logger.info(
                f"Fetched {len(df):,} records for {len(stock_codes)} stocks "
                f"in {duration:.2f}s"
            )
            
            return df
            
        except Exception as e:
            self.logger.error(f"Failed to fetch historical data: {str(e)}")
            raise DataFetchError(f"Afrimarket historical fetch failed: {str(e)}") from e
    

    
    def fetch_all_stocks(self) -> List[str]:
        """
        Get list of all available stock codes from the exchange.
        
        Returns:
            List of stock ticker symbols
            
        Example:
            >>> source = AfrimarketDataSource()
            >>> stocks = source.fetch_all_stocks()
            >>> print(f"Found {len(stocks)} stocks")
        """
        try:
            self.logger.info("Fetching list of all available stocks")
            
            df = self.exchange.get_listed_companies()
            
            if df is None or df.empty:
                return []
            
            # Extract stock codes (Ticker column)
            stock_codes = df['Ticker'].str.upper().str.strip().tolist()
            
            self.logger.info(f"Found {len(stock_codes)} stocks on NGX")
            return sorted(stock_codes)
            
        except Exception as e:
            self.logger.error(f"Failed to fetch stock list: {str(e)}")
            return []


# Example usage / testing
if __name__ == "__main__":
    # Test current prices
    source = AfrimarketDataSource()
    
    # Test 1: Fetch all current prices
    print("Test 1: Fetching current prices...")
    df_current = source.fetch()
    print(f"✓ Fetched {len(df_current)} current prices")
    print(df_current.head())
    
    # Test 2: Fetch all available stocks
    print("\nTest 2: Fetching stock list...")
    stocks = source.fetch_all_stocks()
    print(f"✓ Found {len(stocks)} stocks")
    print(f"Sample: {stocks[:10]}")
    
    # Test 3: Fetch historical data for 2 stocks (1 year)
    print("\nTest 3: Fetching historical data...")
    test_stocks = stocks[:2] if stocks else ['DANGCEM', 'ZENITH']
    start = date.today() - timedelta(days=365)
    end = date.today()
    
    df_historical = source.fetch_historical(test_stocks, start, end)
    print(f"✓ Fetched {len(df_historical)} historical records")
    print(df_historical.head(10))
    print(df_historical.tail(10))
