"""
Data source for Yahoo Finance.

Fetches historical stock data for LSE-listed Nigerian stocks using yfinance.
"""

import time
from typing import Optional, List
from datetime import date, datetime, timedelta
import pandas as pd
import yfinance as yf

from app.services.data_sources.base import DataSource
from app.config import get_settings
from app.utils import retry
from app.utils.exceptions import DataFetchError, DataParseError


class YahooDataSource(DataSource):
    """
    Data source for Yahoo Finance data.
    
    Fetches historical OHLCV data for Nigerian stocks listed on LSE
    (SEPL.L for Seplat Energy, GTCO.L for Guaranty Trust).
    """
    
    # Mapping of ticker symbols to company info
    TICKER_INFO = {
        'SEPL.L': {
            'company_name': 'Seplat Energy Plc',
            'sector': 'Oil & Gas',
            'exchange': 'LSE'
        },
        'GTCO.L': {
            'company_name': 'Guaranty Trust Holding Company Plc',
            'sector': 'Financials',
            'exchange': 'LSE'
        }
    }
    
    def __init__(self):
        """Initialize Yahoo Finance data source."""
        super().__init__("yahoo")
        self.settings = get_settings()
        
        # Get tickers from config (already a list)
        self.tickers = self.settings.data_sources.yahoo_tickers
        
        self.logger.info(f"Initialized Yahoo source with tickers: {self.tickers}")
    
    @retry(max_attempts=3, delay=2.0)
    def fetch(
        self,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None
    ) -> pd.DataFrame:
        """
        Fetch historical stock data from Yahoo Finance.
        
        Args:
            start_date: Start date (default: 90 days ago)
            end_date: End date (default: today)
            
        Returns:
            DataFrame with columns:
                - stock_code
                - company_name
                - sector
                - exchange (always 'LSE')
                - price_date
                - open_price
                - high_price
                - low_price
                - close_price
                - volume
                - change_1d_pct
        """
        start_time = time.time()
        
        # Set default date range
        if not end_date:
            end_date = date.today()
        if not start_date:
            start_date = end_date - timedelta(days=90)
        
        try:
            self.logger.info(
                f"Fetching Yahoo data for {len(self.tickers)} tickers",
                extra={
                    "tickers": self.tickers,
                    "start_date": str(start_date),
                    "end_date": str(end_date)
                }
            )
            
            # Fetch data for all tickers
            all_data = []
            for ticker in self.tickers:
                ticker_df = self._fetch_ticker(ticker, start_date, end_date)
                if not ticker_df.empty:
                    all_data.append(ticker_df)
            
            if not all_data:
                self.logger.warning("No data fetched from Yahoo Finance")
                return pd.DataFrame()
            
            # Combine all tickers
            df = pd.concat(all_data, ignore_index=True)
            
            # Validate
            self.validate_dataframe(df)
            
            # Log summary
            duration = time.time() - start_time
            self.log_fetch_summary(df, duration)
            
            return df
            
        except Exception as e:
            self.logger.error(f"Failed to fetch Yahoo data: {str(e)}")
            raise DataFetchError(f"Yahoo fetch failed: {str(e)}") from e
    
    def _fetch_ticker(
        self,
        ticker: str,
        start_date: date,
        end_date: date
    ) -> pd.DataFrame:
        """
        Fetch data for a single ticker.
        
        Args:
            ticker: Ticker symbol (e.g., 'SEPL.L')
            start_date: Start date
            end_date: End date
            
        Returns:
            DataFrame with ticker data
        """
        try:
            self.logger.debug(f"Fetching {ticker}")
            
            # Download data using yfinance
            ticker_obj = yf.Ticker(ticker)
            hist = ticker_obj.history(
                start=start_date,
                end=end_date,
                auto_adjust=False  # Keep unadjusted prices
            )
            
            if hist.empty:
                self.logger.warning(f"No data returned for {ticker}")
                return pd.DataFrame()
            
            # Reset index to make date a column
            hist = hist.reset_index()
            
            # Rename columns to standard format
            hist = hist.rename(columns={
                'Date': 'price_date',
                'Open': 'open_price',
                'High': 'high_price',
                'Low': 'low_price',
                'Close': 'close_price',
                'Volume': 'volume'
            })
            
            # Convert date to date object (not datetime)
            hist['price_date'] = pd.to_datetime(hist['price_date']).dt.date
            
            # Calculate 1-day percentage change
            hist['change_1d_pct'] = hist['close_price'].pct_change() * 100
            
            # Add ticker info
            info = self.TICKER_INFO.get(ticker, {
                'company_name': ticker,
                'sector': 'Unknown',
                'exchange': 'LSE'
            })
            
            hist['stock_code'] = ticker
            hist['company_name'] = info['company_name']
            hist['sector'] = info['sector']
            hist['exchange'] = info['exchange']
            hist['source'] = 'yf'  # Add source identifier
            
            # Select relevant columns
            columns = [
                'stock_code',
                'company_name',
                'sector',
                'exchange',
                'price_date',
                'open_price',
                'high_price',
                'low_price',
                'close_price',
                'volume',
                'change_1d_pct',
                'source'
            ]
            
            hist = hist[columns]
            
            self.logger.debug(
                f"Fetched {len(hist)} records for {ticker}",
                extra={"ticker": ticker, "records": len(hist)}
            )
            
            return hist
            
        except Exception as e:
            self.logger.error(
                f"Failed to fetch {ticker}: {str(e)}",
                extra={"ticker": ticker, "error": str(e)}
            )
            # Return empty DataFrame instead of raising, so other tickers can be fetched
            return pd.DataFrame()
    
    def add_ticker(self, ticker: str, company_name: str, sector: str) -> None:
        """
        Add a new ticker to fetch.
        
        Args:
            ticker: Yahoo Finance ticker symbol (e.g., 'XYZ.L')
            company_name: Full company name
            sector: Sector name
        """
        if ticker not in self.tickers:
            self.tickers.append(ticker)
            self.TICKER_INFO[ticker] = {
                'company_name': company_name,
                'sector': sector,
                'exchange': 'LSE'
            }
            self.logger.info(f"Added ticker {ticker}")
