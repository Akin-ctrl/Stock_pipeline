#!/usr/bin/env python3
"""
Historical backfill script for Afrimarket data.

Fetches historical price data from Afrimarket API for all Nigerian stocks
and loads it into the staging area for reconciliation with existing NGX data.

Usage:
    python scripts/backfill_historical_afrimarket.py --years 10
    python scripts/backfill_historical_afrimarket.py --stocks DANGCEM,ZENITHBANK --years 1
    python scripts/backfill_historical_afrimarket.py --start-date 2020-01-01 --end-date 2023-12-31
"""

import sys
import argparse
from datetime import date, datetime, timedelta
from typing import List, Optional
import pandas as pd
from decimal import Decimal

# Add project root to path
sys.path.insert(0, '/home/Stock_pipeline')

from app.config.database import get_db
from app.services.data_sources.afrimarket_source import AfrimarketDataSource
from app.repositories.staging_repository import StagingRepository
from app.repositories import StockRepository
from app.utils.logger import get_logger


class HistoricalBackfill:
    """
    Backfill historical Afrimarket data into staging.
    """
    
    def __init__(
        self,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        stock_codes: Optional[List[str]] = None,
        batch_size: int = 1000,
        rate_limit_delay: float = 0.5
    ):
        """
        Initialize backfill.
        
        Args:
            start_date: Start date for backfill (default: 10 years ago)
            end_date: End date for backfill (default: yesterday)
            stock_codes: Specific stocks to backfill (default: all)
            batch_size: Records per database batch
            rate_limit_delay: Seconds to wait between API calls
        """
        self.start_date = start_date or (date.today() - timedelta(days=365*10))
        self.end_date = end_date or (date.today() - timedelta(days=1))
        self.stock_codes = stock_codes
        self.batch_size = batch_size
        self.rate_limit_delay = rate_limit_delay
        
        self.logger = get_logger("historical_backfill")
        self.db = get_db()
        self.afrimarket_source = AfrimarketDataSource()
        
        # Stats
        self.total_stocks = 0
        self.processed_stocks = 0
        self.total_records = 0
        self.failed_stocks = []
        self.skipped_stocks = []
    
    def run(self) -> dict:
        """
        Execute backfill.
        
        Returns:
            Dict with backfill statistics
        """
        start_time = datetime.now()
        
        self.logger.info("=" * 80)
        self.logger.info("Starting Afrimarket Historical Backfill")
        self.logger.info("=" * 80)
        self.logger.info(f"Date range: {self.start_date} to {self.end_date}")
        self.logger.info(f"Days: {(self.end_date - self.start_date).days}")
        
        try:
            # Get stocks to process
            stocks = self._get_stocks()
            self.total_stocks = len(stocks)
            
            if not stocks:
                self.logger.warning("No stocks to process")
                return self._build_result(start_time, success=False)
            
            self.logger.info(f"Processing {self.total_stocks} stocks")
            
            # Process each stock
            for stock_code in stocks:
                try:
                    self._process_stock(stock_code)
                    self.processed_stocks += 1
                    
                    # Progress update every 10 stocks
                    if self.processed_stocks % 10 == 0:
                        progress = (self.processed_stocks / self.total_stocks) * 100
                        self.logger.info(
                            f"Progress: {self.processed_stocks}/{self.total_stocks} "
                            f"({progress:.1f}%) - {self.total_records} records loaded"
                        )
                except Exception as e:
                    self.logger.error(f"Failed to process {stock_code}: {str(e)}")
                    self.failed_stocks.append(stock_code)
            
            success = len(self.failed_stocks) == 0
            result = self._build_result(start_time, success)
            self._log_summary(result)
            
            return result
            
        except Exception as e:
            self.logger.error(f"Backfill failed: {str(e)}", exc_info=True)
            return self._build_result(start_time, success=False)
    
    def _get_stocks(self) -> List[str]:
        """
        Get list of stocks to process.
        
        Returns:
            List of stock codes
        """
        if self.stock_codes:
            # Use provided list
            return self.stock_codes
        
        # Get all active stocks from database
        with self.db.get_session() as session:
            stock_repo = StockRepository(session)
            stocks = stock_repo.get_all_active()
            return [s.stock_code for s in stocks]
    
    def _process_stock(self, stock_code: str) -> None:
        """
        Process historical data for one stock.
        
        Args:
            stock_code: Stock code to process
        """
        self.logger.info(f"Processing {stock_code}...")
        
        try:
            # Fetch historical data from Afrimarket
            historical_data = self.afrimarket_source.fetch_historical(
                stock_codes=[stock_code],
                start_date=self.start_date,
                end_date=self.end_date
            )
            
            if historical_data.empty:
                self.logger.warning(f"No historical data for {stock_code}")
                self.skipped_stocks.append(stock_code)
                return
            
            self.logger.info(f"Fetched {len(historical_data)} records for {stock_code}")
            
            # Calculate daily change percentages
            historical_data = self._calculate_changes(historical_data)
            
            # Validate schema and table compatibility
            self._validate_schema(historical_data, stock_code)
            
            # Load to staging in batches
            records_loaded = self._load_to_staging(stock_code, historical_data)
            self.total_records += records_loaded
            
            self.logger.info(f"Loaded {records_loaded} records for {stock_code}")
            
        except Exception as e:
            error_msg = str(e)
            if "No historical data fetched for any stock" in error_msg:
                self.logger.warning(f"No historical data for {stock_code}, skipping")
                self.skipped_stocks.append(stock_code)
                return

            self.logger.error(f"Error processing {stock_code}: {error_msg}")
            raise
    
    def _validate_schema(self, data: pd.DataFrame, stock_code: str) -> bool:
        """
        Validate staging data schema and compatibility with fact table.
        
        Args:
            data: DataFrame to validate
            stock_code: Stock code for logging
            
        Returns:
            True if valid, False otherwise
            
        Raises:
            ValueError: If required columns missing or data invalid
        """
        # Required columns for fact_daily_prices
        required_columns = ['price_date', 'close_price', 'change_1d_pct', 'change_ytd_pct']
        
        # Check required columns exist
        missing_cols = [col for col in required_columns if col not in data.columns]
        if missing_cols:
            raise ValueError(f"Missing required columns: {missing_cols}")
        
        # Validate column types and values
        errors = []
        
        # price_date must be datetime/Date
        if not pd.api.types.is_datetime64_any_dtype(data['price_date']):
            try:
                data['price_date'] = pd.to_datetime(data['price_date'])
            except Exception as e:
                errors.append(f"price_date conversion failed: {str(e)}")
        
        # close_price must be numeric and > 0
        if not pd.api.types.is_numeric_dtype(data['close_price']):
            try:
                data['close_price'] = pd.to_numeric(data['close_price'])
            except Exception as e:
                errors.append(f"close_price conversion failed: {str(e)}")
        
        # Validate positive prices
        if (data['close_price'] <= 0).any():
            invalid_count = (data['close_price'] <= 0).sum()
            errors.append(f"Found {invalid_count} non-positive prices (must be > 0)")
        
        # Validate change percentages are numeric
        for col in ['change_1d_pct', 'change_ytd_pct']:
            if col in data.columns:
                if not pd.api.types.is_numeric_dtype(data[col]):
                    try:
                        data[col] = pd.to_numeric(data[col])
                    except Exception as e:
                        errors.append(f"{col} conversion failed: {str(e)}")
        
        # Check for nulls in required fields
        null_counts = {
            col: data[col].isnull().sum()
            for col in required_columns
        }
        null_required = {k: v for k, v in null_counts.items() if v > 0}
        if null_required:
            errors.append(f"Found nulls in required columns: {null_required}")
        
        if errors:
            error_msg = "; ".join(errors)
            self.logger.error(f"Schema validation failed for {stock_code}: {error_msg}")
            raise ValueError(error_msg)
        
        self.logger.info(f"Schema validation passed for {stock_code}")
        return True
    
    def _calculate_changes(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate change_1d_pct and change_ytd_pct.
        
        Args:
            data: DataFrame with historical prices
            
        Returns:
            DataFrame with change percentages
        """
        # Sort by date
        data = data.sort_values('price_date').copy()
        
        # Ensure price_date is datetime type (afrimarket source returns date objects)
        data['price_date'] = pd.to_datetime(data['price_date'])
        
        # Calculate 1-day change
        data['prev_close'] = data['close_price'].shift(1)
        data['change_1d_pct'] = (
            (data['close_price'] - data['prev_close']) / data['prev_close'] * 100
        )
        
        # Calculate YTD change
        data['year'] = data['price_date'].dt.year
        data['ytd_start_price'] = data.groupby('year')['close_price'].transform('first')
        data['change_ytd_pct'] = (
            (data['close_price'] - data['ytd_start_price']) / data['ytd_start_price'] * 100
        )
        
        # Fill NaN values with 0 (first row has no previous price for 1-day change)
        data['change_1d_pct'] = data['change_1d_pct'].fillna(0)
        data['change_ytd_pct'] = data['change_ytd_pct'].fillna(0)
        
        # Drop helper columns
        data = data.drop(columns=['prev_close', 'year', 'ytd_start_price'])
        
        return data
    
    def _load_to_staging(self, stock_code: str, data: pd.DataFrame) -> int:
        """
        Load historical data to staging table.
        
        Args:
            stock_code: Stock code
            data: DataFrame with historical prices
            
        Returns:
            Number of records loaded
        """
        total_loaded = 0
        
        with self.db.get_session() as session:
            staging_repo = StagingRepository(session)
            
            # Process in batches
            for i in range(0, len(data), self.batch_size):
                batch = data.iloc[i:i + self.batch_size].copy()
                
                # Ensure stock_code column exists
                batch['stock_code'] = stock_code
                
                # Ensure all required columns exist
                if 'volume' not in batch.columns:
                    batch['volume'] = None
                
                # Bulk insert with DataFrame
                loaded = staging_repo.bulk_insert_staging(
                    df=batch,
                    source='afrimarket'
                )
                session.commit()
                total_loaded += loaded
        
        return total_loaded
    
    def _build_result(self, start_time: datetime, success: bool) -> dict:
        """
        Build result dictionary.
        
        Args:
            start_time: Start time of backfill
            success: Whether backfill succeeded
            
        Returns:
            Result dictionary
        """
        execution_time = (datetime.now() - start_time).total_seconds()
        
        return {
            'success': success,
            'execution_time': execution_time,
            'start_date': str(self.start_date),
            'end_date': str(self.end_date),
            'total_stocks': self.total_stocks,
            'processed_stocks': self.processed_stocks,
            'failed_stocks': len(self.failed_stocks),
            'skipped_stocks': len(self.skipped_stocks),
            'total_records': self.total_records,
            'failed_stock_list': self.failed_stocks,
            'skipped_stock_list': self.skipped_stocks
        }
    
    def _log_summary(self, result: dict) -> None:
        """
        Log backfill summary.
        
        Args:
            result: Result dictionary
        """
        self.logger.info("=" * 80)
        self.logger.info("Backfill Summary")
        self.logger.info("=" * 80)
        self.logger.info(f"Status: {'SUCCESS' if result['success'] else 'FAILED'}")
        self.logger.info(f"Execution time: {result['execution_time']:.2f}s")
        self.logger.info(f"Date range: {result['start_date']} to {result['end_date']}")
        self.logger.info(f"Total stocks: {result['total_stocks']}")
        self.logger.info(f"Processed stocks: {result['processed_stocks']}")
        self.logger.info(f"Failed stocks: {result['failed_stocks']}")
        self.logger.info(f"Skipped stocks: {result['skipped_stocks']}")
        self.logger.info(f"Total records loaded: {result['total_records']}")
        
        if result['failed_stock_list']:
            self.logger.info(f"\nFailed stocks: {', '.join(result['failed_stock_list'][:10])}")
        
        if result['skipped_stock_list']:
            self.logger.info(f"\nSkipped stocks (no data): {', '.join(result['skipped_stock_list'][:10])}")
        
        self.logger.info("=" * 80)


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description='Backfill historical Afrimarket data'
    )
    parser.add_argument(
        '--years',
        type=int,
        help='Number of years to backfill (default: 10)'
    )
    parser.add_argument(
        '--start-date',
        type=str,
        help='Start date (YYYY-MM-DD)'
    )
    parser.add_argument(
        '--end-date',
        type=str,
        help='End date (YYYY-MM-DD, default: yesterday)'
    )
    parser.add_argument(
        '--stocks',
        type=str,
        help='Comma-separated list of stock codes (default: all)'
    )
    parser.add_argument(
        '--batch-size',
        type=int,
        default=1000,
        help='Records per database batch (default: 1000)'
    )
    parser.add_argument(
        '--test',
        action='store_true',
        help='Test mode: 5 stocks, 1 year'
    )
    
    args = parser.parse_args()
    
    # Parse dates
    if args.test:
        # Test mode: 5 stocks, 1 year
        start_date = date.today() - timedelta(days=365)
        end_date = date.today() - timedelta(days=1)
        stock_codes = None  # Will use first 5 from database
        print("Running in TEST MODE: 1 year, first 5 stocks")
    elif args.start_date and args.end_date:
        start_date = datetime.strptime(args.start_date, '%Y-%m-%d').date()
        end_date = datetime.strptime(args.end_date, '%Y-%m-%d').date()
        stock_codes = args.stocks.split(',') if args.stocks else None
    elif args.years:
        start_date = date.today() - timedelta(days=365 * args.years)
        end_date = date.today() - timedelta(days=1)
        stock_codes = args.stocks.split(',') if args.stocks else None
    else:
        # Default: 10 years
        start_date = date.today() - timedelta(days=365 * 10)
        end_date = date.today() - timedelta(days=1)
        stock_codes = args.stocks.split(',') if args.stocks else None
    
    # Create and run backfill
    backfill = HistoricalBackfill(
        start_date=start_date,
        end_date=end_date,
        stock_codes=stock_codes[:5] if args.test and stock_codes else stock_codes,
        batch_size=args.batch_size
    )
    
    result = backfill.run()
    
    # Exit with appropriate code
    sys.exit(0 if result['success'] else 1)


if __name__ == '__main__':
    main()
