"""
Data transformer for stock price data.

Handles data cleaning, standardization, and enrichment.
"""

from typing import Optional
from datetime import date, datetime
import pandas as pd
import numpy as np

from app.utils import get_logger


class DataTransformer:
    """
    Transforms and cleans stock price data.
    
    Operations:
    - Standardize stock codes (uppercase, trim)
    - Clean company names
    - Calculate derived fields (change_%, YTD%)
    - Standardize data types
    - Fill missing values
    """
    
    def __init__(self):
        """Initialize transformer."""
        self.logger = get_logger("transformer")
    
    def transform(self, df: pd.DataFrame, source: str) -> pd.DataFrame:
        """
        Apply all transformations to DataFrame.
        
        Args:
            df: Raw DataFrame from data source
            source: Data source name ('ngx', 'yahoo', etc.)
            
        Returns:
            Transformed DataFrame ready for database insertion
        """
        if df.empty:
            return df
        
        self.logger.info(f"Transforming {len(df)} records from {source}")
        
        df = df.copy()
        
        # Basic cleaning
        df = self._standardize_stock_codes(df)
        df = self._clean_company_names(df)
        df = self._standardize_dates(df)
        
        # Add metadata
        df['source'] = source
        df['has_complete_data'] = self._check_completeness(df)
        df['ingestion_timestamp'] = datetime.now()
        
        # Fill missing optional fields
        df = self._fill_missing_values(df)
        
        self.logger.info(f"Transformation complete: {len(df)} records")
        
        return df
    
    def _standardize_stock_codes(self, df: pd.DataFrame) -> pd.DataFrame:
        """Standardize stock code format."""
        if 'stock_code' in df.columns:
            df['stock_code'] = (
                df['stock_code']
                .astype(str)
                .str.strip()
                .str.upper()
                .str.replace(r'[^A-Z0-9.]', '', regex=True)  # Remove special chars except dot
            )
        return df
    
    def _clean_company_names(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean and standardize company names."""
        if 'company_name' in df.columns:
            df['company_name'] = (
                df['company_name']
                .astype(str)
                .str.strip()
                .str.replace(r'\s+', ' ', regex=True)  # Normalize whitespace
                .str.title()  # Title case
            )
        return df
    
    def _standardize_dates(self, df: pd.DataFrame) -> pd.DataFrame:
        """Ensure price_date is proper date object."""
        if 'price_date' in df.columns:
            # Convert to date if it's datetime
            if pd.api.types.is_datetime64_any_dtype(df['price_date']):
                df['price_date'] = pd.to_datetime(df['price_date']).dt.date
            elif not pd.api.types.is_object_dtype(df['price_date']):
                df['price_date'] = pd.to_datetime(df['price_date']).dt.date
        return df
    
    def _check_completeness(self, df: pd.DataFrame) -> pd.Series:
        """
        Check if each record has complete OHLCV data.
        
        Returns:
            Boolean series indicating completeness
        """
        ohlcv_cols = ['open_price', 'high_price', 'low_price', 'close_price', 'volume']
        existing_cols = [col for col in ohlcv_cols if col in df.columns]
        
        if not existing_cols:
            return pd.Series(False, index=df.index)
        
        # Has complete data if all OHLCV columns are non-null
        return df[existing_cols].notna().all(axis=1)
    
    def _fill_missing_values(self, df: pd.DataFrame) -> pd.DataFrame:
        """Fill missing values with appropriate defaults."""
        # Fill numeric NaN with None (for SQL NULL)
        numeric_cols = ['open_price', 'high_price', 'low_price', 'volume', 
                       'change_1d_pct', 'change_ytd_pct']
        
        for col in numeric_cols:
            if col in df.columns:
                df[col] = df[col].replace({np.nan: None})
        
        # Fill missing sectors with 'Unknown'
        if 'sector' in df.columns:
            df['sector'] = df['sector'].fillna('Unknown')
        
        # Fill missing market cap with empty string
        if 'market_cap' in df.columns:
            df['market_cap'] = df['market_cap'].fillna('')
        
        return df
    
    def calculate_ytd_change(
        self,
        df: pd.DataFrame,
        reference_prices: pd.DataFrame
    ) -> pd.DataFrame:
        """
        Calculate YTD percentage change.
        
        Args:
            df: Current price data
            reference_prices: DataFrame with stock_code and year_start_price
            
        Returns:
            DataFrame with change_ytd_pct filled
        """
        if reference_prices.empty:
            return df
        
        # Merge with reference prices
        df = df.merge(
            reference_prices[['stock_code', 'year_start_price']],
            on='stock_code',
            how='left'
        )
        
        # Calculate YTD change
        df['change_ytd_pct'] = (
            (df['close_price'] - df['year_start_price']) / df['year_start_price'] * 100
        )
        
        # Drop temporary column
        df = df.drop(columns=['year_start_price'])
        
        return df
    
    def deduplicate(
        self,
        df: pd.DataFrame,
        keep: str = 'last'
    ) -> pd.DataFrame:
        """
        Remove duplicate records.
        
        Args:
            df: DataFrame
            keep: Which duplicate to keep ('first', 'last')
            
        Returns:
            Deduplicated DataFrame
        """
        before_count = len(df)
        
        df = df.drop_duplicates(
            subset=['stock_code', 'price_date'],
            keep=keep
        )
        
        after_count = len(df)
        removed = before_count - after_count
        
        if removed > 0:
            self.logger.warning(
                f"Removed {removed} duplicate records",
                extra={"removed": removed}
            )
        
        return df
