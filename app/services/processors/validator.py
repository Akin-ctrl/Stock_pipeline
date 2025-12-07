"""
Data validator for stock price data.

Validates data quality, detects anomalies, and flags suspicious records.
"""

from typing import Dict, List, Any, Tuple
from dataclasses import dataclass
import pandas as pd
import numpy as np

from app.utils import get_logger
from app.utils.exceptions import DataValidationError


@dataclass
class ValidationResult:
    """
    Result of data validation.
    
    Attributes:
        valid_count: Number of valid records
        invalid_count: Number of invalid records
        suspicious_count: Number of suspicious but accepted records
        errors: List of validation error details
        warnings: List of validation warnings
    """
    valid_count: int
    invalid_count: int
    suspicious_count: int
    errors: List[Dict[str, Any]]
    warnings: List[Dict[str, Any]]
    
    @property
    def is_valid(self) -> bool:
        """Check if validation passed (no invalid records)."""
        return self.invalid_count == 0
    
    @property
    def total_count(self) -> int:
        """Total records processed."""
        return self.valid_count + self.invalid_count + self.suspicious_count


class DataValidator:
    """
    Validates stock price data quality.
    
    Performs checks:
    - Required fields present and non-null
    - Price ranges (min/max)
    - Percentage change ranges (anomaly detection)
    - Date validity
    - Duplicate detection
    - Sector validation
    """
    
    # Validation thresholds
    MIN_PRICE = 0.01
    MAX_PRICE = 1_000_000.0
    MAX_DAILY_CHANGE_PCT = 50.0  # Flag >50% daily change as suspicious
    VALID_EXCHANGES = {'NGX', 'LSE'}
    
    def __init__(self, valid_sectors: List[str]):
        """
        Initialize validator.
        
        Args:
            valid_sectors: List of valid sector names
        """
        self.valid_sectors = set(valid_sectors)
        self.logger = get_logger("validator")
    
    def validate(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, ValidationResult]:
        """
        Validate DataFrame and add quality flags.
        
        Args:
            df: DataFrame with stock data
            
        Returns:
            Tuple of (cleaned_df, validation_result)
            - cleaned_df has 'data_quality_flag' column added
            - Invalid records are removed
        """
        if df.empty:
            return df, ValidationResult(0, 0, 0, [], [])
        
        self.logger.info(f"Validating {len(df)} records")
        
        errors = []
        warnings = []
        
        # Add quality flag column
        df = df.copy()
        df['data_quality_flag'] = 'GOOD'
        
        # Check required fields
        required_fields = ['stock_code', 'company_name', 'exchange', 'price_date', 'close_price']
        missing_fields = [f for f in required_fields if f not in df.columns]
        
        if missing_fields:
            raise DataValidationError(f"Missing required fields: {missing_fields}")
        
        # Validate each check
        df = self._validate_null_values(df, errors)
        df = self._validate_price_ranges(df, warnings)
        df = self._validate_percentage_changes(df, warnings)
        df = self._validate_exchanges(df, errors)
        df = self._validate_sectors(df, warnings)
        df = self._check_duplicates(df, errors)
        
        # Count results
        valid_count = len(df[df['data_quality_flag'] == 'GOOD'])
        suspicious_count = len(df[df['data_quality_flag'] == 'SUSPICIOUS'])
        invalid_count = len(errors)
        
        # Remove invalid records (those flagged with errors)
        invalid_indices = [e['index'] for e in errors if 'index' in e]
        df = df[~df.index.isin(invalid_indices)]
        
        result = ValidationResult(
            valid_count=valid_count,
            invalid_count=invalid_count,
            suspicious_count=suspicious_count,
            errors=errors,
            warnings=warnings
        )
        
        self.logger.info(
            "Validation complete",
            extra={
                "valid": result.valid_count,
                "suspicious": result.suspicious_count,
                "invalid": result.invalid_count
            }
        )
        
        return df, result
    
    def _validate_null_values(self, df: pd.DataFrame, errors: List[Dict]) -> pd.DataFrame:
        """Check for null values in required fields."""
        required_fields = ['stock_code', 'close_price', 'price_date']
        
        for field in required_fields:
            null_mask = df[field].isna()
            null_indices = df[null_mask].index.tolist()
            
            if null_indices:
                for idx in null_indices:
                    errors.append({
                        'index': idx,
                        'field': field,
                        'error': 'Missing required field',
                        'value': None
                    })
                    df.loc[idx, 'data_quality_flag'] = 'MISSING'
        
        return df
    
    def _validate_price_ranges(self, df: pd.DataFrame, warnings: List[Dict]) -> pd.DataFrame:
        """Validate price is within acceptable range."""
        # Check close_price
        invalid_price = (df['close_price'] < self.MIN_PRICE) | (df['close_price'] > self.MAX_PRICE)
        invalid_indices = df[invalid_price].index.tolist()
        
        for idx in invalid_indices:
            price = df.loc[idx, 'close_price']
            warnings.append({
                'index': idx,
                'field': 'close_price',
                'warning': f'Price outside normal range: {price}',
                'value': price
            })
            df.loc[idx, 'data_quality_flag'] = 'SUSPICIOUS'
        
        # Check OHLC consistency if available
        if all(col in df.columns for col in ['open_price', 'high_price', 'low_price']):
            ohlc_invalid = (
                (df['high_price'] < df['low_price']) |
                (df['high_price'] < df['close_price']) |
                (df['low_price'] > df['close_price'])
            )
            
            ohlc_invalid_indices = df[ohlc_invalid].index.tolist()
            for idx in ohlc_invalid_indices:
                warnings.append({
                    'index': idx,
                    'field': 'ohlc',
                    'warning': 'OHLC consistency violated',
                    'value': {
                        'open': df.loc[idx, 'open_price'],
                        'high': df.loc[idx, 'high_price'],
                        'low': df.loc[idx, 'low_price'],
                        'close': df.loc[idx, 'close_price']
                    }
                })
                df.loc[idx, 'data_quality_flag'] = 'SUSPICIOUS'
        
        return df
    
    def _validate_percentage_changes(self, df: pd.DataFrame, warnings: List[Dict]) -> pd.DataFrame:
        """Flag extreme percentage changes as suspicious."""
        if 'change_1d_pct' not in df.columns:
            return df
        
        # Only check rows with non-null values
        not_null_mask = df['change_1d_pct'].notna()
        extreme_mask = df.loc[not_null_mask, 'change_1d_pct'].abs() > self.MAX_DAILY_CHANGE_PCT
        extreme_indices = extreme_mask[extreme_mask].index.tolist()
        
        for idx in extreme_indices:
            change = df.loc[idx, 'change_1d_pct']
            warnings.append({
                'index': idx,
                'field': 'change_1d_pct',
                'warning': f'Extreme daily change: {change:.2f}%',
                'value': change
            })
            df.loc[idx, 'data_quality_flag'] = 'SUSPICIOUS'
        
        return df
    
    def _validate_exchanges(self, df: pd.DataFrame, errors: List[Dict]) -> pd.DataFrame:
        """Validate exchange is valid."""
        invalid_exchange = ~df['exchange'].isin(self.VALID_EXCHANGES)
        invalid_indices = df[invalid_exchange].index.tolist()
        
        for idx in invalid_indices:
            exchange = df.loc[idx, 'exchange']
            errors.append({
                'index': idx,
                'field': 'exchange',
                'error': f'Invalid exchange: {exchange}',
                'value': exchange
            })
            df.loc[idx, 'data_quality_flag'] = 'MISSING'
        
        return df
    
    def _validate_sectors(self, df: pd.DataFrame, warnings: List[Dict]) -> pd.DataFrame:
        """Validate sector is in known list."""
        if 'sector' not in df.columns or not self.valid_sectors:
            return df
        
        unknown_sector = ~df['sector'].isin(self.valid_sectors)
        unknown_indices = df[unknown_sector].index.tolist()
        
        for idx in unknown_indices:
            sector = df.loc[idx, 'sector']
            warnings.append({
                'index': idx,
                'field': 'sector',
                'warning': f'Unknown sector: {sector}',
                'value': sector
            })
            # Don't mark as suspicious - might be new sector
        
        return df
    
    def _check_duplicates(self, df: pd.DataFrame, errors: List[Dict]) -> pd.DataFrame:
        """Check for duplicate stock_code + price_date."""
        duplicates = df[df.duplicated(subset=['stock_code', 'price_date'], keep='first')]
        
        for idx in duplicates.index:
            errors.append({
                'index': idx,
                'field': 'duplicate',
                'error': 'Duplicate stock_code + price_date',
                'value': {
                    'stock_code': df.loc[idx, 'stock_code'],
                    'price_date': df.loc[idx, 'price_date']
                }
            })
        
        return df
