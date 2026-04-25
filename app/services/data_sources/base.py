"""
Base class for data sources.

Defines the interface that all data sources must implement.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Optional
import pandas as pd
from datetime import date

from app.utils import get_logger


class DataSource(ABC):
    """
    Abstract base class for stock data sources.
    
    All data sources must implement the fetch() method that returns
    a standardized pandas DataFrame with stock price information.
    """
    
    def __init__(self, source_name: str):
        """
        Initialize data source.
        
        Args:
            source_name: Identifier for this data source
        """
        self.source_name = source_name
        self.logger = get_logger(f"datasource.{source_name}")

    def get_capabilities(self) -> "SourceCapabilities":
        """Describe the fields and characteristics this adapter can provide."""
        return SourceCapabilities(source_name=self.source_name)
    
    @abstractmethod
    def fetch(
        self,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None
    ) -> pd.DataFrame:
        """
        Fetch stock data from the source.
        
        Args:
            start_date: Start date for historical data (optional)
            end_date: End date for historical data (optional)
            
        Returns:
            DataFrame with standardized columns.

            Required in the current pipeline:
                - stock_code
                - company_name
                - exchange
                - price_date
                - close_price

            Common optional fields:
                - sector
                - volume
                - change_1d_pct
                - change_ytd_pct

            The base interface still allows richer market fields, but the live
            NGX pipeline currently operates primarily on close-price-centric data.
                
        Raises:
            DataFetchError: If fetching data fails
            DataParseError: If parsing data fails
        """
        pass
    
    def validate_dataframe(self, df: pd.DataFrame) -> bool:
        """
        Validate that DataFrame has required columns.
        
        Args:
            df: DataFrame to validate
            
        Returns:
            True if valid
            
        Raises:
            ValueError: If required columns missing
        """
        required_columns = [
            'stock_code',
            'company_name',
            'exchange',
            'price_date',
            'close_price'
        ]
        
        missing = [col for col in required_columns if col not in df.columns]
        
        if missing:
            raise ValueError(f"Missing required columns: {missing}")
        
        return True
    
    def log_fetch_summary(self, df: pd.DataFrame, duration: float) -> None:
        """
        Log summary of fetch operation.
        
        Args:
            df: Fetched DataFrame
            duration: Time taken in seconds
        """
        self.logger.info(
            f"Fetched data from {self.source_name}",
            extra={
                "records": len(df),
                "duration_seconds": round(duration, 2),
                "date_range": f"{df['price_date'].min()} to {df['price_date'].max()}" if len(df) > 0 else "N/A"
            }
        )


@dataclass(frozen=True)
class SourceCapabilities:
    """Declared capabilities for a market-data adapter."""

    source_name: str
    has_current_quotes: bool = True
    has_historical_eod: bool = False
    has_volume: bool = False
    has_change_1d_pct: bool = False
    has_change_ytd_pct: bool = False
    has_official_eod: bool = False
    has_documents: bool = False
