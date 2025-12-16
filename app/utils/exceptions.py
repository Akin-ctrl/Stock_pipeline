"""
Custom exceptions for the Stock Pipeline system.

Follows reference.py principles:
- Clear error messages
- Proper inheritance hierarchy
- Type safety
"""

from typing import Any, Optional, Dict


class StockPipelineError(Exception):
    """Base exception for all stock pipeline errors."""
    
    def __init__(self, message: str, details: Optional[Dict[str, Any]] = None):
        """
        Initialize exception with message and optional details.
        
        Args:
            message: Human-readable error description
            details: Additional context (e.g., stock_code, timestamp)
        """
        super().__init__(message)
        self.message = message
        self.details = details or {}
    
    def __str__(self) -> str:
        if self.details:
            details_str = ", ".join(f"{k}={v}" for k, v in self.details.items())
            return f"{self.message} ({details_str})"
        return self.message


# Data Source Exceptions
class DataSourceError(StockPipelineError):
    """Raised when data source operations fail."""
    pass


class DataFetchError(DataSourceError):
    """Raised when fetching data from external source fails."""
    pass


class DataParseError(DataSourceError):
    """Raised when parsing fetched data fails."""
    pass


# Data Validation Exceptions
class DataValidationError(StockPipelineError):
    """Raised when data validation fails."""
    pass


class InvalidPriceError(DataValidationError):
    """Raised when price data is invalid."""
    pass


class MissingDataError(DataValidationError):
    """Raised when required data is missing."""
    pass


class DuplicateDataError(DataValidationError):
    """Raised when duplicate data is detected."""
    pass


# Database Exceptions
class DatabaseError(StockPipelineError):
    """Raised when database operations fail."""
    pass


class RecordNotFoundError(DatabaseError):
    """Raised when a requested record doesn't exist."""
    pass


class ConnectionError(DatabaseError):
    """Raised when database connection fails."""
    pass


# Processing Exceptions
class ProcessingError(StockPipelineError):
    """Raised when data processing fails."""
    pass


class CalculationError(ProcessingError):
    """Raised when indicator calculation fails."""
    pass


# Configuration Exceptions
class ConfigurationError(StockPipelineError):
    """Raised when configuration is invalid."""
    pass


class MissingConfigError(ConfigurationError):
    """Raised when required configuration is missing."""
    pass
