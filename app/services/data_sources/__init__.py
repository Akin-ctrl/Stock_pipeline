"""
Data source implementations for fetching stock data.

Provides adapters for different data sources (Afrimarket API).
"""

from app.services.data_sources.base import DataSource, SourceCapabilities
from app.services.data_sources.afrimarket_source import AfrimarketDataSource

__all__ = [
    'DataSource',
    'SourceCapabilities',
    'AfrimarketDataSource',
]
