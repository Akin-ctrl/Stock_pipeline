"""
Data source implementations for fetching stock data.

Provides adapters for different data sources (NGX web scraping).
"""

from app.services.data_sources.base import DataSource
from app.services.data_sources.ngx_source import NGXDataSource

__all__ = [
    'DataSource',
    'NGXDataSource',
]
