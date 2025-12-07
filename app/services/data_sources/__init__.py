"""
Data source implementations for fetching stock data.

Provides adapters for different data sources (NGX web scraping, Yahoo Finance API).
"""

from app.services.data_sources.base import DataSource
from app.services.data_sources.ngx_source import NGXDataSource
from app.services.data_sources.yahoo_source import YahooDataSource

__all__ = [
    'DataSource',
    'NGXDataSource',
    'YahooDataSource',
]
