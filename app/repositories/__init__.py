"""
Repository layer for database access.

This module provides the data access layer using the Repository pattern,
separating database operations from business logic.
"""

from app.repositories.base import BaseRepository
from app.repositories.stock_repository import StockRepository
from app.repositories.price_repository import PriceRepository
from app.repositories.indicator_repository import IndicatorRepository
from app.repositories.alert_repository import AlertRepository

__all__ = [
    'BaseRepository',
    'StockRepository',
    'PriceRepository',
    'IndicatorRepository',
    'AlertRepository',
]
