"""
Repository for stock (dim_stocks) operations.

Handles all database operations related to stock master data.
"""

from typing import Optional, List
from datetime import date
from sqlalchemy.orm import Session, joinedload
from sqlalchemy import and_, or_

from app.repositories.base import BaseRepository
from app.models import DimStock, DimSector
from app.utils.exceptions import RecordNotFoundError


class StockRepository(BaseRepository[DimStock]):
    """
    Repository for stock master data operations.
    
    Provides methods to query, create, and update stock information
    including sector relationships and exchange filtering.
    """
    
    def __init__(self, session: Session):
        """
        Initialize stock repository.
        
        Args:
            session: Active database session
        """
        super().__init__(DimStock, session)
    
    def get_by_code(self, stock_code: str) -> Optional[DimStock]:
        """
        Get stock by ticker symbol.
        
        Args:
            stock_code: Stock ticker (e.g., 'DANGCEM', 'GTCO')
            
        Returns:
            Stock instance or None if not found
        """
        return (
            self.session.query(DimStock)
            .filter(DimStock.stock_code == stock_code.upper())
            .first()
        )
    
    def get_by_code_or_raise(self, stock_code: str) -> DimStock:
        """
        Get stock by code or raise exception if not found.
        
        Args:
            stock_code: Stock ticker
            
        Returns:
            Stock instance
            
        Raises:
            RecordNotFoundError: If stock not found
        """
        stock = self.get_by_code(stock_code)
        if not stock:
            raise RecordNotFoundError(f"Stock not found: {stock_code}")
        return stock
    
    def get_all_active(self, exchange: Optional[str] = None) -> List[DimStock]:
        """
        Get all active (not delisted) stocks.
        
        Args:
            exchange: Filter by exchange ('NGX' or 'LSE'), None for all
            
        Returns:
            List of active stocks with sector information
        """
        query = (
            self.session.query(DimStock)
            .options(joinedload(DimStock.sector))
            .filter(DimStock.is_active == True)
        )
        
        if exchange:
            query = query.filter(DimStock.exchange == exchange.upper())
        
        return query.order_by(DimStock.stock_code).all()
    
    def get_by_sector(self, sector_name: str) -> List[DimStock]:
        """
        Get all stocks in a specific sector.
        
        Args:
            sector_name: Sector name (e.g., 'Financials')
            
        Returns:
            List of stocks in the sector
        """
        return (
            self.session.query(DimStock)
            .join(DimSector)
            .filter(
                and_(
                    DimSector.sector_name == sector_name,
                    DimStock.is_active == True
                )
            )
            .order_by(DimStock.stock_code)
            .all()
        )
    
    def get_by_exchange(self, exchange: str) -> List[DimStock]:
        """
        Get all active stocks for a specific exchange.
        
        Args:
            exchange: Exchange code ('NGX' or 'LSE')
            
        Returns:
            List of stocks on the exchange
        """
        return (
            self.session.query(DimStock)
            .filter(
                and_(
                    DimStock.exchange == exchange.upper(),
                    DimStock.is_active == True
                )
            )
            .order_by(DimStock.stock_code)
            .all()
        )
    
    def create_stock(
        self,
        stock_code: str,
        company_name: str,
        sector_id: int,
        exchange: str,
        listing_date: Optional[date] = None,
        metadata: Optional[dict] = None
    ) -> DimStock:
        """
        Create a new stock record.
        
        Args:
            stock_code: Ticker symbol (will be uppercased)
            company_name: Full company name
            sector_id: Reference to sector
            exchange: 'NGX' or 'LSE'
            listing_date: Date stock was listed
            metadata: Additional JSON metadata
            
        Returns:
            Created stock instance
        """
        return self.create(
            stock_code=stock_code.upper(),
            company_name=company_name,
            sector_id=sector_id,
            exchange=exchange.upper(),
            listing_date=listing_date,
            metadata=metadata,
            is_active=True
        )
    
    def delist_stock(self, stock_code: str, delisting_date: date) -> DimStock:
        """
        Mark a stock as delisted.
        
        Args:
            stock_code: Stock ticker
            delisting_date: Date of delisting
            
        Returns:
            Updated stock instance
            
        Raises:
            RecordNotFoundError: If stock not found
        """
        stock = self.get_by_code_or_raise(stock_code)
        return self.update(
            stock,
            is_active=False,
            delisting_date=delisting_date
        )
    
    def search_by_name(self, search_term: str) -> List[DimStock]:
        """
        Search stocks by company name (case-insensitive).
        
        Args:
            search_term: Partial company name to search
            
        Returns:
            List of matching stocks
        """
        return (
            self.session.query(DimStock)
            .filter(
                and_(
                    DimStock.company_name.ilike(f"%{search_term}%"),
                    DimStock.is_active == True
                )
            )
            .order_by(DimStock.company_name)
            .all()
        )
    
    def get_stock_codes(self, exchange: Optional[str] = None) -> List[str]:
        """
        Get list of all active stock codes.
        
        Args:
            exchange: Filter by exchange, None for all
            
        Returns:
            List of stock ticker symbols
        """
        query = (
            self.session.query(DimStock.stock_code)
            .filter(DimStock.is_active == True)
        )
        
        if exchange:
            query = query.filter(DimStock.exchange == exchange.upper())
        
        return [code for (code,) in query.order_by(DimStock.stock_code).all()]
    
    def update_metadata(self, stock_code: str, metadata: dict) -> DimStock:
        """
        Update stock metadata JSON field.
        
        Args:
            stock_code: Stock ticker
            metadata: New metadata dictionary
            
        Returns:
            Updated stock instance
        """
        stock = self.get_by_code_or_raise(stock_code)
        return self.update(stock, metadata=metadata)
