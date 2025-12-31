"""
Dimension table models (master data).
Features:
- Type hints
- Comprehensive docstrings
- Business logic methods
"""

from datetime import datetime
from typing import List, Optional
from sqlalchemy import (
    Column, Integer, String, Date, Boolean, 
    Text, TIMESTAMP, CheckConstraint, ForeignKey
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func

from app.models.base import Base, TimestampMixin


class DimSector(Base, TimestampMixin):
    """
    Sector dimension table (master data).
    
    Stores stock market sectors like Financials, Technology, etc.
    
    Attributes:
        sector_id: Primary key
        sector_name: Unique sector name
        description: Detailed sector description
        created_at: Record creation timestamp
        updated_at: Last update timestamp
        stocks: Related stocks (one-to-many relationship)
    
    Example:
        >>> sector = DimSector(
        >>>     sector_name='Financials',
        >>>     description='Banks, Insurance, Asset Management'
        >>> )
    """
    __tablename__ = 'dim_sectors'
    
    sector_id = Column(Integer, primary_key=True, autoincrement=True)
    sector_name = Column(String(100), unique=True, nullable=False, index=True)
    description = Column(Text)
    created_at = Column(TIMESTAMP, server_default=func.now(), nullable=False)
    updated_at = Column(TIMESTAMP, server_default=func.now(), onupdate=func.now())
    
    # Relationships
    stocks = relationship("DimStock", back_populates="sector")
    
    def __repr__(self) -> str:
        return f"DimSector(id={self.sector_id}, name={self.sector_name!r})"
    
    @property
    def stock_count(self) -> int:
        """Get number of stocks in this sector."""
        return len(self.stocks) if self.stocks else 0


class DimStock(Base, TimestampMixin):
    """
    Stock dimension table (master data).
    
    Stores master information about each stock/company.
    
    Attributes:
        stock_id: Primary key
        stock_code: Unique ticker symbol (e.g., 'DANGCEM', 'GTCO')
        company_name: Full company name
        sector_id: Foreign key to dim_sectors
        exchange: Exchange name ('NGX' or 'LSE')
        listing_date: Date stock was listed
        delisting_date: Date stock was delisted (NULL if active)
        is_active: Whether stock is currently tradeable
        metadata: Additional information as JSON
        created_at: Record creation timestamp
        updated_at: Last update timestamp
    
    Relationships:
        sector: Many-to-one with DimSector
        prices: One-to-many with FactDailyPrice
        indicators: One-to-many with FactTechnicalIndicator
        alerts: One-to-many with AlertHistory
    
    Example:
        >>> stock = DimStock(
        >>>     stock_code='DANGCEM',
        >>>     company_name='Dangote Cement PLC',
        >>>     exchange='NGX',
        >>>     is_active=True
        >>> )
    """
    __tablename__ = 'dim_stocks'
    
    stock_id = Column(Integer, primary_key=True, autoincrement=True)
    stock_code = Column(String(20), unique=True, nullable=False, index=True)
    company_name = Column(String(255), nullable=False)
    sector_id = Column(Integer, ForeignKey('dim_sectors.sector_id', ondelete='SET NULL'))
    exchange = Column(String(10), nullable=False, index=True)
    listing_date = Column(Date)
    delisting_date = Column(Date)
    is_active = Column(Boolean, default=True, nullable=False, index=True)
    stock_metadata = Column('metadata', JSONB)  # Renamed to avoid SQLAlchemy conflict
    created_at = Column(TIMESTAMP, server_default=func.now(), nullable=False)
    updated_at = Column(TIMESTAMP, server_default=func.now(), onupdate=func.now())
    
    # Constraints
    __table_args__ = (
        CheckConstraint("exchange IN ('NGX', 'LSE')", name='chk_exchange'),
        CheckConstraint("stock_code ~ '^[A-Z0-9._]+$'", name='chk_stock_code_format'),
    )
    
    # Relationships
    sector = relationship("DimSector", back_populates="stocks")
    prices = relationship("FactDailyPrice", back_populates="stock", cascade="all, delete-orphan")
    indicators = relationship("FactTechnicalIndicator", back_populates="stock", cascade="all, delete-orphan")
    alerts = relationship("AlertHistory", back_populates="stock", cascade="all, delete-orphan")
    recommendations = relationship("FactRecommendation", back_populates="stock", cascade="all, delete-orphan")
    
    def __repr__(self) -> str:
        return (
            f"DimStock(id={self.stock_id}, code={self.stock_code!r}, "
            f"company={self.company_name!r}, exchange={self.exchange!r})"
        )
    
    @property
    def is_nigerian(self) -> bool:
        """Check if stock is Nigerian (NGX or LSE-listed Nigerian company)."""
        return self.exchange == 'NGX' or (
            self.exchange == 'LSE' and 
            self.stock_metadata and 
            self.stock_metadata.get('country') == 'Nigeria'
        )
    
    @property
    def display_name(self) -> str:
        """Get display name for UI."""
        return f"{self.stock_code} - {self.company_name}"
    
    def deactivate(self, delisting_date: Optional[datetime] = None) -> None:
        """
        Mark stock as inactive (delisted).
        
        Args:
            delisting_date: Date of delisting (defaults to today)
        """
        self.is_active = False
        self.delisting_date = delisting_date or datetime.now().date()
    
    def activate(self) -> None:
        """Mark stock as active (relisted)."""
        self.is_active = True
        self.delisting_date = None
