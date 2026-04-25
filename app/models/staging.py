"""
Staging table models for source-ingestion and reconciliation workflows.

The current live pipeline loads source observations into staging before
reconciliation and promotion to production tables. The staging layer is the
foundation for current single-source operation and future multi-source support.

Features:
- source tracking
- reconciliation status
- audit trail
- promotion lineage
"""

from datetime import datetime, date
from typing import Optional
from decimal import Decimal
from sqlalchemy import (
    Column, BigInteger, Integer, String, Date, 
    Boolean, Text, TIMESTAMP, Numeric, ARRAY,
    CheckConstraint, Index
)
from sqlalchemy.sql import func

from app.models.base import Base, TimestampMixin


class StagingDailyPrice(Base, TimestampMixin):
    """
    Staging table for daily stock prices prior to promotion.
    
    Holds source-ingested data before reconciliation and promotion to fact_daily_prices.
    Retains source information and reconciliation status for audit purposes.
    
    Attributes:
        staging_id: Primary key
        stock_code: Stock ticker symbol (e.g., 'DANGCEM', 'ZENITH')
        source: Data source identifier
        price_date: Trading date
        close_price: Closing price (required)
        change_1d_pct: Daily percentage change (nullable)
        change_ytd_pct: Year-to-date percentage change (nullable)
        volume: Trading volume (nullable)
        loaded_at: When data was loaded to staging
        reconciled: Whether this record has been reconciled
        promoted_at: When data was promoted to production (nullable)
        reconciliation_notes: Notes from reconciliation process (JSON)
        
    Example:
        >>> staging = StagingDailyPrice(
        >>>     stock_code='DANGCEM',
        >>>     source='afrimarket',
        >>>     price_date=date(2025, 12, 30),
        >>>     close_price=Decimal('350.50'),
        >>>     loaded_at=datetime.now(),
        >>>     reconciled=False
        >>> )
    """
    
    __tablename__ = 'staging_daily_prices'
    
    # Primary Key
    staging_id = Column(BigInteger, primary_key=True, autoincrement=True)
    
    # Stock Identification
    stock_code = Column(String(20), nullable=False, index=True, 
                       comment='Stock ticker symbol from source')
    
    # Source Tracking
    source = Column(String(50), nullable=False, index=True,
                   comment='Data source identifier')
    
    # Price Data
    price_date = Column(Date, nullable=False, index=True, 
                       comment='Trading date')
    close_price = Column(Numeric(15, 4), nullable=False,
                        comment='Closing price')
    change_1d_pct = Column(Numeric(8, 4), nullable=True,
                          comment='Daily percentage change')
    change_ytd_pct = Column(Numeric(8, 4), nullable=True,
                           comment='Year-to-date percentage change')
    volume = Column(BigInteger, nullable=True,
                   comment='Trading volume')
    
    # Timestamps
    loaded_at = Column(TIMESTAMP, nullable=False, server_default=func.now(),
                      comment='When data was loaded to staging')
    
    # Reconciliation Status
    reconciled = Column(Boolean, nullable=False, default=False, index=True,
                       comment='Whether this record has been reconciled')
    promoted_at = Column(TIMESTAMP, nullable=True,
                        comment='When data was promoted to production')
    reconciliation_notes = Column(Text, nullable=True,
                                 comment='Notes from reconciliation (JSON)')
    
    # Constraints
    __table_args__ = (
        # Unique constraint: one record per stock/date/source
        Index('idx_staging_stock_date_source', 'stock_code', 'price_date', 'source', unique=True),
        
        # Performance indexes
        Index('idx_staging_reconciled_date', 'reconciled', 'price_date'),
        Index('idx_staging_promoted', 'promoted_at'),
        
        # Data validation
        CheckConstraint('close_price > 0', name='ck_staging_positive_price'),
    )
    
    def __repr__(self) -> str:
        return (f"<StagingDailyPrice("
                f"stock_code={self.stock_code}, "
                f"date={self.price_date}, "
                f"source={self.source}, "
                f"price={self.close_price}, "
                f"reconciled={self.reconciled})>")
    
    def to_dict(self) -> dict:
        """Convert model to dictionary."""
        return {
            'staging_id': self.staging_id,
            'stock_code': self.stock_code,
            'source': self.source,
            'price_date': self.price_date.isoformat() if self.price_date else None,
            'close_price': float(self.close_price) if self.close_price is not None else None,
            'change_1d_pct': float(self.change_1d_pct) if self.change_1d_pct is not None else None,
            'change_ytd_pct': float(self.change_ytd_pct) if self.change_ytd_pct is not None else None,
            'volume': self.volume,
            'loaded_at': self.loaded_at.isoformat() if self.loaded_at else None,
            'reconciled': self.reconciled,
            'promoted_at': self.promoted_at.isoformat() if self.promoted_at else None,
            'reconciliation_notes': self.reconciliation_notes,
        }


class StagingAuditLog(Base):
    """
    Audit log for staging data reconciliation process.
    
    Tracks all reconciliation decisions, conflicts, and resolutions.
    Enables debugging and analysis of data quality issues.
    
    Attributes:
        audit_id: Primary key
        stock_code: Stock ticker symbol
        price_date: Trading date
        sources: Array of sources involved (e.g., ['afrimarket'])
        prices: Array of prices from each source
        resolution_method: How conflict was resolved ('average', 'prefer_source', 'manual')
        selected_price: Final price selected after reconciliation
        selected_source: Source of final price (if preferred)
        conflict_severity: Severity level ('low', 'medium', 'high', 'critical')
        notes: Additional reconciliation notes
        created_at: Timestamp of reconciliation
        
    Example:
        >>> audit = StagingAuditLog(
        >>>     stock_code='DANGCEM',
        >>>     price_date=date(2025, 12, 30),
        >>>     sources=['afrimarket'],
        >>>     prices=[Decimal('350.00'), Decimal('351.50')],
        >>>     resolution_method='average',
        >>>     selected_price=Decimal('350.75'),
        >>>     conflict_severity='low'
        >>> )
    """
    
    __tablename__ = 'staging_audit_log'
    
    # Primary Key
    audit_id = Column(BigInteger, primary_key=True, autoincrement=True)
    
    # Stock Identification
    stock_code = Column(String(20), nullable=False, index=True,
                       comment='Stock ticker symbol')
    price_date = Column(Date, nullable=False, index=True,
                       comment='Trading date')
    
    # Source Data
    sources = Column(ARRAY(String), nullable=False,
                    comment='Array of data sources involved')
    prices = Column(ARRAY(Numeric), nullable=False,
                   comment='Array of prices from each source')
    
    # Resolution
    resolution_method = Column(String(50), nullable=False,
                              comment='How conflict was resolved')
    selected_price = Column(Numeric(15, 4), nullable=True,
                           comment='Final price after reconciliation')
    selected_source = Column(String(50), nullable=True,
                            comment='Source of final price (if preferred)')
    
    # Severity Classification
    conflict_severity = Column(String(20), nullable=False, index=True,
                              comment='Severity: low, medium, high, critical')
    
    # Notes
    notes = Column(Text, nullable=True,
                  comment='Additional reconciliation notes')
    
    # Timestamp
    created_at = Column(TIMESTAMP, nullable=False, server_default=func.now(),
                       index=True, comment='Reconciliation timestamp')
    
    # Constraints
    __table_args__ = (
        # Performance indexes
        Index('idx_audit_date_severity', 'price_date', 'conflict_severity'),
        Index('idx_audit_stock_date', 'stock_code', 'price_date'),
        
        # Data validation
        CheckConstraint(
            "resolution_method IN ('average', 'prefer_source', 'manual', 'single_source')",
            name='ck_audit_valid_resolution'
        ),
        CheckConstraint(
            "conflict_severity IN ('low', 'medium', 'high', 'critical')",
            name='ck_audit_valid_severity'
        ),
    )
    
    def __repr__(self) -> str:
        return (f"<StagingAuditLog("
                f"stock_code={self.stock_code}, "
                f"date={self.price_date}, "
                f"severity={self.conflict_severity})>")
    
    def to_dict(self) -> dict:
        """Convert model to dictionary."""
        return {
            'audit_id': self.audit_id,
            'stock_code': self.stock_code,
            'price_date': self.price_date.isoformat() if self.price_date else None,
            'sources': self.sources,
            'prices': [float(p) for p in self.prices] if self.prices else [],
            'resolution_method': self.resolution_method,
            'selected_price': float(self.selected_price) if self.selected_price else None,
            'selected_source': self.selected_source,
            'conflict_severity': self.conflict_severity,
            'notes': self.notes,
            'created_at': self.created_at.isoformat() if self.created_at else None,
        }
