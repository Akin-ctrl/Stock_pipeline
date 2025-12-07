"""
Fact table models (transactional/time-series data).

Follows reference.py principles:
- Type hints
- Data validation
- Business logic methods
"""

from datetime import datetime, date
from typing import Optional
from decimal import Decimal
from sqlalchemy import (
    Column, BigInteger, Integer, String, Date, 
    Boolean, Text, TIMESTAMP, CheckConstraint, 
    ForeignKey, Index, BIGINT, Numeric
)
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func

from app.models.base import Base, TimestampMixin


class FactDailyPrice(Base, TimestampMixin):
    """
    Daily stock price fact table (time-series).
    
    Stores OHLCV (Open, High, Low, Close, Volume) data for each stock per day.
    
    Attributes:
        price_id: Primary key
        stock_id: Foreign key to dim_stocks
        price_date: Trading date
        open_price: Opening price
        high_price: Highest price
        low_price: Lowest price
        close_price: Closing price (required)
        volume: Trading volume
        change_1d_pct: Daily percentage change
        change_ytd_pct: Year-to-date percentage change
        market_cap: Market capitalization as string
        source: Data source identifier
        data_quality_flag: Quality indicator ('GOOD', 'SUSPICIOUS', 'MISSING', 'STALE')
        has_complete_data: Whether all OHLCV fields are present
        ingestion_timestamp: When data was ingested
    
    Example:
        >>> price = FactDailyPrice(
        >>>     stock_id=1,
        >>>     price_date=date(2025, 12, 6),
        >>>     close_price=Decimal('35.50'),
        >>>     volume=1500000
        >>> )
    """
    __tablename__ = 'fact_daily_prices'
    
    price_id = Column(BigInteger, primary_key=True, autoincrement=True)
    stock_id = Column(Integer, ForeignKey('dim_stocks.stock_id', ondelete='CASCADE'), nullable=False)
    price_date = Column(Date, nullable=False, index=True)
    
    # OHLCV Data
    open_price = Column(Numeric(18, 4))
    high_price = Column(Numeric(18, 4))
    low_price = Column(Numeric(18, 4))
    close_price = Column(Numeric(18, 4), nullable=False)
    volume = Column(BIGINT)
    
    # Calculated Fields
    change_1d_pct = Column(Numeric(10, 4))
    change_ytd_pct = Column(Numeric(10, 4))
    market_cap = Column(String(50))
    
    # Metadata
    source = Column(String(50), nullable=False)
    data_quality_flag = Column(String(20), default='GOOD')
    has_complete_data = Column(Boolean, default=True)
    ingestion_timestamp = Column(TIMESTAMP, server_default=func.now())
    
    # Constraints
    __table_args__ = (
        CheckConstraint('close_price > 0', name='chk_price_positive'),
        CheckConstraint(
            "high_price IS NULL OR low_price IS NULL OR "
            "(high_price >= low_price AND high_price >= close_price AND low_price <= close_price)",
            name='chk_ohlc_logic'
        ),
        CheckConstraint(
            "data_quality_flag IN ('GOOD', 'SUSPICIOUS', 'MISSING', 'STALE')",
            name='chk_data_quality'
        ),
        Index('idx_stock_date', 'stock_id', 'price_date', unique=True),
    )
    
    # Relationships
    stock = relationship("DimStock", back_populates="prices")
    
    def __repr__(self) -> str:
        return (
            f"FactDailyPrice(id={self.price_id}, stock_id={self.stock_id}, "
            f"date={self.price_date}, close={self.close_price})"
        )
    
    @property
    def is_significant_move(self) -> bool:
        """Check if daily price change is significant (>4%)."""
        if self.change_1d_pct is None:
            return False
        return abs(float(self.change_1d_pct)) >= 4.0
    
    @property
    def is_suspicious(self) -> bool:
        """Check if data quality is flagged as suspicious."""
        return self.data_quality_flag in ('SUSPICIOUS', 'MISSING', 'STALE')
    
    def validate_ohlc(self) -> bool:
        """
        Validate OHLC logic: high >= low, close within range.
        
        Returns:
            True if OHLC data is valid or incomplete, False if invalid
        """
        if None in (self.open_price, self.high_price, self.low_price, self.close_price):
            return True  # Incomplete data, can't validate
        
        return (
            float(self.high_price) >= float(self.low_price) and
            float(self.high_price) >= float(self.close_price) and
            float(self.low_price) <= float(self.close_price)
        )


class FactTechnicalIndicator(Base, TimestampMixin):
    """
    Technical indicators fact table (computed metrics).
    
    Stores calculated technical analysis indicators.
    
    Attributes:
        indicator_id: Primary key
        stock_id: Foreign key to dim_stocks
        calculation_date: Date indicators were calculated
        ma_7: 7-day moving average
        ma_30: 30-day moving average
        ma_90: 90-day moving average
        rsi_14: 14-day Relative Strength Index
        macd: MACD line
        macd_signal: MACD signal line
        macd_histogram: MACD histogram
        volatility_30: 30-day volatility
        atr_14: 14-day Average True Range
        bollinger_upper: Upper Bollinger Band
        bollinger_middle: Middle Bollinger Band
        bollinger_lower: Lower Bollinger Band
        ma_crossover_signal: Crossover signal ('BULLISH', 'BEARISH', 'NEUTRAL')
        trend_strength: Trend strength score (0-100)
        created_at: Calculation timestamp
    
    Example:
        >>> indicator = FactTechnicalIndicator(
        >>>     stock_id=1,
        >>>     calculation_date=date(2025, 12, 6),
        >>>     ma_7=Decimal('35.20'),
        >>>     ma_30=Decimal('34.50'),
        >>>     rsi_14=Decimal('65.5')
        >>> )
    """
    __tablename__ = 'fact_technical_indicators'
    
    indicator_id = Column(BigInteger, primary_key=True, autoincrement=True)
    stock_id = Column(Integer, ForeignKey('dim_stocks.stock_id', ondelete='CASCADE'), nullable=False)
    calculation_date = Column(Date, nullable=False, index=True)
    
    # Moving Averages
    ma_7 = Column(Numeric(18, 4))
    ma_30 = Column(Numeric(18, 4))
    ma_90 = Column(Numeric(18, 4))
    
    # Momentum Indicators
    rsi_14 = Column(Numeric(5, 2))
    macd = Column(Numeric(18, 4))
    macd_signal = Column(Numeric(18, 4))
    macd_histogram = Column(Numeric(18, 4))
    
    # Volatility Indicators
    volatility_30 = Column(Numeric(10, 4))
    atr_14 = Column(Numeric(18, 4))
    
    # Bollinger Bands
    bollinger_upper = Column(Numeric(18, 4))
    bollinger_middle = Column(Numeric(18, 4))
    bollinger_lower = Column(Numeric(18, 4))
    
    # Trading Signals
    ma_crossover_signal = Column(String(10))
    trend_strength = Column(Numeric(5, 2))
    
    created_at = Column(TIMESTAMP, server_default=func.now())
    
    # Constraints
    __table_args__ = (
        CheckConstraint(
            'rsi_14 IS NULL OR rsi_14 BETWEEN 0 AND 100',
            name='chk_rsi_range'
        ),
        CheckConstraint(
            'trend_strength IS NULL OR trend_strength BETWEEN 0 AND 100',
            name='chk_trend_range'
        ),
        CheckConstraint(
            "ma_crossover_signal IS NULL OR ma_crossover_signal IN ('BULLISH', 'BEARISH', 'NEUTRAL')",
            name='chk_ma_crossover'
        ),
        Index('idx_stock_calc_date', 'stock_id', 'calculation_date', unique=True),
    )
    
    # Relationships
    stock = relationship("DimStock", back_populates="indicators")
    
    def __repr__(self) -> str:
        return (
            f"FactTechnicalIndicator(id={self.indicator_id}, stock_id={self.stock_id}, "
            f"date={self.calculation_date}, rsi={self.rsi_14})"
        )
    
    @property
    def is_bullish_crossover(self) -> bool:
        """Check if MA crossover signal is bullish."""
        return self.ma_crossover_signal == 'BULLISH'
    
    @property
    def is_bearish_crossover(self) -> bool:
        """Check if MA crossover signal is bearish."""
        return self.ma_crossover_signal == 'BEARISH'
    
    @property
    def is_oversold(self) -> bool:
        """Check if RSI indicates oversold condition (<30)."""
        return self.rsi_14 is not None and float(self.rsi_14) < 30.0
    
    @property
    def is_overbought(self) -> bool:
        """Check if RSI indicates overbought condition (>70)."""
        return self.rsi_14 is not None and float(self.rsi_14) > 70.0
    
    def get_signal_strength(self) -> str:
        """
        Get overall signal strength based on indicators.
        
        Returns:
            'STRONG_BUY', 'BUY', 'HOLD', 'SELL', 'STRONG_SELL'
        """
        if self.trend_strength is None:
            return 'HOLD'
        
        strength = float(self.trend_strength)
        
        if strength >= 80:
            return 'STRONG_BUY'
        elif strength >= 60:
            return 'BUY'
        elif strength >= 40:
            return 'HOLD'
        elif strength >= 20:
            return 'SELL'
        else:
            return 'STRONG_SELL'
