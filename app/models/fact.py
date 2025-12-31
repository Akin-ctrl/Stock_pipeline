"""
Fact table models (transactional/time-series data).
Features:
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
    
    Stores daily closing price and derived metrics for each stock.
    Optimized for NGX data which provides close price, daily/YTD changes, and market cap.
    
    Attributes:
        price_id: Primary key
        stock_id: Foreign key to dim_stocks
        price_date: Trading date
        close_price: Closing price (required)
        change_1d_pct: Daily percentage change
        change_ytd_pct: Year-to-date percentage change
        market_cap: Market capitalization as string (billions NGN)
        source: Data source identifier (ngx, yf)
        data_quality_flag: Quality indicator ('GOOD', 'INCOMPLETE', 'SUSPICIOUS', 'MISSING', 'STALE', 'POOR')
        has_complete_data: Whether all expected fields are present
        ingestion_timestamp: When data was ingested
    
    Example:
        >>> price = FactDailyPrice(
        >>>     stock_id=1,
        >>>     price_date=date(2025, 12, 30),
        >>>     close_price=Decimal('35.50'),
        >>>     change_1d_pct=Decimal('2.5'),
        >>>     change_ytd_pct=Decimal('15.3'),
        >>>     market_cap='450.5'
        >>> )
    """
    __tablename__ = 'fact_daily_prices'
    
    price_id = Column(BigInteger, primary_key=True, autoincrement=True)
    stock_id = Column(Integer, ForeignKey('dim_stocks.stock_id', ondelete='CASCADE'), nullable=False)
    price_date = Column(Date, nullable=False, index=True)
    
    # Price Data (NGX provides only close price)
    close_price = Column(Numeric(18, 4), nullable=False)
    
    # Calculated Fields (from source)
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
            "data_quality_flag IN ('GOOD', 'INCOMPLETE', 'SUSPICIOUS', 'MISSING', 'STALE', 'POOR')",
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


class FactRecommendation(Base, TimestampMixin):
    """
    Investment recommendation fact table.
    
    Stores AI-generated stock recommendations with confidence scores,
    target prices, and reasoning.
    
    Attributes:
        recommendation_id: Primary key
        stock_id: Foreign key to dim_stocks
        recommendation_date: Date of recommendation
        signal_type: Trading signal (STRONG_BUY, BUY, HOLD, SELL, STRONG_SELL)
        confidence_score: Confidence percentage (0-100)
        overall_score: Overall stock score (0-100)
        score_category: Score category (EXCELLENT, GOOD, FAIR, POOR, VERY_POOR)
        current_price: Stock price at recommendation time
        target_price: Estimated target price
        stop_loss: Recommended stop-loss price
        potential_return_pct: Expected return percentage
        risk_level: Risk assessment (LOW, MEDIUM, HIGH)
        recommendation_reason: Primary recommendation reason
        technical_score: Technical analysis score (0-100)
        momentum_score: Momentum score (0-100)
        volatility_score: Volatility score (0-100)
        trend_score: Trend score (0-100)
        volume_score: Volume score (0-100)
        rsi_value: RSI value at recommendation time
        macd_value: MACD value at recommendation time
        is_active: Whether recommendation is still active
        outcome: Actual outcome (HIT_TARGET, HIT_STOP_LOSS, ONGOING, EXPIRED)
        outcome_date: Date outcome was determined
        actual_return_pct: Actual return if closed
    
    Indexes:
        - (stock_id, recommendation_date) - for historical lookup
        - (recommendation_date) - for date range queries
        - (signal_type) - for filtering by recommendation type
        - (is_active) - for active recommendations
    """
    __tablename__ = 'fact_recommendations'
    
    recommendation_id = Column(BigInteger, primary_key=True, autoincrement=True)
    stock_id = Column(Integer, ForeignKey('dim_stocks.stock_id'), nullable=False)
    recommendation_date = Column(Date, nullable=False, index=True)
    
    # Signal and scoring
    signal_type = Column(
        String(20),
        nullable=False,
        index=True,
        comment="Trading signal: STRONG_BUY, BUY, HOLD, SELL, STRONG_SELL"
    )
    confidence_score = Column(
        Numeric(5, 2),
        nullable=False,
        comment="Confidence percentage (0-100)"
    )
    overall_score = Column(
        Numeric(5, 2),
        nullable=False,
        comment="Overall stock score (0-100)"
    )
    score_category = Column(
        String(20),
        nullable=False,
        comment="EXCELLENT, GOOD, FAIR, POOR, VERY_POOR"
    )
    
    # Price targets
    current_price = Column(Numeric(12, 2), nullable=False)
    target_price = Column(Numeric(12, 2), nullable=True)
    stop_loss = Column(Numeric(12, 2), nullable=True)
    potential_return_pct = Column(Numeric(7, 2), nullable=True)
    
    # Risk and reasoning
    risk_level = Column(
        String(10),
        nullable=False,
        comment="LOW, MEDIUM, HIGH"
    )
    recommendation_reason = Column(Text, nullable=True)
    
    # Score breakdown
    technical_score = Column(Numeric(5, 2), nullable=True)
    momentum_score = Column(Numeric(5, 2), nullable=True)
    volatility_score = Column(Numeric(5, 2), nullable=True)
    trend_score = Column(Numeric(5, 2), nullable=True)
    volume_score = Column(Numeric(5, 2), nullable=True)
    
    # Indicator values at time of recommendation
    rsi_value = Column(Numeric(5, 2), nullable=True)
    macd_value = Column(Numeric(10, 4), nullable=True)
    
    # Outcome tracking
    is_active = Column(Boolean, default=True, nullable=False, index=True)
    outcome = Column(
        String(20),
        nullable=True,
        comment="HIT_TARGET, HIT_STOP_LOSS, ONGOING, EXPIRED"
    )
    outcome_date = Column(Date, nullable=True)
    actual_return_pct = Column(Numeric(7, 2), nullable=True)
    
    # Relationships
    stock = relationship("DimStock", back_populates="recommendations")
    
    # Constraints
    __table_args__ = (
        CheckConstraint(
            "signal_type IN ('STRONG_BUY', 'BUY', 'HOLD', 'SELL', 'STRONG_SELL')",
            name='chk_signal_type'
        ),
        CheckConstraint(
            "risk_level IN ('LOW', 'MEDIUM', 'HIGH')",
            name='chk_risk_level'
        ),
        CheckConstraint(
            'confidence_score >= 0 AND confidence_score <= 100',
            name='chk_confidence_score'
        ),
        CheckConstraint(
            'overall_score >= 0 AND overall_score <= 100',
            name='chk_overall_score'
        ),
        Index('ix_recommendation_stock_date', 'stock_id', 'recommendation_date'),
    )
    
    def __repr__(self) -> str:
        """String representation."""
        return (
            f"<FactRecommendation(id={self.recommendation_id}, "
            f"stock_id={self.stock_id}, date={self.recommendation_date}, "
            f"signal={self.signal_type}, score={self.overall_score})>"
        )
    
    def is_buy_signal(self) -> bool:
        """Check if recommendation is a buy signal."""
        return self.signal_type in ('BUY', 'STRONG_BUY')
    
    def is_sell_signal(self) -> bool:
        """Check if recommendation is a sell signal."""
        return self.signal_type in ('SELL', 'STRONG_SELL')
    
    def get_days_active(self) -> Optional[int]:
        """Get number of days recommendation has been active."""
        if not self.is_active or self.outcome_date is None:
            return None
        return (self.outcome_date - self.recommendation_date).days
    
    def calculate_actual_return(self, current_price: Decimal) -> Decimal:
        """
        Calculate actual return based on current price.
        
        Args:
            current_price: Current stock price
            
        Returns:
            Return percentage
        """
        return ((current_price - self.current_price) / self.current_price) * 100
