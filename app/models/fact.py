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
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func

from app.models.base import Base, TimestampMixin


class FactDailyPrice(Base, TimestampMixin):
    """
    Daily stock price fact table (time-series).
    
    Stores canonical daily closing price and derived metrics for each stock.
    
    Attributes:
        price_id: Primary key
        stock_id: Foreign key to dim_stocks
        price_date: Trading date
        close_price: Closing price (required)
        change_1d_pct: Daily percentage change
        change_ytd_pct: Year-to-date percentage change
        volume: Trading volume
        source: Selected source identifier for the promoted daily record
        source_count: Number of source observations considered for this daily record
        bar_status: Promotion/trust state ('OBSERVED', 'RECONCILED', 'OFFICIAL', 'ESTIMATED')
        is_official: Whether the row comes from an official market source
        confidence_score: Confidence score for the promoted row (0-100)
        data_quality_flag: Quality indicator ('GOOD', 'INCOMPLETE', 'SUSPICIOUS', 'MISSING', 'STALE', 'POOR')
        has_complete_data: Whether all expected fields are present
        ingestion_timestamp: When data was ingested
    
    Example:
        >>> price = FactDailyPrice(
        >>>     stock_id=1,
        >>>     price_date=date(2025, 12, 30),
        >>>     close_price=Decimal('35.50'),
        >>>     change_1d_pct=Decimal('2.5'),
        >>>     change_ytd_pct=Decimal('15.3')
        >>> )
    """
    __tablename__ = 'fact_daily_prices'
    
    price_id = Column(BigInteger, primary_key=True, autoincrement=True)
    stock_id = Column(Integer, ForeignKey('dim_stocks.stock_id', ondelete='CASCADE'), nullable=False)
    price_date = Column(Date, nullable=False, index=True)
    
    # Price Data
    close_price = Column(Numeric(18, 4), nullable=False)
    volume = Column(BigInteger)
    
    # Calculated Fields (from source)
    change_1d_pct = Column(Numeric(10, 4))
    change_ytd_pct = Column(Numeric(10, 4))
    
    # Metadata
    source = Column(String(50), nullable=False)
    source_count = Column(Integer, nullable=False, default=1)
    bar_status = Column(String(20), nullable=False, default='OBSERVED')
    is_official = Column(Boolean, nullable=False, default=False)
    confidence_score = Column(Numeric(5, 2))
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
        CheckConstraint(
            "bar_status IN ('OBSERVED', 'RECONCILED', 'OFFICIAL', 'ESTIMATED')",
            name='chk_bar_status'
        ),
        CheckConstraint(
            'source_count >= 1',
            name='chk_source_count_positive'
        ),
        CheckConstraint(
            'confidence_score IS NULL OR confidence_score BETWEEN 0 AND 100',
            name='chk_confidence_score_range'
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
    
    def validate_price_fields(self) -> bool:
        """
        Validate close price and volume fields.
        
        Returns:
            True if fields are valid, False otherwise
        """
        if self.close_price is None:
            return False
        if float(self.close_price) <= 0:
            return False
        if self.volume is not None and int(self.volume) < 0:
            return False
        return True


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


class FactRecommendationAudit(Base, TimestampMixin):
    """Per-stock audit trail for one recommendation generation run.

    The production recommendation table intentionally stores only final
    recommendations. This audit fact stores the full candidate funnel so zero
    recommendation days remain explainable and dashboard-visible.
    """

    __tablename__ = 'fact_recommendation_audit'

    audit_id = Column(BigInteger, primary_key=True, autoincrement=True)
    stock_id = Column(Integer, ForeignKey('dim_stocks.stock_id'), nullable=False)
    recommendation_date = Column(Date, nullable=False, index=True)
    profile = Column(String(50), nullable=False, default='steady_20p_10d', index=True)

    price_date = Column(Date, nullable=True)
    indicator_date = Column(Date, nullable=True)
    current_price = Column(Numeric(18, 4), nullable=True)

    stage_reached = Column(String(50), nullable=False)
    rejection_reason = Column(String(100), nullable=True)
    eligible = Column(Boolean, nullable=False, default=False)
    selected = Column(Boolean, nullable=False, default=False)
    candidate_tier = Column(String(20), nullable=False, default='blocked')
    portfolio_approved = Column(Boolean, nullable=False, default=False)
    portfolio_rejection_reason = Column(String(50), nullable=True)
    portfolio_rank = Column(Integer, nullable=True)

    action_type = Column(String(20), nullable=True)
    technical_signal_type = Column(String(20), nullable=True)
    signal_agreement = Column(Numeric(6, 4), nullable=True)
    predicted_probability_10d_up = Column(Numeric(6, 4), nullable=True)
    heuristic_score = Column(Numeric(6, 2), nullable=True)
    heuristic_score_category = Column(String(20), nullable=True)

    rsi_14 = Column(Numeric(5, 2), nullable=True)
    volatility = Column(Numeric(10, 4), nullable=True)
    volume_ratio = Column(Numeric(12, 4), nullable=True)
    price_change_20d = Column(Numeric(10, 4), nullable=True)
    drawdown_20d_pct = Column(Numeric(10, 4), nullable=True)
    trusted_history_days = Column(Integer, nullable=True)
    price_quality_flag = Column(String(20), nullable=True)
    bar_status = Column(String(20), nullable=True)
    has_complete_data = Column(Boolean, nullable=True)
    is_official = Column(Boolean, nullable=True)

    score_breakdown = Column(JSONB, default=dict)
    indicators = Column(JSONB, default=dict)
    model_version = Column(String(50), nullable=True)

    stock = relationship("DimStock")

    __table_args__ = (
        CheckConstraint(
            "stage_reached IN ("
            "'stock_loaded', 'no_indicator', 'no_trusted_price', "
            "'indicator_price_date_mismatch', 'scored', 'eligibility_failed', "
            "'selection_failed', 'selected', 'portfolio_evaluated'"
            ")",
            name='chk_recommendation_audit_stage'
        ),
        CheckConstraint(
            "candidate_tier IN ('approved', 'watchlist', 'avoid', 'blocked')",
            name='chk_recommendation_audit_candidate_tier'
        ),
        CheckConstraint(
            'signal_agreement IS NULL OR signal_agreement BETWEEN 0 AND 1',
            name='chk_recommendation_audit_signal_agreement'
        ),
        CheckConstraint(
            'predicted_probability_10d_up IS NULL OR '
            'predicted_probability_10d_up BETWEEN 0 AND 1',
            name='chk_recommendation_audit_predicted_probability'
        ),
        CheckConstraint(
            'heuristic_score IS NULL OR heuristic_score BETWEEN 0 AND 100',
            name='chk_recommendation_audit_heuristic_score'
        ),
        Index(
            'ux_recommendation_audit_stock_date_profile',
            'stock_id',
            'recommendation_date',
            'profile',
            unique=True
        ),
        Index(
            'ix_recommendation_audit_date_profile_stage',
            'recommendation_date',
            'profile',
            'stage_reached'
        ),
        Index(
            'ix_recommendation_audit_rejection_reason',
            'rejection_reason'
        ),
    )

    def __repr__(self) -> str:
        """String representation."""
        return (
            f"<FactRecommendationAudit(id={self.audit_id}, "
            f"stock_id={self.stock_id}, date={self.recommendation_date}, "
            f"stage={self.stage_reached}, reason={self.rejection_reason})>"
        )


class FactRecommendation(Base, TimestampMixin):
    """Model-aligned recommendation fact table.

    The revamped advisor separates the user-facing long-only action from the
    underlying technical signal and from the scoring/probability evidence.
    This table stores that decision grain directly for dashboard use.
    """

    __tablename__ = 'fact_recommendations'
    
    recommendation_id = Column(BigInteger, primary_key=True, autoincrement=True)
    stock_id = Column(Integer, ForeignKey('dim_stocks.stock_id'), nullable=False)
    recommendation_date = Column(Date, nullable=False, index=True)
    profile = Column(String(50), nullable=False, default='steady_20p_10d', index=True)
    
    # Decision and model evidence
    action_type = Column(
        String(20),
        nullable=False,
        index=True,
        comment="Long-only user action: STRONG_BUY, BUY, HOLD, AVOID, STRONGLY_AVOID"
    )
    technical_signal_type = Column(
        String(20),
        nullable=False,
        index=True,
        comment="Underlying technical signal: STRONG_BUY, BUY, HOLD, SELL, STRONG_SELL"
    )
    signal_agreement = Column(
        Numeric(6, 4),
        nullable=False,
        comment="Heuristic signal agreement, stored as 0.0000-1.0000"
    )
    predicted_probability_10d_up = Column(
        Numeric(6, 4),
        nullable=True,
        comment="Estimated probability of a positive 10-trading-day move"
    )
    heuristic_score = Column(
        Numeric(6, 2),
        nullable=False,
        comment="Rule-based heuristic score, 0-100"
    )
    heuristic_score_category = Column(
        String(20),
        nullable=False,
        comment="EXCELLENT, GOOD, FAIR, POOR, VERY_POOR"
    )
    
    # Price policy
    current_price = Column(Numeric(18, 4), nullable=False)
    policy_target_price = Column(Numeric(18, 4), nullable=True)
    policy_stop_loss = Column(Numeric(18, 4), nullable=True)
    policy_upside_pct = Column(Numeric(8, 4), nullable=True)
    policy_downside_pct = Column(Numeric(8, 4), nullable=True)
    risk_reward_ratio = Column(Numeric(10, 4), nullable=True)
    
    # Risk, reasons, and score breakdown
    heuristic_risk_level = Column(
        String(10),
        nullable=False,
        comment="LOW, MEDIUM, HIGH"
    )
    reasons = Column(JSONB, default=list)
    technical_score = Column(Numeric(5, 2), nullable=True)
    momentum_score = Column(Numeric(5, 2), nullable=True)
    volatility_score = Column(Numeric(5, 2), nullable=True)
    trend_score = Column(Numeric(5, 2), nullable=True)
    volume_score = Column(Numeric(5, 2), nullable=True)
    
    # Indicator values at time of recommendation
    rsi_14 = Column(Numeric(5, 2), nullable=True)
    macd = Column(Numeric(18, 4), nullable=True)

    # Portfolio-level production gate
    portfolio_approved = Column(Boolean, default=False, nullable=False, index=True)
    portfolio_rejection_reason = Column(String(50), nullable=True)
    portfolio_rank = Column(Integer, nullable=True)
    portfolio_position_size_pct = Column(Numeric(6, 4), nullable=True)
    portfolio_policy_version = Column(String(50), nullable=True)
    portfolio_open_positions_before = Column(Integer, nullable=True)
    portfolio_available_slots_before = Column(Integer, nullable=True)
    portfolio_max_concurrent_positions = Column(Integer, nullable=True)
    portfolio_max_entries_per_day = Column(Integer, nullable=True)
    
    model_version = Column(String(50), nullable=True)
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
            "action_type IN ('STRONG_BUY', 'BUY', 'HOLD', 'AVOID', 'STRONGLY_AVOID')",
            name='chk_recommendation_action_type'
        ),
        CheckConstraint(
            "technical_signal_type IN ('STRONG_BUY', 'BUY', 'HOLD', 'SELL', 'STRONG_SELL')",
            name='chk_recommendation_technical_signal_type'
        ),
        CheckConstraint(
            'signal_agreement >= 0 AND signal_agreement <= 1',
            name='chk_recommendation_signal_agreement'
        ),
        CheckConstraint(
            'predicted_probability_10d_up IS NULL OR '
            '(predicted_probability_10d_up >= 0 AND predicted_probability_10d_up <= 1)',
            name='chk_recommendation_predicted_probability'
        ),
        CheckConstraint(
            "heuristic_risk_level IN ('LOW', 'MEDIUM', 'HIGH')",
            name='chk_recommendation_risk_level'
        ),
        CheckConstraint(
            'heuristic_score >= 0 AND heuristic_score <= 100',
            name='chk_recommendation_heuristic_score'
        ),
        CheckConstraint(
            'current_price > 0',
            name='chk_recommendation_current_price_positive'
        ),
        Index(
            'ux_recommendation_stock_date_profile',
            'stock_id',
            'recommendation_date',
            'profile',
            unique=True
        ),
    )
    
    def __repr__(self) -> str:
        """String representation."""
        return (
            f"<FactRecommendation(id={self.recommendation_id}, "
            f"stock_id={self.stock_id}, date={self.recommendation_date}, "
            f"action={self.action_type}, score={self.heuristic_score})>"
        )
    
    def is_buy_signal(self) -> bool:
        """Check if recommendation is an actionable long-entry signal."""
        return self.action_type in ('BUY', 'STRONG_BUY')
    
    def is_sell_signal(self) -> bool:
        """Check if recommendation is an avoid action in the long-only model."""
        return self.action_type in ('AVOID', 'STRONGLY_AVOID')
    
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
