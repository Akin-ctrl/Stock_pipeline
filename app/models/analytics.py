"""
Analytics models for backtesting and recommendation snapshots.
"""

from datetime import date, datetime
from decimal import Decimal
from sqlalchemy import (
    Column,
    BigInteger,
    Integer,
    String,
    Date,
    Boolean,
    Numeric,
    TIMESTAMP,
    ForeignKey,
    Index,
    CheckConstraint,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import relationship

from app.models.base import Base, TimestampMixin


class BacktestRun(Base, TimestampMixin):
    """Summary record for a backtest execution."""

    __tablename__ = "backtest_runs"

    run_id = Column(BigInteger, primary_key=True, autoincrement=True)
    run_date = Column(Date, nullable=False, index=True)
    start_date = Column(Date, nullable=False)
    end_date = Column(Date, nullable=False)
    horizon_days = Column(Integer, nullable=False)
    profile = Column(String(50), nullable=False)
    run_type = Column(String(50), nullable=False, default="weekly_validation", index=True)

    total_trades = Column(Integer, nullable=False)
    win_rate_pct = Column(Numeric(6, 2), nullable=False)
    average_return_pct = Column(Numeric(8, 2), nullable=False)
    average_win_pct = Column(Numeric(8, 2), nullable=False)
    average_loss_pct = Column(Numeric(8, 2), nullable=False)
    profit_factor = Column(Numeric(10, 4), nullable=False)
    directional_accuracy_pct = Column(Numeric(6, 2), nullable=False)
    max_drawdown_pct = Column(Numeric(8, 2), nullable=False)

    run_metadata = Column(JSONB, default={})
    created_at = Column(TIMESTAMP, default=datetime.utcnow)

    trades = relationship("BacktestTrade", back_populates="run", cascade="all, delete-orphan")
    snapshots = relationship("RecommendationSnapshot", back_populates="run", cascade="all, delete-orphan")
    portfolio_positions = relationship(
        "BacktestPortfolioPosition",
        back_populates="run",
        cascade="all, delete-orphan",
    )
    portfolio_equity_curve = relationship(
        "BacktestPortfolioEquityPoint",
        back_populates="run",
        cascade="all, delete-orphan",
    )
    yearly_performance = relationship(
        "BacktestYearlyPerformance",
        back_populates="run",
        cascade="all, delete-orphan",
    )
    stock_performance = relationship(
        "BacktestStockPerformance",
        back_populates="run",
        cascade="all, delete-orphan",
    )
    sector_performance = relationship(
        "BacktestSectorPerformance",
        back_populates="run",
        cascade="all, delete-orphan",
    )


class BacktestTrade(Base, TimestampMixin):
    """Individual trade produced by a backtest run."""

    __tablename__ = "backtest_trades"

    trade_id = Column(BigInteger, primary_key=True, autoincrement=True)
    run_id = Column(BigInteger, ForeignKey("backtest_runs.run_id", ondelete="CASCADE"), nullable=False)

    stock_code = Column(String(20), nullable=False)
    entry_date = Column(Date, nullable=False)
    exit_date = Column(Date, nullable=False)
    signal_type = Column(String(20), nullable=False)
    confidence = Column(Numeric(5, 4), nullable=False)
    score = Column(Numeric(6, 2), nullable=False)
    entry_price = Column(Numeric(18, 4), nullable=False)
    exit_price = Column(Numeric(18, 4), nullable=False)
    gross_return_pct = Column(Numeric(8, 4), nullable=False)
    net_return_pct = Column(Numeric(8, 4), nullable=False)
    correct_direction = Column(Boolean, nullable=False)

    created_at = Column(TIMESTAMP, default=datetime.utcnow)

    run = relationship("BacktestRun", back_populates="trades")

    __table_args__ = (
        Index("idx_backtest_trade_run", "run_id"),
        Index("idx_backtest_trade_stock", "stock_code"),
    )


class BacktestPortfolioPosition(Base, TimestampMixin):
    """Portfolio-gated trade allocation produced by a validation run."""

    __tablename__ = "backtest_portfolio_positions"

    position_id = Column(BigInteger, primary_key=True, autoincrement=True)
    run_id = Column(BigInteger, ForeignKey("backtest_runs.run_id", ondelete="CASCADE"), nullable=False)

    stock_code = Column(String(20), nullable=False)
    sector_name = Column(String(255))
    entry_date = Column(Date, nullable=False)
    exit_date = Column(Date, nullable=False)
    holding_days = Column(Integer, nullable=False)
    signal_type = Column(String(20), nullable=False)
    confidence = Column(Numeric(5, 4))
    score = Column(Numeric(6, 2))
    predicted_probability_10d_up = Column(Numeric(6, 4))
    entry_price = Column(Numeric(18, 4), nullable=False)
    exit_price = Column(Numeric(18, 4), nullable=False)
    allocated_capital = Column(Numeric(18, 4), nullable=False)
    net_return_pct = Column(Numeric(8, 4), nullable=False)
    realized_pnl = Column(Numeric(18, 4), nullable=False)
    exit_value = Column(Numeric(18, 4), nullable=False)
    was_winner = Column(Boolean, nullable=False)

    created_at = Column(TIMESTAMP, default=datetime.utcnow)

    run = relationship("BacktestRun", back_populates="portfolio_positions")

    __table_args__ = (
        Index("idx_backtest_portfolio_position_run", "run_id"),
        Index("idx_backtest_portfolio_position_stock", "stock_code"),
        Index("idx_backtest_portfolio_position_entry", "entry_date"),
    )


class BacktestPortfolioEquityPoint(Base, TimestampMixin):
    """Portfolio equity and drawdown point for a validation run."""

    __tablename__ = "backtest_portfolio_equity_curve"

    equity_point_id = Column(BigInteger, primary_key=True, autoincrement=True)
    run_id = Column(BigInteger, ForeignKey("backtest_runs.run_id", ondelete="CASCADE"), nullable=False)

    point_index = Column(Integer, nullable=False)
    event_date = Column(Date, nullable=False)
    cash = Column(Numeric(18, 4), nullable=False)
    open_position_capital = Column(Numeric(18, 4), nullable=False)
    equity = Column(Numeric(18, 4), nullable=False)
    drawdown_pct = Column(Numeric(8, 4), nullable=False)
    open_positions = Column(Integer, nullable=False)

    created_at = Column(TIMESTAMP, default=datetime.utcnow)

    run = relationship("BacktestRun", back_populates="portfolio_equity_curve")

    __table_args__ = (
        Index("idx_backtest_portfolio_equity_run", "run_id"),
        Index("idx_backtest_portfolio_equity_date", "event_date"),
        Index("ux_backtest_portfolio_equity_run_index", "run_id", "point_index", unique=True),
    )


class BacktestYearlyPerformance(Base, TimestampMixin):
    """Calendar-year performance summary for a validation run."""

    __tablename__ = "backtest_yearly_performance"

    yearly_metric_id = Column(BigInteger, primary_key=True, autoincrement=True)
    run_id = Column(BigInteger, ForeignKey("backtest_runs.run_id", ondelete="CASCADE"), nullable=False)

    calendar_year = Column(Integer, nullable=False)
    trade_count = Column(Integer, nullable=False)
    win_rate_pct = Column(Numeric(6, 2), nullable=False)
    average_return_pct = Column(Numeric(8, 2), nullable=False)
    profit_factor = Column(Numeric(10, 4))
    portfolio_return_pct = Column(Numeric(8, 2), nullable=False)
    portfolio_max_drawdown_pct = Column(Numeric(8, 2), nullable=False)
    portfolio_win_rate_pct = Column(Numeric(6, 2), nullable=False)
    portfolio_profit_factor = Column(Numeric(10, 4))
    starting_equity = Column(Numeric(18, 4), nullable=False)
    ending_equity = Column(Numeric(18, 4), nullable=False)

    created_at = Column(TIMESTAMP, default=datetime.utcnow)

    run = relationship("BacktestRun", back_populates="yearly_performance")

    __table_args__ = (
        Index("idx_backtest_yearly_run", "run_id"),
        Index("ux_backtest_yearly_run_year", "run_id", "calendar_year", unique=True),
    )


class BacktestStockPerformance(Base, TimestampMixin):
    """Per-stock validation performance for a backtest run."""

    __tablename__ = "backtest_stock_performance"

    stock_metric_id = Column(BigInteger, primary_key=True, autoincrement=True)
    run_id = Column(BigInteger, ForeignKey("backtest_runs.run_id", ondelete="CASCADE"), nullable=False)

    stock_code = Column(String(20), nullable=False)
    sector_name = Column(String(255))
    trade_count = Column(Integer, nullable=False)
    win_rate_pct = Column(Numeric(6, 2), nullable=False)
    average_return_pct = Column(Numeric(8, 2), nullable=False)
    total_realized_pnl = Column(Numeric(18, 4), nullable=False)
    best_trade_pct = Column(Numeric(8, 4), nullable=False)
    worst_trade_pct = Column(Numeric(8, 4), nullable=False)
    profit_factor = Column(Numeric(10, 4))

    created_at = Column(TIMESTAMP, default=datetime.utcnow)

    run = relationship("BacktestRun", back_populates="stock_performance")

    __table_args__ = (
        Index("idx_backtest_stock_performance_run", "run_id"),
        Index("ux_backtest_stock_performance_run_stock", "run_id", "stock_code", unique=True),
    )


class BacktestSectorPerformance(Base, TimestampMixin):
    """Per-sector validation performance for a backtest run."""

    __tablename__ = "backtest_sector_performance"

    sector_metric_id = Column(BigInteger, primary_key=True, autoincrement=True)
    run_id = Column(BigInteger, ForeignKey("backtest_runs.run_id", ondelete="CASCADE"), nullable=False)

    sector_name = Column(String(255), nullable=False)
    trade_count = Column(Integer, nullable=False)
    win_rate_pct = Column(Numeric(6, 2), nullable=False)
    average_return_pct = Column(Numeric(8, 2), nullable=False)
    total_realized_pnl = Column(Numeric(18, 4), nullable=False)
    profit_factor = Column(Numeric(10, 4))

    created_at = Column(TIMESTAMP, default=datetime.utcnow)

    run = relationship("BacktestRun", back_populates="sector_performance")

    __table_args__ = (
        Index("idx_backtest_sector_performance_run", "run_id"),
        Index("ux_backtest_sector_performance_run_sector", "run_id", "sector_name", unique=True),
    )


class RecommendationSnapshot(Base, TimestampMixin):
    """Snapshot of top recommendations at a given run."""

    __tablename__ = "recommendation_snapshots"

    snapshot_id = Column(BigInteger, primary_key=True, autoincrement=True)
    run_id = Column(BigInteger, ForeignKey("backtest_runs.run_id", ondelete="CASCADE"), nullable=False)
    snapshot_date = Column(Date, nullable=False, index=True)
    profile = Column(String(50), nullable=False)

    stock_code = Column(String(20), nullable=False)
    company_name = Column(String(255), nullable=False)
    signal_type = Column(String(20), nullable=False)
    confidence = Column(Numeric(5, 4), nullable=False)
    score = Column(Numeric(6, 2), nullable=False)
    current_price = Column(Numeric(18, 4), nullable=False)
    target_price = Column(Numeric(18, 4))
    stop_loss = Column(Numeric(18, 4))
    reasons = Column(JSONB, default=list)

    created_at = Column(TIMESTAMP, default=datetime.utcnow)

    run = relationship("BacktestRun", back_populates="snapshots")

    __table_args__ = (
        Index("idx_reco_snapshot_run", "run_id"),
        Index("idx_reco_snapshot_date", "snapshot_date"),
        Index("idx_reco_snapshot_stock", "stock_code"),
        Index("ux_reco_snapshot_run_stock", "run_id", "stock_code", unique=True),
    )


class WeeklyRecommendation(Base, TimestampMixin):
    """Weekly candidate board sourced from recommendation audit snapshots."""

    __tablename__ = "weekly_recommendations"

    weekly_recommendation_id = Column(BigInteger, primary_key=True, autoincrement=True)
    week_start_date = Column(Date, nullable=False, index=True)
    week_end_date = Column(Date, nullable=False, index=True)
    recommendation_date = Column(Date, nullable=False, index=True)
    profile = Column(String(50), nullable=False, index=True)

    stock_id = Column(Integer, ForeignKey("dim_stocks.stock_id", ondelete="CASCADE"), nullable=False)
    stock_code = Column(String(20), nullable=False)
    company_name = Column(String(255), nullable=False)
    sector_name = Column(String(100))

    rank = Column(Integer, nullable=False)
    weekly_status = Column(String(30), nullable=False, index=True)
    candidate_tier = Column(String(20), nullable=False)
    action_type = Column(String(20), nullable=False)
    technical_signal_type = Column(String(20))
    rejection_reason = Column(String(100))

    signal_agreement = Column(Numeric(6, 4))
    heuristic_score = Column(Numeric(6, 2), nullable=False)
    current_price = Column(Numeric(18, 4), nullable=False)
    rsi_14 = Column(Numeric(5, 2))
    volatility = Column(Numeric(10, 4))
    volume_ratio = Column(Numeric(12, 4))
    price_change_20d = Column(Numeric(10, 4))
    drawdown_20d_pct = Column(Numeric(10, 4))

    rationale = Column(JSONB, default=list)
    source = Column(String(50), nullable=False, default="recommendation_audit")
    created_at = Column(TIMESTAMP, default=datetime.utcnow)

    stock = relationship("DimStock")

    __table_args__ = (
        CheckConstraint(
            "weekly_status IN ("
            "'APPROVED', 'WATCHLIST', 'WAIT_FOR_PULLBACK', "
            "'WAIT_FOR_VOLUME', 'HIGH_RISK_WATCHLIST', 'SPECULATIVE_WATCHLIST'"
            ")",
            name="chk_weekly_recommendation_status",
        ),
        CheckConstraint(
            "candidate_tier IN ('approved', 'watchlist')",
            name="chk_weekly_recommendation_candidate_tier",
        ),
        CheckConstraint(
            "action_type IN ('STRONG_BUY', 'BUY')",
            name="chk_weekly_recommendation_action_type",
        ),
        CheckConstraint(
            "rank > 0",
            name="chk_weekly_recommendation_rank_positive",
        ),
        Index("idx_weekly_recommendation_week", "week_end_date", "profile"),
        Index("idx_weekly_recommendation_stock", "stock_code"),
        Index(
            "ux_weekly_recommendation_week_profile_stock",
            "week_end_date",
            "profile",
            "stock_id",
            unique=True,
        ),
    )


class DecisionSignal(Base, TimestampMixin):
    """Weekly decision flag derived from backtest performance."""

    __tablename__ = "decision_signals"

    signal_id = Column(BigInteger, primary_key=True, autoincrement=True)
    run_date = Column(Date, nullable=False, index=True)
    profile = Column(String(50), nullable=False)
    run_type = Column(String(50), nullable=False, default="weekly_validation", index=True)
    status = Column(String(20), nullable=False)  # GREEN / YELLOW / RED

    win_rate_pct = Column(Numeric(6, 2), nullable=False)
    average_return_pct = Column(Numeric(8, 2), nullable=False)
    profit_factor = Column(Numeric(10, 4), nullable=False)
    max_drawdown_pct = Column(Numeric(8, 2), nullable=False)
    lookback_runs = Column(Integer, nullable=False)

    rationale = Column(JSONB, default=list)
    created_at = Column(TIMESTAMP, default=datetime.utcnow)

    __table_args__ = (
        Index("idx_decision_signal_date", "run_date"),
        Index("idx_decision_signal_profile", "profile"),
        Index("ux_decision_signal_date_profile_type", "run_date", "profile", "run_type", unique=True),
    )
