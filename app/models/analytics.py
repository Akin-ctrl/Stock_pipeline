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


class DecisionSignal(Base, TimestampMixin):
    """Weekly decision flag derived from backtest performance."""

    __tablename__ = "decision_signals"

    signal_id = Column(BigInteger, primary_key=True, autoincrement=True)
    run_date = Column(Date, nullable=False, index=True)
    profile = Column(String(50), nullable=False)
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
        Index("ux_decision_signal_date_profile", "run_date", "profile", unique=True),
    )
