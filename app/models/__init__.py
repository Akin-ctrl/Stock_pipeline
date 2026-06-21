"""Models package initialization."""

from app.models.base import Base, BaseModel, TimestampMixin
from app.models.dimension import DimSector, DimStock
from app.models.fact import (
    FactDailyPrice,
    FactTechnicalIndicator,
    FactRecommendation,
    FactRecommendationAudit,
)
from app.models.alert import AlertRule, AlertHistory
from app.models.staging import StagingDailyPrice, StagingAuditLog
from app.models.analytics import (
    BacktestRun,
    BacktestTrade,
    BacktestPortfolioPosition,
    BacktestPortfolioEquityPoint,
    BacktestYearlyPerformance,
    BacktestStockPerformance,
    BacktestSectorPerformance,
    RecommendationSnapshot,
    WeeklyRecommendation,
    DecisionSignal,
)

__all__ = [
    "Base",
    "BaseModel",
    "TimestampMixin",
    "DimSector",
    "DimStock",
    "FactDailyPrice",
    "FactTechnicalIndicator",
    "FactRecommendation",
    "FactRecommendationAudit",
    "AlertRule",
    "AlertHistory",
    "StagingDailyPrice",
    "StagingAuditLog",
    "BacktestRun",
    "BacktestTrade",
    "BacktestPortfolioPosition",
    "BacktestPortfolioEquityPoint",
    "BacktestYearlyPerformance",
    "BacktestStockPerformance",
    "BacktestSectorPerformance",
    "RecommendationSnapshot",
    "WeeklyRecommendation",
    "DecisionSignal",
]
