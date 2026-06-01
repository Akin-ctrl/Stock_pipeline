"""Backtest recommendation quality using historical close prices only."""

from __future__ import annotations

from bisect import bisect_right
from dataclasses import asdict, dataclass, field
from datetime import date, timedelta
from math import isinf
from typing import Callable, Dict, List, Optional

from app.models import FactDailyPrice, FactTechnicalIndicator
from app.repositories import IndicatorRepository, PriceRepository, StockRepository
from app.repositories.price_repository import PriceRepository as ProductionPriceRepository
from app.services.advisory.advisor import (
    RecommendationAction,
    RecommendationProfile,
    StockScreener,
    StockRecommendation,
)
from app.services.modeling.targets import (
    DEFAULT_DIRECTION_TARGET,
    calculate_forward_return_pct,
)
from app.services.modeling import NullProbabilityEstimator


ACTIONABLE_ACTIONS = {"BUY", "STRONG_BUY"}
BACKTEST_CACHE_LOOKBACK_DAYS = 260
RecommendationFilter = Callable[[date, List[StockRecommendation]], List[StockRecommendation]]


@dataclass
class BacktestTrade:
    stock_code: str
    entry_date: date
    exit_date: date
    action_type: str
    signal_type: str
    confidence: float
    score: float
    predicted_probability_10d_up: Optional[float]
    entry_price: float
    exit_price: float
    gross_return_pct: float
    net_return_pct: float
    correct_direction: bool


@dataclass
class BacktestResult:
    start_date: date
    end_date: date
    horizon_days: int
    trades: List[BacktestTrade] = field(default_factory=list)

    @property
    def total_trades(self) -> int:
        return len(self.trades)

    @property
    def wins(self) -> int:
        return sum(1 for trade in self.trades if trade.net_return_pct > 0)

    @property
    def losses(self) -> int:
        return sum(1 for trade in self.trades if trade.net_return_pct <= 0)

    @property
    def win_rate_pct(self) -> float:
        return (self.wins / self.total_trades * 100.0) if self.total_trades else 0.0

    @property
    def average_return_pct(self) -> float:
        if not self.trades:
            return 0.0
        return sum(trade.net_return_pct for trade in self.trades) / len(self.trades)

    @property
    def average_win_pct(self) -> float:
        wins = [trade.net_return_pct for trade in self.trades if trade.net_return_pct > 0]
        return sum(wins) / len(wins) if wins else 0.0

    @property
    def average_loss_pct(self) -> float:
        losses = [trade.net_return_pct for trade in self.trades if trade.net_return_pct <= 0]
        return sum(losses) / len(losses) if losses else 0.0

    @property
    def profit_factor(self) -> float:
        gross_profit = sum(trade.net_return_pct for trade in self.trades if trade.net_return_pct > 0)
        gross_loss = abs(sum(trade.net_return_pct for trade in self.trades if trade.net_return_pct < 0))
        return gross_profit / gross_loss if gross_loss > 0 else float("inf") if gross_profit > 0 else 0.0

    @property
    def directional_accuracy_pct(self) -> float:
        if not self.trades:
            return 0.0
        return sum(1 for trade in self.trades if trade.correct_direction) / len(self.trades) * 100.0

    @property
    def max_drawdown_pct(self) -> float:
        if not self.trades:
            return 0.0

        equity = 1.0
        peak = 1.0
        max_drawdown = 0.0
        for trade in sorted(self.trades, key=lambda item: (item.entry_date, item.stock_code)):
            equity *= 1.0 + (trade.net_return_pct / 100.0)
            if equity > peak:
                peak = equity
            drawdown = (peak - equity) / peak * 100.0
            if drawdown > max_drawdown:
                max_drawdown = drawdown
        return max_drawdown

    def to_dict(self) -> Dict:
        return {
            "start_date": self.start_date.isoformat(),
            "end_date": self.end_date.isoformat(),
            "horizon_days": self.horizon_days,
            "total_trades": self.total_trades,
            "wins": self.wins,
            "losses": self.losses,
            "win_rate_pct": round(self.win_rate_pct, 2),
            "average_return_pct": round(self.average_return_pct, 2),
            "average_win_pct": round(self.average_win_pct, 2),
            "average_loss_pct": round(self.average_loss_pct, 2),
            "profit_factor": _json_profit_factor(self.profit_factor),
            "profit_factor_unbounded": isinf(self.profit_factor),
            "directional_accuracy_pct": round(self.directional_accuracy_pct, 2),
            "max_drawdown_pct": round(self.max_drawdown_pct, 2),
            "trades": [asdict(trade) for trade in self.trades],
        }


class RecommendationBacktester:
    """Evaluate recommendation quality on historical close prices."""

    def __init__(
        self,
        session,
        strategy_profile: str = RecommendationProfile.STEADY_20P_10D.value,
        round_trip_cost_pct: float = 0.20,
        max_abs_gross_return_pct: Optional[float] = 50.0,
        calculate_probability: bool = True,
        recommendation_filter: Optional[RecommendationFilter] = None,
    ):
        self.session = session
        self.price_repo = PriceRepository(session)
        self.stock_repo = StockRepository(session)
        self.indicator_repo = IndicatorRepository(session)
        probability_estimator = None if calculate_probability else NullProbabilityEstimator()
        self.screener = StockScreener(
            session,
            strategy_profile=strategy_profile,
            probability_estimator=probability_estimator,
        )
        self.round_trip_cost_pct = round_trip_cost_pct
        self.max_abs_gross_return_pct = max_abs_gross_return_pct
        self.recommendation_filter = recommendation_filter

    def run(
        self,
        start_date: date,
        end_date: date,
        horizon_days: int = DEFAULT_DIRECTION_TARGET.horizon_trading_days,
        stock_codes: Optional[List[str]] = None,
        min_score: Optional[float] = None,
        min_confidence: Optional[float] = None,
        min_predicted_probability: Optional[float] = None,
        include_hold: bool = False,
        top_n_per_day: Optional[int] = None,
        avoid_overlapping_positions: bool = False,
    ) -> BacktestResult:
        stocks = self._resolve_stocks(stock_codes)
        resolved_stock_codes = [stock.stock_code for stock in stocks]
        trades: List[BacktestTrade] = []
        prices_by_stock_id: dict[int, dict[date, FactDailyPrice]] = {}
        trading_dates_by_stock_id: dict[int, list[date]] = {}
        trading_date_index_by_stock_id: dict[int, dict[date, int]] = {}
        candidate_entry_dates = set()
        open_until_by_stock_code = {}

        for stock in stocks:
            history = self.price_repo.get_price_history(
                stock.stock_id,
                start_date=start_date - timedelta(days=BACKTEST_CACHE_LOOKBACK_DAYS),
                end_date=end_date,
            )

            if len(history) <= horizon_days:
                continue

            prices_by_date = {price.price_date: price for price in history}
            prices_by_stock_id[stock.stock_id] = prices_by_date
            trading_dates = sorted(prices_by_date.keys())
            trading_dates_by_stock_id[stock.stock_id] = trading_dates
            trading_date_index_by_stock_id[stock.stock_id] = {
                trading_date: index
                for index, trading_date in enumerate(trading_dates)
            }

            for index, entry_date in enumerate(trading_dates[:-horizon_days]):
                if entry_date < start_date or entry_date > end_date:
                    continue
                candidate_entry_dates.add(entry_date)

        self._install_screener_caches(
            stocks=stocks,
            prices_by_stock_id=prices_by_stock_id,
            start_date=start_date,
            end_date=end_date,
        )

        for entry_date in sorted(candidate_entry_dates):
            recommendations = self.screener.generate_recommendations(
                recommendation_date=entry_date,
                stock_codes=resolved_stock_codes,
                min_score=min_score,
                min_confidence=min_confidence,
                min_predicted_probability=min_predicted_probability,
            )
            if self.recommendation_filter is not None:
                recommendations = self.recommendation_filter(
                    entry_date,
                    recommendations,
                )
            if top_n_per_day is not None and top_n_per_day > 0:
                recommendations = recommendations[:top_n_per_day]
            recommendations_by_stock = {
                recommendation.stock_code: recommendation
                for recommendation in recommendations
            }

            for stock in stocks:
                recommendation = recommendations_by_stock.get(stock.stock_code)
                if recommendation is None:
                    continue
                if avoid_overlapping_positions:
                    open_until = open_until_by_stock_code.get(stock.stock_code)
                    if open_until is not None and entry_date <= open_until:
                        continue

                action_value = recommendation.action_type.value
                signal_value = recommendation.signal_type.value
                if not include_hold and action_value not in ACTIONABLE_ACTIONS:
                    continue

                prices_by_date = prices_by_stock_id.get(stock.stock_id)
                trading_dates = trading_dates_by_stock_id.get(stock.stock_id)
                trading_date_index = trading_date_index_by_stock_id.get(stock.stock_id)
                if not prices_by_date or not trading_dates or not trading_date_index:
                    continue

                index = trading_date_index.get(entry_date)
                if index is None or index + horizon_days >= len(trading_dates):
                    continue

                exit_index = min(index + horizon_days, len(trading_dates) - 1)
                exit_date = trading_dates[exit_index]
                if avoid_overlapping_positions:
                    open_until_by_stock_code[stock.stock_code] = exit_date
                entry_price = float(prices_by_date[entry_date].close_price)
                exit_price = float(prices_by_date[exit_date].close_price)

                gross_return_pct, correct_direction = self._evaluate_return(
                    recommendation.action_type,
                    entry_price,
                    exit_price,
                )
                if self._has_extreme_trade_return(gross_return_pct):
                    continue
                net_return_pct = gross_return_pct - self.round_trip_cost_pct

                trades.append(
                    BacktestTrade(
                        stock_code=stock.stock_code,
                        entry_date=entry_date,
                        exit_date=exit_date,
                        action_type=action_value,
                        signal_type=signal_value,
                        confidence=float(recommendation.confidence),
                        score=float(recommendation.score),
                        predicted_probability_10d_up=recommendation.predicted_probability_10d_up,
                        entry_price=entry_price,
                        exit_price=exit_price,
                        gross_return_pct=round(gross_return_pct, 4),
                        net_return_pct=round(net_return_pct, 4),
                        correct_direction=correct_direction,
                    )
                )

        trades.sort(key=lambda item: (item.stock_code, item.entry_date))
        return BacktestResult(
            start_date=start_date,
            end_date=end_date,
            horizon_days=horizon_days,
            trades=trades,
        )

    def _has_extreme_trade_return(self, gross_return_pct: float) -> bool:
        """Exclude split-like/outlier return windows from accuracy evaluation."""
        threshold = self.max_abs_gross_return_pct
        return threshold is not None and abs(gross_return_pct) > threshold

    def _install_screener_caches(
        self,
        *,
        stocks,
        prices_by_stock_id: dict[int, dict[date, FactDailyPrice]],
        start_date: date,
        end_date: date,
    ) -> None:
        """Avoid repeated read-only repository queries during historical backtests."""
        indicator_history_by_stock_code = {}
        for stock in stocks:
            indicators = self.indicator_repo.get_indicator_history(
                stock.stock_id,
                start_date=start_date - timedelta(days=BACKTEST_CACHE_LOOKBACK_DAYS),
                end_date=end_date,
            )
            indicator_history_by_stock_code[stock.stock_code.upper()] = indicators

        self.screener.stock_repo = _CachedStockRepository(stocks)
        self.screener.price_repo = _CachedPriceRepository(prices_by_stock_id)
        self.screener.indicator_repo = _CachedIndicatorRepository(
            indicator_history_by_stock_code
        )

    def _resolve_stocks(self, stock_codes: Optional[List[str]]):
        if stock_codes:
            stocks = [self.stock_repo.get_by_code(code) for code in stock_codes]
            return [stock for stock in stocks if stock is not None]
        return self.stock_repo.get_all_active()

    def _evaluate_return(
        self,
        action_type: RecommendationAction,
        entry_price: float,
        exit_price: float,
    ) -> tuple[float, bool]:
        forward_return_pct = calculate_forward_return_pct(entry_price, exit_price)

        if action_type in {RecommendationAction.BUY, RecommendationAction.STRONG_BUY}:
            gross_return = forward_return_pct
            correct_direction = forward_return_pct > 0
        else:
            gross_return = 0.0
            correct_direction = False
        return gross_return, correct_direction


def _json_profit_factor(profit_factor: float) -> float | None:
    """Return strict-JSON profit factor; unbounded values are represented as null."""
    if isinf(profit_factor):
        return None
    return round(profit_factor, 2)


class _CachedStockRepository:
    """Small read-through stock lookup used only inside backtest runs."""

    def __init__(self, stocks):
        self._stocks = list(stocks)
        self._stocks_by_code = {
            stock.stock_code.upper(): stock
            for stock in self._stocks
        }

    def get_by_code(self, stock_code: str):
        return self._stocks_by_code.get(stock_code.upper())

    def get_all_active(self, exchange: Optional[str] = None):
        if exchange is None:
            return list(self._stocks)
        return [
            stock
            for stock in self._stocks
            if stock.exchange == exchange.upper()
        ]


class _CachedIndicatorRepository:
    """Latest-as-of lookup for indicator rows already loaded into memory."""

    def __init__(
        self,
        indicator_history_by_stock_code: dict[str, list[FactTechnicalIndicator]],
    ):
        self._rows_by_code = {}
        self._dates_by_code = {}
        for stock_code, rows in indicator_history_by_stock_code.items():
            sorted_rows = sorted(rows, key=lambda row: row.calculation_date)
            self._rows_by_code[stock_code.upper()] = sorted_rows
            self._dates_by_code[stock_code.upper()] = [
                row.calculation_date
                for row in sorted_rows
            ]

    def get_latest_by_code(
        self,
        stock_code: str,
        as_of_date: Optional[date] = None,
    ) -> Optional[FactTechnicalIndicator]:
        key = stock_code.upper()
        rows = self._rows_by_code.get(key, [])
        if not rows:
            return None
        if as_of_date is None:
            return rows[-1]

        dates = self._dates_by_code.get(key, [])
        index = bisect_right(dates, as_of_date) - 1
        if index < 0:
            return None
        return rows[index]


class _CachedPriceRepository:
    """Trusted price lookups backed by the backtest's loaded price history."""

    TRUSTED_BAR_STATUSES = ProductionPriceRepository.TRUSTED_BAR_STATUSES
    TRUSTED_QUALITY_FLAGS = ProductionPriceRepository.TRUSTED_QUALITY_FLAGS

    def __init__(self, prices_by_stock_id: dict[int, dict[date, FactDailyPrice]]):
        self._rows_by_stock_id = {}
        self._dates_by_stock_id = {}
        for stock_id, prices_by_date in prices_by_stock_id.items():
            rows = sorted(
                prices_by_date.values(),
                key=lambda row: row.price_date,
            )
            self._rows_by_stock_id[stock_id] = rows
            self._dates_by_stock_id[stock_id] = [
                row.price_date
                for row in rows
            ]

    def get_latest_trusted_price(
        self,
        stock_id: int,
        as_of_date: Optional[date] = None,
        min_confidence: Optional[float] = 65.0,
        require_complete: bool = False,
    ) -> Optional[FactDailyPrice]:
        rows = self._candidate_rows(stock_id, as_of_date)
        for row in reversed(rows):
            if self._is_trusted(
                row,
                min_confidence=min_confidence,
                require_complete=require_complete,
                require_volume=False,
            ):
                return row
        return None

    def get_trusted_price_history(
        self,
        stock_id: int,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        limit: Optional[int] = None,
        min_confidence: Optional[float] = 65.0,
        require_complete: bool = False,
        require_volume: bool = False,
    ) -> list[FactDailyPrice]:
        rows = self._rows_by_stock_id.get(stock_id, [])
        trusted_rows = [
            row
            for row in rows
            if (start_date is None or row.price_date >= start_date)
            and (end_date is None or row.price_date <= end_date)
            and self._is_trusted(
                row,
                min_confidence=min_confidence,
                require_complete=require_complete,
                require_volume=require_volume,
            )
        ]
        trusted_rows = list(reversed(trusted_rows))
        if limit is not None:
            return trusted_rows[:limit]
        return trusted_rows

    def _candidate_rows(
        self,
        stock_id: int,
        as_of_date: Optional[date],
    ) -> list[FactDailyPrice]:
        rows = self._rows_by_stock_id.get(stock_id, [])
        if as_of_date is None:
            return rows

        dates = self._dates_by_stock_id.get(stock_id, [])
        index = bisect_right(dates, as_of_date)
        return rows[:index]

    def _is_trusted(
        self,
        row: FactDailyPrice,
        *,
        min_confidence: Optional[float],
        require_complete: bool,
        require_volume: bool,
    ) -> bool:
        if row.bar_status not in self.TRUSTED_BAR_STATUSES:
            return False
        if row.data_quality_flag not in self.TRUSTED_QUALITY_FLAGS:
            return False
        if min_confidence is not None and row.confidence_score is not None:
            if float(row.confidence_score) < min_confidence:
                return False
        if require_complete and not row.has_complete_data:
            return False
        if require_volume and row.volume is None:
            return False
        return True
