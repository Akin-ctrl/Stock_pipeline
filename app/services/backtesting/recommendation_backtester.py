"""Backtest recommendation quality using historical close prices only."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import date, timedelta
from typing import Dict, List, Optional

from app.repositories import PriceRepository, StockRepository
from app.services.advisory.advisor import (
    RecommendationAction,
    RecommendationProfile,
    StockRecommendation,
    StockScreener,
)
from app.services.modeling.targets import (
    DEFAULT_DIRECTION_TARGET,
    calculate_forward_return_pct,
)


ACTIONABLE_ACTIONS = {"BUY", "STRONG_BUY"}


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
            "profit_factor": round(self.profit_factor, 2) if self.profit_factor != float("inf") else float("inf"),
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
    ):
        self.session = session
        self.price_repo = PriceRepository(session)
        self.stock_repo = StockRepository(session)
        self.screener = StockScreener(session, strategy_profile=strategy_profile)
        self.round_trip_cost_pct = round_trip_cost_pct
        self.max_abs_gross_return_pct = max_abs_gross_return_pct

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
    ) -> BacktestResult:
        stocks = self._resolve_stocks(stock_codes)
        trades: List[BacktestTrade] = []

        for stock in stocks:
            history = self.price_repo.get_price_history(
                stock.stock_id,
                start_date=start_date - timedelta(days=120),
                end_date=end_date,
            )

            if len(history) <= horizon_days:
                continue

            prices_by_date = {price.price_date: price for price in history}
            trading_dates = sorted(prices_by_date.keys())

            for index, entry_date in enumerate(trading_dates[:-horizon_days]):
                if entry_date < start_date or entry_date > end_date:
                    continue

                recommendation = self._get_recommendation(
                    stock_code=stock.stock_code,
                    recommendation_date=entry_date,
                    min_score=min_score,
                    min_confidence=min_confidence,
                    min_predicted_probability=min_predicted_probability,
                )
                if recommendation is None:
                    continue

                action_value = recommendation.action_type.value
                signal_value = recommendation.signal_type.value
                if not include_hold and action_value not in ACTIONABLE_ACTIONS:
                    continue

                exit_index = min(index + horizon_days, len(trading_dates) - 1)
                exit_date = trading_dates[exit_index]
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

    def _resolve_stocks(self, stock_codes: Optional[List[str]]):
        if stock_codes:
            stocks = [self.stock_repo.get_by_code(code) for code in stock_codes]
            return [stock for stock in stocks if stock is not None]
        return self.stock_repo.get_all_active()

    def _get_recommendation(
        self,
        stock_code: str,
        recommendation_date: date,
        min_score: Optional[float],
        min_confidence: Optional[float],
        min_predicted_probability: Optional[float],
    ) -> Optional[StockRecommendation]:
        recommendations = self.screener.generate_recommendations(
            recommendation_date=recommendation_date,
            stock_codes=[stock_code],
            min_score=min_score,
            min_confidence=min_confidence,
            min_predicted_probability=min_predicted_probability,
        )
        return recommendations[0] if recommendations else None

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
