"""Portfolio-level simulation for recommendation backtest trades."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import date, timedelta
from math import isinf
from typing import Sequence

from app.services.backtesting.recommendation_backtester import BacktestTrade


@dataclass(frozen=True)
class PortfolioSimulationConfig:
    """Capital allocation and risk-control settings for portfolio simulation."""

    initial_capital: float = 1_000_000.0
    max_concurrent_positions: int = 3
    max_entries_per_day: int = 1
    position_size_pct: float = 0.20
    cooldown_days_after_loss: int = 0
    consecutive_loss_limit: int = 0
    cooldown_days_after_consecutive_losses: int = 0

    def validate(self) -> None:
        """Validate simulator settings before capital is allocated."""
        if self.initial_capital <= 0:
            raise ValueError("initial_capital must be positive")
        if self.max_concurrent_positions <= 0:
            raise ValueError("max_concurrent_positions must be positive")
        if self.max_entries_per_day <= 0:
            raise ValueError("max_entries_per_day must be positive")
        if not 0 < self.position_size_pct <= 1:
            raise ValueError("position_size_pct must be within (0, 1]")
        if self.cooldown_days_after_loss < 0:
            raise ValueError("cooldown_days_after_loss cannot be negative")
        if self.consecutive_loss_limit < 0:
            raise ValueError("consecutive_loss_limit cannot be negative")
        if self.cooldown_days_after_consecutive_losses < 0:
            raise ValueError(
                "cooldown_days_after_consecutive_losses cannot be negative"
            )


@dataclass(frozen=True)
class PortfolioPosition:
    """One accepted trade allocated by the portfolio simulator."""

    stock_code: str
    entry_date: date
    exit_date: date
    allocated_capital: float
    net_return_pct: float
    realized_pnl: float
    exit_value: float


@dataclass(frozen=True)
class PortfolioEquityPoint:
    """Portfolio equity snapshot at a simulation event date."""

    event_date: date
    cash: float
    open_position_capital: float
    equity: float
    drawdown_pct: float
    open_positions: int


@dataclass(frozen=True)
class PortfolioSimulationResult:
    """Portfolio-level result derived from a sequence of backtest trades."""

    config: PortfolioSimulationConfig
    start_date: date | None
    end_date: date | None
    accepted_positions: list[PortfolioPosition] = field(default_factory=list)
    skipped_trade_count: int = 0
    skipped_reasons: dict[str, int] = field(default_factory=dict)
    equity_curve: list[PortfolioEquityPoint] = field(default_factory=list)

    @property
    def final_equity(self) -> float:
        if not self.equity_curve:
            return self.config.initial_capital
        return self.equity_curve[-1].equity

    @property
    def total_return_pct(self) -> float:
        return (
            (self.final_equity - self.config.initial_capital)
            / self.config.initial_capital
            * 100.0
        )

    @property
    def max_drawdown_pct(self) -> float:
        if not self.equity_curve:
            return 0.0
        return max(point.drawdown_pct for point in self.equity_curve)

    @property
    def realized_trade_count(self) -> int:
        return len(self.accepted_positions)

    @property
    def win_rate_pct(self) -> float:
        if not self.accepted_positions:
            return 0.0
        wins = sum(1 for position in self.accepted_positions if position.realized_pnl > 0)
        return wins / len(self.accepted_positions) * 100.0

    @property
    def average_position_return_pct(self) -> float:
        if not self.accepted_positions:
            return 0.0
        return (
            sum(position.net_return_pct for position in self.accepted_positions)
            / len(self.accepted_positions)
        )

    @property
    def profit_factor(self) -> float:
        gross_profit = sum(
            position.realized_pnl
            for position in self.accepted_positions
            if position.realized_pnl > 0
        )
        gross_loss = abs(
            sum(
                position.realized_pnl
                for position in self.accepted_positions
                if position.realized_pnl < 0
            )
        )
        if gross_loss > 0:
            return gross_profit / gross_loss
        return float("inf") if gross_profit > 0 else 0.0

    def to_dict(self) -> dict:
        """Serialize the portfolio simulation result to plain JSON-compatible values."""
        return {
            "config": asdict(self.config),
            "start_date": self.start_date.isoformat() if self.start_date else None,
            "end_date": self.end_date.isoformat() if self.end_date else None,
            "initial_capital": round(self.config.initial_capital, 2),
            "final_equity": round(self.final_equity, 2),
            "total_return_pct": round(self.total_return_pct, 2),
            "max_drawdown_pct": round(self.max_drawdown_pct, 2),
            "realized_trade_count": self.realized_trade_count,
            "skipped_trade_count": self.skipped_trade_count,
            "skipped_reasons": dict(self.skipped_reasons),
            "win_rate_pct": round(self.win_rate_pct, 2),
            "average_position_return_pct": round(
                self.average_position_return_pct,
                2,
            ),
            "profit_factor": _json_profit_factor(self.profit_factor),
            "profit_factor_unbounded": isinf(self.profit_factor),
            "accepted_positions": [
                _position_to_dict(position)
                for position in self.accepted_positions
            ],
            "equity_curve": [
                _equity_point_to_dict(point)
                for point in self.equity_curve
            ],
        }


@dataclass
class _OpenPosition:
    trade: BacktestTrade
    allocated_capital: float


class PortfolioSimulator:
    """Simulate portfolio allocation and risk controls over backtest trades."""

    def __init__(self, config: PortfolioSimulationConfig | None = None):
        self.config = config or PortfolioSimulationConfig()
        self.config.validate()

    def simulate(
        self,
        trades: Sequence[BacktestTrade],
    ) -> PortfolioSimulationResult:
        """Allocate capital to trades while enforcing portfolio-level controls."""
        ordered_trades = sorted(
            trades,
            key=lambda trade: (
                trade.entry_date,
                -trade.score,
                -trade.confidence,
                trade.stock_code,
            ),
        )
        if not ordered_trades:
            return PortfolioSimulationResult(
                config=self.config,
                start_date=None,
                end_date=None,
            )

        state = _SimulationState(config=self.config)
        accepted_positions: list[PortfolioPosition] = []
        skipped_reasons: dict[str, int] = {}
        equity_curve: list[PortfolioEquityPoint] = []

        for entry_date in sorted({trade.entry_date for trade in ordered_trades}):
            self._close_due_positions(
                state=state,
                event_date=entry_date,
                accepted_positions=accepted_positions,
            )
            self._record_equity_point(state, entry_date, equity_curve)

            daily_entries = 0
            day_trades = [
                trade for trade in ordered_trades if trade.entry_date == entry_date
            ]
            for trade in day_trades:
                rejection_reason = self._entry_rejection_reason(
                    state=state,
                    trade=trade,
                    daily_entries=daily_entries,
                )
                if rejection_reason is not None:
                    skipped_reasons[rejection_reason] = (
                        skipped_reasons.get(rejection_reason, 0) + 1
                    )
                    continue

                allocation = min(
                    state.equity * self.config.position_size_pct,
                    state.cash,
                )
                if allocation <= 0:
                    skipped_reasons["insufficient_cash"] = (
                        skipped_reasons.get("insufficient_cash", 0) + 1
                    )
                    continue

                state.cash -= allocation
                state.open_positions.append(
                    _OpenPosition(trade=trade, allocated_capital=allocation)
                )
                daily_entries += 1
                self._record_equity_point(state, entry_date, equity_curve)

        for exit_date in sorted({position.trade.exit_date for position in state.open_positions}):
            self._close_due_positions(
                state=state,
                event_date=exit_date,
                accepted_positions=accepted_positions,
            )
            self._record_equity_point(state, exit_date, equity_curve)

        return PortfolioSimulationResult(
            config=self.config,
            start_date=ordered_trades[0].entry_date,
            end_date=max(trade.exit_date for trade in ordered_trades),
            accepted_positions=accepted_positions,
            skipped_trade_count=sum(skipped_reasons.values()),
            skipped_reasons=skipped_reasons,
            equity_curve=equity_curve,
        )

    def _entry_rejection_reason(
        self,
        *,
        state: "_SimulationState",
        trade: BacktestTrade,
        daily_entries: int,
    ) -> str | None:
        if trade.entry_date < state.cooldown_until:
            return "cooldown_active"
        if daily_entries >= self.config.max_entries_per_day:
            return "daily_entry_limit"
        if len(state.open_positions) >= self.config.max_concurrent_positions:
            return "max_concurrent_positions"
        return None

    def _close_due_positions(
        self,
        *,
        state: "_SimulationState",
        event_date: date,
        accepted_positions: list[PortfolioPosition],
    ) -> None:
        remaining_positions = []
        for open_position in state.open_positions:
            trade = open_position.trade
            if trade.exit_date > event_date:
                remaining_positions.append(open_position)
                continue

            exit_value = open_position.allocated_capital * (
                1.0 + trade.net_return_pct / 100.0
            )
            realized_pnl = exit_value - open_position.allocated_capital
            state.cash += exit_value
            accepted_positions.append(
                PortfolioPosition(
                    stock_code=trade.stock_code,
                    entry_date=trade.entry_date,
                    exit_date=trade.exit_date,
                    allocated_capital=round(open_position.allocated_capital, 4),
                    net_return_pct=trade.net_return_pct,
                    realized_pnl=round(realized_pnl, 4),
                    exit_value=round(exit_value, 4),
                )
            )
            self._update_cooldown_after_exit(
                state=state,
                exit_date=trade.exit_date,
                realized_pnl=realized_pnl,
            )
        state.open_positions = remaining_positions

    def _update_cooldown_after_exit(
        self,
        *,
        state: "_SimulationState",
        exit_date: date,
        realized_pnl: float,
    ) -> None:
        if realized_pnl < 0:
            state.consecutive_losses += 1
            if self.config.cooldown_days_after_loss > 0:
                state.cooldown_until = max(
                    state.cooldown_until,
                    exit_date + timedelta(days=self.config.cooldown_days_after_loss),
                )
            if (
                self.config.consecutive_loss_limit > 0
                and state.consecutive_losses >= self.config.consecutive_loss_limit
                and self.config.cooldown_days_after_consecutive_losses > 0
            ):
                state.cooldown_until = max(
                    state.cooldown_until,
                    exit_date
                    + timedelta(
                        days=self.config.cooldown_days_after_consecutive_losses
                    ),
                )
                state.consecutive_losses = 0
        elif realized_pnl > 0:
            state.consecutive_losses = 0

    def _record_equity_point(
        self,
        state: "_SimulationState",
        event_date: date,
        equity_curve: list[PortfolioEquityPoint],
    ) -> None:
        equity = state.equity
        state.peak_equity = max(state.peak_equity, equity)
        drawdown_pct = (
            (state.peak_equity - equity) / state.peak_equity * 100.0
            if state.peak_equity > 0
            else 0.0
        )
        point = PortfolioEquityPoint(
            event_date=event_date,
            cash=round(state.cash, 4),
            open_position_capital=round(state.open_position_capital, 4),
            equity=round(equity, 4),
            drawdown_pct=round(drawdown_pct, 4),
            open_positions=len(state.open_positions),
        )
        if equity_curve and equity_curve[-1] == point:
            return
        equity_curve.append(point)


@dataclass
class _SimulationState:
    config: PortfolioSimulationConfig
    cash: float = field(init=False)
    peak_equity: float = field(init=False)
    cooldown_until: date = field(default=date.min)
    consecutive_losses: int = 0
    open_positions: list[_OpenPosition] = field(default_factory=list)

    def __post_init__(self) -> None:
        self.cash = self.config.initial_capital
        self.peak_equity = self.config.initial_capital

    @property
    def open_position_capital(self) -> float:
        return sum(position.allocated_capital for position in self.open_positions)

    @property
    def equity(self) -> float:
        return self.cash + self.open_position_capital


def _position_to_dict(position: PortfolioPosition) -> dict:
    return {
        "stock_code": position.stock_code,
        "entry_date": position.entry_date.isoformat(),
        "exit_date": position.exit_date.isoformat(),
        "allocated_capital": round(position.allocated_capital, 2),
        "net_return_pct": round(position.net_return_pct, 4),
        "realized_pnl": round(position.realized_pnl, 2),
        "exit_value": round(position.exit_value, 2),
    }


def _equity_point_to_dict(point: PortfolioEquityPoint) -> dict:
    return {
        "event_date": point.event_date.isoformat(),
        "cash": round(point.cash, 2),
        "open_position_capital": round(point.open_position_capital, 2),
        "equity": round(point.equity, 2),
        "drawdown_pct": round(point.drawdown_pct, 2),
        "open_positions": point.open_positions,
    }


def _json_profit_factor(profit_factor: float) -> float | None:
    """Return strict-JSON profit factor; unbounded values are represented as null."""
    if isinf(profit_factor):
        return None
    return round(profit_factor, 2)
