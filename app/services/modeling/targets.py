"""Shared forward-return target contracts for model training and evaluation."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from typing import Union


Numeric = Union[Decimal, float, int]


@dataclass(frozen=True)
class DirectionTargetDefinition:
    """Canonical directional target definition for stock-date modeling rows."""

    horizon_trading_days: int = 10
    positive_threshold_pct: float = 0.0


@dataclass(frozen=True)
class ForwardReturnLabel:
    """Outcome label built from anchor and horizon close prices."""

    anchor_date: date
    horizon_date: date
    anchor_close_price: float
    horizon_close_price: float
    forward_return_pct: float
    target_up: int


DEFAULT_DIRECTION_TARGET = DirectionTargetDefinition()


def calculate_forward_return_pct(anchor_close_price: Numeric, horizon_close_price: Numeric) -> float:
    """Calculate percentage return between two closing prices."""
    anchor_price = float(anchor_close_price)
    future_price = float(horizon_close_price)

    if anchor_price <= 0:
        raise ValueError("anchor_close_price must be positive")

    return ((future_price - anchor_price) / anchor_price) * 100.0


def build_forward_return_label(
    anchor_date: date,
    anchor_close_price: Numeric,
    horizon_date: date,
    horizon_close_price: Numeric,
    target_definition: DirectionTargetDefinition = DEFAULT_DIRECTION_TARGET,
) -> ForwardReturnLabel:
    """Build the canonical forward-return label for one anchor date."""
    forward_return_pct = calculate_forward_return_pct(anchor_close_price, horizon_close_price)
    target_up = int(forward_return_pct > target_definition.positive_threshold_pct)

    return ForwardReturnLabel(
        anchor_date=anchor_date,
        horizon_date=horizon_date,
        anchor_close_price=float(anchor_close_price),
        horizon_close_price=float(horizon_close_price),
        forward_return_pct=forward_return_pct,
        target_up=target_up,
    )
