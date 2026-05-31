"""Canonical feature-engineering helpers for modeling datasets and inference."""

from __future__ import annotations

from dataclasses import dataclass
from math import sqrt
from typing import Optional, Sequence


PROBABILITY_FEATURE_NAMES = (
    "price_change_pct",
    "price_change_3d",
    "price_change_5d",
    "price_change_10d",
    "price_change_20d",
    "price_change_30d",
    "price_change_60d",
    "rsi_14_centered",
    "macd",
    "macd_signal_gap",
    "volatility_10d",
    "volatility_20d",
    "volatility_30",
    "downside_volatility_20d",
    "volume_ratio_centered",
    "avg_volume_20d_millions",
    "volume_trend_ratio_centered",
    "price_confidence_score_scaled",
    "trusted_history_days_scaled",
    "close_vs_ma_7_pct",
    "close_vs_ma_30_pct",
    "close_vs_ma_90_pct",
    "ma_7_vs_ma_30_pct",
    "ma_30_vs_ma_90_pct",
    "close_vs_20d_high_pct",
    "close_vs_60d_high_pct",
    "close_vs_20d_low_pct",
    "close_vs_60d_low_pct",
    "drawdown_20d_pct",
    "drawdown_60d_pct",
    "rebound_20d_pct",
    "rebound_60d_pct",
    "trend_strength_scaled",
    "quality_good_flag",
    "quality_incomplete_flag",
    "bar_reconciled_flag",
    "bar_official_flag",
    "is_official",
    "has_complete_data",
)


@dataclass(frozen=True)
class HistoricalFeatureSnapshot:
    """Canonical feature bundle built from anchor-date history and indicators."""

    volume_ratio: Optional[float]
    price_change_pct: Optional[float]
    price_change_3d: Optional[float]
    price_change_5d: Optional[float]
    price_change_10d: Optional[float]
    price_change_20d: Optional[float]
    price_change_30d: Optional[float]
    price_change_60d: Optional[float]
    close_vs_ma_7_pct: float
    close_vs_ma_30_pct: float
    close_vs_ma_90_pct: float
    ma_7_vs_ma_30_pct: float
    ma_30_vs_ma_90_pct: float
    close_vs_20d_high_pct: float
    close_vs_60d_high_pct: float
    close_vs_20d_low_pct: float
    close_vs_60d_low_pct: float
    drawdown_20d_pct: float
    drawdown_60d_pct: float
    rebound_20d_pct: float
    rebound_60d_pct: float
    volatility_10d: float
    volatility_20d: float
    downside_volatility_20d: float
    average_volume_20d: Optional[float]
    volume_trend_ratio: Optional[float]


def to_float(value) -> Optional[float]:
    """Convert Decimal-like values to float while preserving nulls."""
    if value is None:
        return None
    return float(value)


def safe_float(value) -> Optional[float]:
    """Safely convert numeric-like values to float for inference snapshots."""
    if value is None:
        return None
    return float(value)


def calculate_volume_ratio(
    history_through_anchor: Sequence[object],
) -> Optional[float]:
    """Calculate the anchor-date volume ratio without using future records."""
    if not history_through_anchor:
        return None

    latest_record = history_through_anchor[-1]
    latest_volume = getattr(latest_record, "volume", None)
    if latest_volume is None:
        return None

    comparison_window = history_through_anchor[-21:-1]
    historical_volumes = [
        int(price.volume)
        for price in comparison_window
        if getattr(price, "volume", None) is not None
    ]
    if not historical_volumes:
        return None

    average_volume = sum(historical_volumes) / len(historical_volumes)
    if average_volume <= 0:
        return None

    return float(int(latest_volume) / average_volume)


def calculate_backward_return(
    history_through_anchor: Sequence[object],
    lookback_sessions: int,
) -> Optional[float]:
    """Calculate a backward-looking return from the anchor-date close."""
    if len(history_through_anchor) <= lookback_sessions:
        return None

    anchor_close = to_float(getattr(history_through_anchor[-1], "close_price", None))
    prior_close = to_float(
        getattr(history_through_anchor[-1 - lookback_sessions], "close_price", None)
    )
    if anchor_close is None or prior_close in (None, 0):
        return None

    prior_close_float = float(prior_close)
    if prior_close_float <= 0:
        return None

    return ((float(anchor_close) - prior_close_float) / prior_close_float) * 100.0


def build_historical_feature_snapshot(
    *,
    history_through_anchor: Sequence[object],
    current_price,
    ma_7,
    ma_30,
    ma_90,
) -> HistoricalFeatureSnapshot:
    """Build canonical history-derived features without future leakage."""
    high_20d = rolling_close_high(history_through_anchor, 20)
    high_60d = rolling_close_high(history_through_anchor, 60)
    low_20d = rolling_close_low(history_through_anchor, 20)
    low_60d = rolling_close_low(history_through_anchor, 60)

    return HistoricalFeatureSnapshot(
        volume_ratio=calculate_volume_ratio(history_through_anchor),
        price_change_pct=calculate_backward_return(history_through_anchor, 1),
        price_change_3d=calculate_backward_return(history_through_anchor, 3),
        price_change_5d=calculate_backward_return(history_through_anchor, 5),
        price_change_10d=calculate_backward_return(history_through_anchor, 10),
        price_change_20d=calculate_backward_return(history_through_anchor, 20),
        price_change_30d=calculate_backward_return(history_through_anchor, 30),
        price_change_60d=calculate_backward_return(history_through_anchor, 60),
        close_vs_ma_7_pct=pct_distance(current_price, ma_7),
        close_vs_ma_30_pct=pct_distance(current_price, ma_30),
        close_vs_ma_90_pct=pct_distance(current_price, ma_90),
        ma_7_vs_ma_30_pct=pct_distance(ma_7, ma_30),
        ma_30_vs_ma_90_pct=pct_distance(ma_30, ma_90),
        close_vs_20d_high_pct=pct_distance(current_price, high_20d),
        close_vs_60d_high_pct=pct_distance(current_price, high_60d),
        close_vs_20d_low_pct=pct_distance(current_price, low_20d),
        close_vs_60d_low_pct=pct_distance(current_price, low_60d),
        drawdown_20d_pct=drawdown_pct(current_price, high_20d),
        drawdown_60d_pct=drawdown_pct(current_price, high_60d),
        rebound_20d_pct=rebound_pct(current_price, low_20d),
        rebound_60d_pct=rebound_pct(current_price, low_60d),
        volatility_10d=rolling_return_volatility(history_through_anchor, 10),
        volatility_20d=rolling_return_volatility(history_through_anchor, 20),
        downside_volatility_20d=downside_return_volatility(history_through_anchor, 20),
        average_volume_20d=average_volume(history_through_anchor, 20),
        volume_trend_ratio=volume_trend_ratio(history_through_anchor, 5, 20),
    )


def build_probability_feature_mapping(
    *,
    current_price,
    ma_7,
    ma_30,
    ma_90,
    rsi_14,
    macd,
    macd_signal,
    volatility_10d,
    volatility_20d,
    volatility_30,
    downside_volatility_20d,
    volume_ratio,
    average_volume_20d,
    volume_trend_ratio,
    price_change_pct,
    price_change_3d,
    price_change_5d,
    price_change_10d,
    price_change_20d,
    price_change_30d,
    price_change_60d,
    price_confidence_score,
    trusted_history_days,
    trend_strength,
    close_vs_20d_high_pct,
    close_vs_60d_high_pct,
    close_vs_20d_low_pct,
    close_vs_60d_low_pct,
    drawdown_20d_pct,
    drawdown_60d_pct,
    rebound_20d_pct,
    rebound_60d_pct,
    price_quality_flag,
    bar_status,
    is_official,
    has_complete_data,
) -> dict[str, float]:
    """Create a stable probability-feature mapping from raw model inputs."""
    return {
        "price_change_pct": coalesce(price_change_pct, 0.0),
        "price_change_3d": coalesce(price_change_3d, 0.0),
        "price_change_5d": coalesce(price_change_5d, 0.0),
        "price_change_10d": coalesce(price_change_10d, 0.0),
        "price_change_20d": coalesce(price_change_20d, 0.0),
        "price_change_30d": coalesce(price_change_30d, 0.0),
        "price_change_60d": coalesce(price_change_60d, 0.0),
        "rsi_14_centered": (coalesce(rsi_14, 50.0) - 50.0) / 50.0,
        "macd": coalesce(macd, 0.0),
        "macd_signal_gap": coalesce(macd, 0.0) - coalesce(macd_signal, 0.0),
        "volatility_10d": coalesce(volatility_10d, 0.0),
        "volatility_20d": coalesce(volatility_20d, 0.0),
        "volatility_30": coalesce(volatility_30, 0.0),
        "downside_volatility_20d": coalesce(downside_volatility_20d, 0.0),
        "volume_ratio_centered": coalesce(volume_ratio, 1.0) - 1.0,
        "avg_volume_20d_millions": coalesce(average_volume_20d, 0.0) / 1_000_000.0,
        "volume_trend_ratio_centered": coalesce(volume_trend_ratio, 1.0) - 1.0,
        "price_confidence_score_scaled": coalesce(price_confidence_score, 50.0) / 100.0,
        "trusted_history_days_scaled": coalesce(trusted_history_days, 0.0) / 100.0,
        "close_vs_ma_7_pct": pct_distance(current_price, ma_7),
        "close_vs_ma_30_pct": pct_distance(current_price, ma_30),
        "close_vs_ma_90_pct": pct_distance(current_price, ma_90),
        "ma_7_vs_ma_30_pct": pct_distance(ma_7, ma_30),
        "ma_30_vs_ma_90_pct": pct_distance(ma_30, ma_90),
        "close_vs_20d_high_pct": coalesce(close_vs_20d_high_pct, 0.0),
        "close_vs_60d_high_pct": coalesce(close_vs_60d_high_pct, 0.0),
        "close_vs_20d_low_pct": coalesce(close_vs_20d_low_pct, 0.0),
        "close_vs_60d_low_pct": coalesce(close_vs_60d_low_pct, 0.0),
        "drawdown_20d_pct": coalesce(drawdown_20d_pct, 0.0),
        "drawdown_60d_pct": coalesce(drawdown_60d_pct, 0.0),
        "rebound_20d_pct": coalesce(rebound_20d_pct, 0.0),
        "rebound_60d_pct": coalesce(rebound_60d_pct, 0.0),
        "trend_strength_scaled": coalesce(trend_strength, 50.0) / 100.0,
        "quality_good_flag": 1.0 if price_quality_flag == "GOOD" else 0.0,
        "quality_incomplete_flag": 1.0 if price_quality_flag == "INCOMPLETE" else 0.0,
        "bar_reconciled_flag": 1.0 if bar_status == "RECONCILED" else 0.0,
        "bar_official_flag": 1.0 if bar_status == "OFFICIAL" else 0.0,
        "is_official": 1.0 if is_official else 0.0,
        "has_complete_data": 1.0 if has_complete_data else 0.0,
    }


def coalesce(value, default: float) -> float:
    """Convert nullable numeric values to floats with a stable default."""
    return default if value is None else float(value)


def pct_distance(numerator, denominator) -> float:
    """Calculate percentage distance while handling missing or zero denominators."""
    if numerator is None or denominator in (None, 0):
        return 0.0

    denominator_float = float(denominator)
    if denominator_float == 0:
        return 0.0

    return ((float(numerator) - denominator_float) / denominator_float) * 100.0


def rolling_close_high(
    history_through_anchor: Sequence[object],
    window_sessions: int,
) -> Optional[float]:
    """Return the highest close in the trailing window ending at the anchor."""
    closes = _window_close_values(history_through_anchor, window_sessions)
    return max(closes) if closes else None


def rolling_close_low(
    history_through_anchor: Sequence[object],
    window_sessions: int,
) -> Optional[float]:
    """Return the lowest close in the trailing window ending at the anchor."""
    closes = _window_close_values(history_through_anchor, window_sessions)
    return min(closes) if closes else None


def drawdown_pct(current_price, rolling_high) -> float:
    """Return positive drawdown from the trailing rolling high."""
    if current_price is None or rolling_high in (None, 0):
        return 0.0

    current_price_float = float(current_price)
    rolling_high_float = float(rolling_high)
    if rolling_high_float <= 0 or current_price_float >= rolling_high_float:
        return 0.0

    return ((rolling_high_float - current_price_float) / rolling_high_float) * 100.0


def rebound_pct(current_price, rolling_low) -> float:
    """Return positive rebound from the trailing rolling low."""
    if current_price is None or rolling_low in (None, 0):
        return 0.0

    current_price_float = float(current_price)
    rolling_low_float = float(rolling_low)
    if rolling_low_float <= 0 or current_price_float <= rolling_low_float:
        return 0.0

    return ((current_price_float - rolling_low_float) / rolling_low_float) * 100.0


def _window_close_values(
    history_through_anchor: Sequence[object],
    window_sessions: int,
) -> list[float]:
    """Collect non-null closing prices from the trailing window."""
    window = history_through_anchor[-window_sessions:]
    closes = [
        to_float(getattr(price, "close_price", None))
        for price in window
    ]
    return [float(close) for close in closes if close is not None]


def rolling_return_volatility(
    history_through_anchor: Sequence[object],
    window_sessions: int,
) -> float:
    """Return the trailing volatility of daily percentage returns."""
    returns = trailing_daily_returns(history_through_anchor, window_sessions)
    return standard_deviation(returns)


def downside_return_volatility(
    history_through_anchor: Sequence[object],
    window_sessions: int,
) -> float:
    """Return trailing volatility computed only from negative daily returns."""
    negative_returns = [
        daily_return
        for daily_return in trailing_daily_returns(history_through_anchor, window_sessions)
        if daily_return < 0
    ]
    return standard_deviation(negative_returns)


def average_volume(
    history_through_anchor: Sequence[object],
    window_sessions: int,
) -> Optional[float]:
    """Return the trailing average volume across the requested window."""
    volumes = _window_volumes(history_through_anchor, window_sessions)
    if not volumes:
        return None
    return sum(volumes) / len(volumes)


def volume_trend_ratio(
    history_through_anchor: Sequence[object],
    short_window: int,
    long_window: int,
) -> Optional[float]:
    """Compare short-term volume to the longer-term trailing average."""
    short_average = average_volume(history_through_anchor, short_window)
    long_average = average_volume(history_through_anchor, long_window)
    if short_average is None or long_average in (None, 0):
        return None

    long_average_float = float(long_average)
    if long_average_float <= 0:
        return None

    return float(short_average / long_average_float)


def trailing_daily_returns(
    history_through_anchor: Sequence[object],
    window_sessions: int,
) -> list[float]:
    """Build trailing daily percentage returns from closing history."""
    closes = _window_close_values(history_through_anchor, window_sessions + 1)
    if len(closes) < 2:
        return []

    returns: list[float] = []
    for previous_close, current_close in zip(closes[:-1], closes[1:]):
        if previous_close <= 0:
            continue
        returns.append(((current_close - previous_close) / previous_close) * 100.0)
    return returns


def standard_deviation(values: Sequence[float]) -> float:
    """Calculate population standard deviation with a stable zero fallback."""
    if not values:
        return 0.0
    mean = sum(values) / len(values)
    variance = sum((value - mean) ** 2 for value in values) / len(values)
    return sqrt(variance)


def _window_volumes(
    history_through_anchor: Sequence[object],
    window_sessions: int,
) -> list[float]:
    """Collect non-null volumes from the trailing window."""
    window = history_through_anchor[-window_sessions:]
    volumes = [
        getattr(price, "volume", None)
        for price in window
    ]
    return [float(volume) for volume in volumes if volume is not None]
