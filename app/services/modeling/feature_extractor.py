"""Shared feature extraction for probability estimation."""

from __future__ import annotations

from typing import Mapping

from app.services.modeling.dataset_builder import ModelingDatasetRow
from app.services.modeling.feature_engineering import (
    PROBABILITY_FEATURE_NAMES,
    build_probability_feature_mapping,
    safe_float,
)


def extract_probability_features_from_row(row: ModelingDatasetRow) -> dict[str, float]:
    """Build the canonical probability feature mapping from a dataset row."""
    return build_probability_feature_mapping(
        current_price=row.close_price,
        ma_7=row.ma_7,
        ma_30=row.ma_30,
        ma_90=row.ma_90,
        rsi_14=row.rsi_14,
        macd=row.macd,
        macd_signal=row.macd_signal,
        volatility_10d=row.volatility_10d,
        volatility_20d=row.volatility_20d,
        volatility_30=row.volatility_30,
        downside_volatility_20d=row.downside_volatility_20d,
        volume_ratio=row.volume_ratio,
        average_volume_20d=row.average_volume_20d,
        volume_trend_ratio=row.volume_trend_ratio,
        price_change_pct=row.price_change_pct,
        price_change_3d=row.price_change_3d,
        price_change_5d=row.price_change_5d,
        price_change_10d=row.price_change_10d,
        price_change_20d=row.price_change_20d,
        price_change_30d=row.price_change_30d,
        price_change_60d=row.price_change_60d,
        price_confidence_score=row.price_confidence_score,
        trusted_history_days=row.trusted_history_days,
        trend_strength=row.trend_strength,
        close_vs_20d_high_pct=row.close_vs_20d_high_pct,
        close_vs_60d_high_pct=row.close_vs_60d_high_pct,
        close_vs_20d_low_pct=row.close_vs_20d_low_pct,
        close_vs_60d_low_pct=row.close_vs_60d_low_pct,
        drawdown_20d_pct=row.drawdown_20d_pct,
        drawdown_60d_pct=row.drawdown_60d_pct,
        rebound_20d_pct=row.rebound_20d_pct,
        rebound_60d_pct=row.rebound_60d_pct,
        price_quality_flag=row.price_quality_flag,
        bar_status=row.bar_status,
        is_official=row.is_official,
        has_complete_data=row.has_complete_data,
    )


def extract_probability_features_from_snapshot(
    feature_snapshot: Mapping[str, object],
) -> dict[str, float]:
    """Build the canonical probability feature mapping from a live recommendation snapshot."""
    indicators = feature_snapshot.get("indicators", {})
    if not isinstance(indicators, Mapping):
        indicators = {}

    return build_probability_feature_mapping(
        current_price=safe_float(indicators.get("current_price")),
        ma_7=safe_float(indicators.get("ma_7")),
        ma_30=safe_float(indicators.get("ma_30")),
        ma_90=safe_float(indicators.get("ma_90")),
        rsi_14=safe_float(indicators.get("rsi_14")),
        macd=safe_float(indicators.get("macd")),
        macd_signal=safe_float(indicators.get("macd_signal")),
        volatility_10d=safe_float(indicators.get("volatility_10d")),
        volatility_20d=safe_float(indicators.get("volatility_20d")),
        volatility_30=safe_float(indicators.get("volatility")),
        downside_volatility_20d=safe_float(indicators.get("downside_volatility_20d")),
        volume_ratio=safe_float(indicators.get("volume_ratio")),
        average_volume_20d=safe_float(indicators.get("average_volume_20d")),
        volume_trend_ratio=safe_float(indicators.get("volume_trend_ratio")),
        price_change_pct=safe_float(indicators.get("price_change_pct")),
        price_change_3d=safe_float(indicators.get("price_change_3d")),
        price_change_5d=safe_float(indicators.get("price_change_5d")),
        price_change_10d=safe_float(indicators.get("price_change_10d")),
        price_change_20d=safe_float(indicators.get("price_change_20d")),
        price_change_30d=safe_float(indicators.get("price_change_30d")),
        price_change_60d=safe_float(indicators.get("price_change_60d")),
        price_confidence_score=safe_float(indicators.get("price_confidence_score")),
        trusted_history_days=safe_float(indicators.get("trusted_history_days")),
        trend_strength=safe_float(indicators.get("trend_strength")),
        close_vs_20d_high_pct=safe_float(indicators.get("close_vs_20d_high_pct")),
        close_vs_60d_high_pct=safe_float(indicators.get("close_vs_60d_high_pct")),
        close_vs_20d_low_pct=safe_float(indicators.get("close_vs_20d_low_pct")),
        close_vs_60d_low_pct=safe_float(indicators.get("close_vs_60d_low_pct")),
        drawdown_20d_pct=safe_float(indicators.get("drawdown_20d_pct")),
        drawdown_60d_pct=safe_float(indicators.get("drawdown_60d_pct")),
        rebound_20d_pct=safe_float(indicators.get("rebound_20d_pct")),
        rebound_60d_pct=safe_float(indicators.get("rebound_60d_pct")),
        price_quality_flag=indicators.get("price_quality_flag"),
        bar_status=indicators.get("bar_status"),
        is_official=bool(indicators.get("is_official", False)),
        has_complete_data=bool(indicators.get("has_complete_data", False)),
    )


def feature_vector_from_mapping(feature_mapping: Mapping[str, float]) -> list[float]:
    """Convert a feature mapping to an ordered numeric vector."""
    return [float(feature_mapping[name]) for name in PROBABILITY_FEATURE_NAMES]
