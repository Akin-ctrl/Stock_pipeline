"""Validation helpers for probability feature health and train/inference alignment."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Mapping, Optional, Sequence

from app.services.modeling.dataset_builder import ModelingDatasetRow
from app.services.modeling.feature_engineering import PROBABILITY_FEATURE_NAMES
from app.services.modeling.feature_extractor import (
    extract_probability_features_from_row,
    extract_probability_features_from_snapshot,
)


@dataclass(frozen=True)
class FeatureHealthStat:
    """Health summary for one probability feature."""

    raw_missing_count: int
    raw_missing_rate: float
    distinct_mapped_count: int
    constant_mapped: bool
    all_zero_mapped: bool
    min_mapped: float
    max_mapped: float
    mean_mapped: float


@dataclass(frozen=True)
class FeatureValidationSummary:
    """Validation summary for the current probability feature contract."""

    row_count: int
    feature_count: int
    feature_stats: Dict[str, FeatureHealthStat]
    high_missing_features: tuple[str, ...]
    constant_features: tuple[str, ...]
    zero_only_features: tuple[str, ...]


@dataclass(frozen=True)
class FeatureAlignmentReport:
    """Comparison report between row-based and snapshot-based feature extraction."""

    feature_count: int
    missing_from_row: tuple[str, ...]
    missing_from_snapshot: tuple[str, ...]
    mismatches: Dict[str, tuple[float, float]]

    @property
    def is_aligned(self) -> bool:
        """Return True when the two extraction paths agree on all features."""
        return (
            not self.missing_from_row
            and not self.missing_from_snapshot
            and not self.mismatches
        )


def summarize_probability_features(
    rows: Sequence[ModelingDatasetRow],
    *,
    high_missing_threshold: float = 0.25,
) -> FeatureValidationSummary:
    """Summarize probability feature health across model-ready rows."""
    if not rows:
        return FeatureValidationSummary(
            row_count=0,
            feature_count=len(PROBABILITY_FEATURE_NAMES),
            feature_stats={},
            high_missing_features=(),
            constant_features=(),
            zero_only_features=(),
        )

    feature_stats: Dict[str, FeatureHealthStat] = {}
    high_missing_features = []
    constant_features = []
    zero_only_features = []

    row_raw_inputs = [_raw_probability_feature_sources_from_row(row) for row in rows]
    row_feature_mappings = [extract_probability_features_from_row(row) for row in rows]

    for feature_name in PROBABILITY_FEATURE_NAMES:
        raw_values = [raw_inputs[feature_name] for raw_inputs in row_raw_inputs]
        mapped_values = [
            float(feature_mapping[feature_name])
            for feature_mapping in row_feature_mappings
        ]

        raw_missing_count = sum(1 for value in raw_values if value is None)
        raw_missing_rate = raw_missing_count / len(rows)
        distinct_mapped_values = {round(value, 12) for value in mapped_values}
        constant_mapped = len(distinct_mapped_values) == 1
        all_zero_mapped = all(abs(value) < 1e-12 for value in mapped_values)
        mean_mapped = sum(mapped_values) / len(mapped_values)

        feature_stats[feature_name] = FeatureHealthStat(
            raw_missing_count=raw_missing_count,
            raw_missing_rate=raw_missing_rate,
            distinct_mapped_count=len(distinct_mapped_values),
            constant_mapped=constant_mapped,
            all_zero_mapped=all_zero_mapped,
            min_mapped=min(mapped_values),
            max_mapped=max(mapped_values),
            mean_mapped=mean_mapped,
        )

        if raw_missing_rate >= high_missing_threshold:
            high_missing_features.append(feature_name)
        if constant_mapped:
            constant_features.append(feature_name)
        if all_zero_mapped:
            zero_only_features.append(feature_name)

    return FeatureValidationSummary(
        row_count=len(rows),
        feature_count=len(PROBABILITY_FEATURE_NAMES),
        feature_stats=feature_stats,
        high_missing_features=tuple(sorted(high_missing_features)),
        constant_features=tuple(sorted(constant_features)),
        zero_only_features=tuple(sorted(zero_only_features)),
    )


def validate_probability_feature_alignment(
    row: ModelingDatasetRow,
    feature_snapshot: Mapping[str, object],
    *,
    tolerance: float = 1e-9,
) -> FeatureAlignmentReport:
    """Validate that row-based and snapshot-based extraction agree."""
    row_features = extract_probability_features_from_row(row)
    snapshot_features = extract_probability_features_from_snapshot(feature_snapshot)

    row_names = set(row_features.keys())
    snapshot_names = set(snapshot_features.keys())
    missing_from_row = tuple(sorted(snapshot_names - row_names))
    missing_from_snapshot = tuple(sorted(row_names - snapshot_names))

    mismatches: Dict[str, tuple[float, float]] = {}
    for feature_name in sorted(row_names & snapshot_names):
        row_value = float(row_features[feature_name])
        snapshot_value = float(snapshot_features[feature_name])
        if abs(row_value - snapshot_value) > tolerance:
            mismatches[feature_name] = (row_value, snapshot_value)

    return FeatureAlignmentReport(
        feature_count=len(PROBABILITY_FEATURE_NAMES),
        missing_from_row=missing_from_row,
        missing_from_snapshot=missing_from_snapshot,
        mismatches=mismatches,
    )


def _raw_probability_feature_sources_from_row(
    row: ModelingDatasetRow,
) -> Dict[str, object]:
    """Expose raw feature-source values before numeric coalescing."""
    return {
        "price_change_pct": row.price_change_pct,
        "price_change_3d": row.price_change_3d,
        "price_change_5d": row.price_change_5d,
        "price_change_10d": row.price_change_10d,
        "price_change_20d": row.price_change_20d,
        "price_change_30d": row.price_change_30d,
        "price_change_60d": row.price_change_60d,
        "rsi_14_centered": row.rsi_14,
        "macd": row.macd,
        "macd_signal_gap": row.macd_signal,
        "volatility_10d": row.volatility_10d,
        "volatility_20d": row.volatility_20d,
        "volatility_30": row.volatility_30,
        "downside_volatility_20d": row.downside_volatility_20d,
        "volume_ratio_centered": row.volume_ratio,
        "avg_volume_20d_millions": row.average_volume_20d,
        "volume_trend_ratio_centered": row.volume_trend_ratio,
        "price_confidence_score_scaled": row.price_confidence_score,
        "trusted_history_days_scaled": row.trusted_history_days,
        "close_vs_ma_7_pct": row.ma_7,
        "close_vs_ma_30_pct": row.ma_30,
        "close_vs_ma_90_pct": row.ma_90,
        "ma_7_vs_ma_30_pct": row.ma_7 if row.ma_30 is not None else None,
        "ma_30_vs_ma_90_pct": row.ma_30 if row.ma_90 is not None else None,
        "close_vs_20d_high_pct": row.close_vs_20d_high_pct,
        "close_vs_60d_high_pct": row.close_vs_60d_high_pct,
        "close_vs_20d_low_pct": row.close_vs_20d_low_pct,
        "close_vs_60d_low_pct": row.close_vs_60d_low_pct,
        "drawdown_20d_pct": row.drawdown_20d_pct,
        "drawdown_60d_pct": row.drawdown_60d_pct,
        "rebound_20d_pct": row.rebound_20d_pct,
        "rebound_60d_pct": row.rebound_60d_pct,
        "trend_strength_scaled": row.trend_strength,
        "quality_good_flag": row.price_quality_flag,
        "quality_incomplete_flag": row.price_quality_flag,
        "bar_reconciled_flag": row.bar_status,
        "bar_official_flag": row.bar_status,
        "is_official": row.is_official,
        "has_complete_data": row.has_complete_data,
    }
