"""Empirical validation helpers for trust and data-quality signals."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Callable, Iterable, Optional, Sequence

from app.repositories import PriceRepository
from app.services.modeling.dataset_builder import (
    ModelingDatasetBuilder,
    ModelingDatasetConfig,
    ModelingDatasetRow,
)


ALL_BAR_STATUSES = ("OBSERVED", "RECONCILED", "OFFICIAL", "ESTIMATED")
ALL_QUALITY_FLAGS = ("GOOD", "INCOMPLETE", "SUSPICIOUS", "MISSING", "STALE", "POOR")


@dataclass(frozen=True)
class TrustCohortStat:
    """Outcome summary for one trust cohort."""

    label: str
    row_count: int
    retained_pct: float
    positive_rate: float
    average_forward_return_10d: float
    average_confidence_score: float
    average_trusted_history_days: float


@dataclass(frozen=True)
class TrustFilterComparison:
    """Quality-versus-coverage comparison for one trust filter."""

    label: str
    row_count: int
    retained_pct: float
    positive_rate: float
    average_forward_return_10d: float
    average_confidence_score: float


@dataclass(frozen=True)
class TrustValidationReport:
    """Aggregate empirical summary of trust-related cohorts and filters."""

    overall_row_count: int
    confidence_band_stats: tuple[TrustCohortStat, ...]
    quality_flag_stats: tuple[TrustCohortStat, ...]
    bar_status_stats: tuple[TrustCohortStat, ...]
    completeness_stats: tuple[TrustCohortStat, ...]
    official_stats: tuple[TrustCohortStat, ...]
    history_threshold_stats: tuple[TrustCohortStat, ...]
    filter_comparisons: tuple[TrustFilterComparison, ...]


class TrustValidator:
    """Evaluate trust cohorts and stricter trust filters against realized outcomes."""

    def __init__(
        self,
        session,
        *,
        dataset_builder: Optional[ModelingDatasetBuilder] = None,
        confidence_bands: Sequence[tuple[Optional[float], Optional[float], str]] = (
            (None, 70.0, "<70"),
            (70.0, 85.0, "70-84.99"),
            (85.0, None, "85+"),
        ),
        history_thresholds: Sequence[int] = (30, 60),
    ):
        self.session = session
        self.dataset_builder = dataset_builder or ModelingDatasetBuilder(
            session,
            config=ModelingDatasetConfig(
                allowed_bar_statuses=ALL_BAR_STATUSES,
                allowed_quality_flags=ALL_QUALITY_FLAGS,
                require_complete_data=False,
                min_confidence_score=None,
            ),
        )
        self.confidence_bands = tuple(confidence_bands)
        self.history_thresholds = tuple(history_thresholds)

    def run(
        self,
        *,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        stock_codes: Optional[Sequence[str]] = None,
    ) -> TrustValidationReport:
        """Build canonical rows for the requested window and summarize trust performance."""
        rows = self.dataset_builder.build(
            start_date=start_date,
            end_date=end_date,
            stock_codes=stock_codes,
        )
        return self.summarize_rows(rows)

    def summarize_rows(
        self,
        rows: Sequence[ModelingDatasetRow],
    ) -> TrustValidationReport:
        """Summarize trust cohorts and filter tradeoffs over model-ready rows."""
        total_count = len(rows)

        return TrustValidationReport(
            overall_row_count=total_count,
            confidence_band_stats=tuple(
                build_confidence_band_stats(rows, bands=self.confidence_bands)
            ),
            quality_flag_stats=tuple(
                build_categorical_cohort_stats(rows, attribute_name="price_quality_flag")
            ),
            bar_status_stats=tuple(
                build_categorical_cohort_stats(rows, attribute_name="bar_status")
            ),
            completeness_stats=tuple(
                build_boolean_cohort_stats(rows, attribute_name="has_complete_data")
            ),
            official_stats=tuple(
                build_boolean_cohort_stats(rows, attribute_name="is_official")
            ),
            history_threshold_stats=tuple(
                build_history_threshold_stats(rows, thresholds=self.history_thresholds)
            ),
            filter_comparisons=tuple(build_standard_trust_filter_comparisons(rows)),
        )


def build_confidence_band_stats(
    rows: Sequence[ModelingDatasetRow],
    *,
    bands: Sequence[tuple[Optional[float], Optional[float], str]],
) -> list[TrustCohortStat]:
    """Group rows into confidence-score bands and summarize outcomes."""
    total_count = len(rows)
    stats: list[TrustCohortStat] = []

    for lower_bound, upper_bound, label in bands:
        cohort_rows = [
            row
            for row in rows
            if _is_within_band(row.price_confidence_score, lower_bound, upper_bound)
        ]
        stats.append(_build_cohort_stat(label, cohort_rows, total_count))

    return stats


def build_categorical_cohort_stats(
    rows: Sequence[ModelingDatasetRow],
    *,
    attribute_name: str,
) -> list[TrustCohortStat]:
    """Group rows by one categorical trust field and summarize outcomes."""
    total_count = len(rows)
    labels = sorted({str(getattr(row, attribute_name)) for row in rows})
    return [
        _build_cohort_stat(
            label,
            [row for row in rows if str(getattr(row, attribute_name)) == label],
            total_count,
        )
        for label in labels
    ]


def build_boolean_cohort_stats(
    rows: Sequence[ModelingDatasetRow],
    *,
    attribute_name: str,
) -> list[TrustCohortStat]:
    """Group rows by one boolean trust field and summarize outcomes."""
    total_count = len(rows)
    stats: list[TrustCohortStat] = []
    for value in (True, False):
        stats.append(
            _build_cohort_stat(
                f"{attribute_name}={value}",
                [row for row in rows if bool(getattr(row, attribute_name)) is value],
                total_count,
            )
        )
    return stats


def build_history_threshold_stats(
    rows: Sequence[ModelingDatasetRow],
    *,
    thresholds: Sequence[int],
) -> list[TrustCohortStat]:
    """Summarize rows that meet or miss configured trusted-history thresholds."""
    total_count = len(rows)
    stats: list[TrustCohortStat] = []
    for threshold in thresholds:
        stats.append(
            _build_cohort_stat(
                f"trusted_history_days>={threshold}",
                [row for row in rows if row.trusted_history_days >= threshold],
                total_count,
            )
        )
    return stats


def build_standard_trust_filter_comparisons(
    rows: Sequence[ModelingDatasetRow],
) -> list[TrustFilterComparison]:
    """Compare a standard set of stricter trust filters against the full dataset."""
    total_count = len(rows)
    comparisons = [
        ("baseline_all_rows", lambda row: True),
        ("min_confidence_score_60", lambda row: _meets_min_confidence(row, 60.0)),
        ("min_confidence_score_80", lambda row: _meets_min_confidence(row, 80.0)),
        ("quality_flag_good_only", lambda row: row.price_quality_flag == "GOOD"),
        (
            "bar_status_reconciled_or_official",
            lambda row: row.bar_status in PriceRepository.TRUSTED_BAR_STATUSES,
        ),
        ("has_complete_data_only", lambda row: row.has_complete_data),
    ]

    return [
        _build_filter_comparison(
            label,
            [row for row in rows if predicate(row)],
            total_count,
        )
        for label, predicate in comparisons
    ]


def _build_cohort_stat(
    label: str,
    cohort_rows: Sequence[ModelingDatasetRow],
    total_count: int,
) -> TrustCohortStat:
    """Summarize one cohort against the full row set."""
    row_count = len(cohort_rows)
    return TrustCohortStat(
        label=label,
        row_count=row_count,
        retained_pct=(row_count / total_count) * 100.0 if total_count else 0.0,
        positive_rate=(
            sum(row.target_up_10d for row in cohort_rows) / row_count
            if row_count
            else 0.0
        ),
        average_forward_return_10d=(
            sum(row.forward_return_10d for row in cohort_rows) / row_count
            if row_count
            else 0.0
        ),
        average_confidence_score=(
            sum(float(row.price_confidence_score or 0.0) for row in cohort_rows) / row_count
            if row_count
            else 0.0
        ),
        average_trusted_history_days=(
            sum(row.trusted_history_days for row in cohort_rows) / row_count
            if row_count
            else 0.0
        ),
    )


def _build_filter_comparison(
    label: str,
    cohort_rows: Sequence[ModelingDatasetRow],
    total_count: int,
) -> TrustFilterComparison:
    """Summarize one stricter trust filter against the full dataset."""
    row_count = len(cohort_rows)
    return TrustFilterComparison(
        label=label,
        row_count=row_count,
        retained_pct=(row_count / total_count) * 100.0 if total_count else 0.0,
        positive_rate=(
            sum(row.target_up_10d for row in cohort_rows) / row_count
            if row_count
            else 0.0
        ),
        average_forward_return_10d=(
            sum(row.forward_return_10d for row in cohort_rows) / row_count
            if row_count
            else 0.0
        ),
        average_confidence_score=(
            sum(float(row.price_confidence_score or 0.0) for row in cohort_rows) / row_count
            if row_count
            else 0.0
        ),
    )


def _is_within_band(
    value: Optional[float],
    lower_bound: Optional[float],
    upper_bound: Optional[float],
) -> bool:
    """Return True when the optional value lies inside the configured band."""
    if value is None:
        return False
    value_float = float(value)
    if lower_bound is not None and value_float < lower_bound:
        return False
    if upper_bound is not None and value_float >= upper_bound:
        return False
    return True


def _meets_min_confidence(row: ModelingDatasetRow, threshold: float) -> bool:
    """Return True when a row meets the requested minimum confidence score."""
    if row.price_confidence_score is None:
        return False
    return float(row.price_confidence_score) >= threshold
