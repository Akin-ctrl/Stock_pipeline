from datetime import date

from app.services.modeling import ModelingDatasetRow
from app.services.modeling.trust_validation import (
    TrustValidator,
    build_confidence_band_stats,
    build_standard_trust_filter_comparisons,
)


def _make_row(**overrides) -> ModelingDatasetRow:
    base = ModelingDatasetRow(
        stock_id=1,
        stock_code="TEST",
        anchor_date=date(2026, 1, 1),
        horizon_date=date(2026, 1, 15),
        close_price=110.0,
        horizon_close_price=118.0,
        volume=1_500_000,
        change_1d_pct=1.5,
        change_ytd_pct=8.0,
        price_confidence_score=85.0,
        price_quality_flag="GOOD",
        bar_status="RECONCILED",
        has_complete_data=True,
        is_official=False,
        trusted_history_days=70,
        volume_ratio=1.2,
        price_change_pct=1.5,
        price_change_3d=3.0,
        price_change_5d=4.5,
        price_change_10d=8.0,
        price_change_20d=12.0,
        price_change_30d=16.0,
        price_change_60d=20.0,
        ma_7=105.0,
        ma_30=100.0,
        ma_90=90.0,
        close_vs_ma_7_pct=4.0,
        close_vs_ma_30_pct=10.0,
        close_vs_ma_90_pct=22.0,
        ma_7_vs_ma_30_pct=5.0,
        ma_30_vs_ma_90_pct=11.0,
        close_vs_20d_high_pct=-1.0,
        close_vs_60d_high_pct=-2.0,
        close_vs_20d_low_pct=8.0,
        close_vs_60d_low_pct=16.0,
        drawdown_20d_pct=1.0,
        drawdown_60d_pct=2.0,
        rebound_20d_pct=8.0,
        rebound_60d_pct=16.0,
        volatility_10d=0.08,
        volatility_20d=0.11,
        downside_volatility_20d=0.05,
        average_volume_20d=1_300_000.0,
        volume_trend_ratio=1.1,
        rsi_14=58.0,
        macd=1.2,
        macd_signal=0.8,
        macd_histogram=0.4,
        volatility_30=0.14,
        atr_14=2.1,
        bollinger_upper=115.0,
        bollinger_middle=110.0,
        bollinger_lower=105.0,
        ma_crossover_signal="BULLISH",
        trend_strength=72.0,
        target_up_10d=1,
        forward_return_10d=7.27,
    )
    row_dict = base.to_dict()
    row_dict.update(overrides)
    return ModelingDatasetRow(**row_dict)


def test_trust_cohorts_and_filter_comparisons_capture_quality_vs_coverage():
    rows = [
        _make_row(
            stock_code="AAA",
            price_confidence_score=88.0,
            price_quality_flag="GOOD",
            bar_status="RECONCILED",
            target_up_10d=1,
            forward_return_10d=8.0,
        ),
        _make_row(
            stock_code="BBB",
            anchor_date=date(2026, 1, 2),
            horizon_date=date(2026, 1, 16),
            price_confidence_score=74.0,
            price_quality_flag="INCOMPLETE",
            bar_status="OFFICIAL",
            target_up_10d=1,
            forward_return_10d=3.5,
        ),
        _make_row(
            stock_code="CCC",
            anchor_date=date(2026, 1, 3),
            horizon_date=date(2026, 1, 17),
            price_confidence_score=55.0,
            price_quality_flag="POOR",
            bar_status="OBSERVED",
            has_complete_data=False,
            target_up_10d=0,
            forward_return_10d=-4.0,
        ),
    ]

    confidence_stats = build_confidence_band_stats(
        rows,
        bands=((None, 70.0, "<70"), (70.0, 85.0, "70-84.99"), (85.0, None, "85+")),
    )
    filter_stats = build_standard_trust_filter_comparisons(rows)
    report = TrustValidator(session=None).summarize_rows(rows)

    assert [stat.row_count for stat in confidence_stats] == [1, 1, 1]
    assert confidence_stats[0].positive_rate == 0.0
    assert confidence_stats[2].positive_rate == 1.0

    comparisons = {stat.label: stat for stat in filter_stats}
    assert comparisons["baseline_all_rows"].row_count == 3
    assert comparisons["min_confidence_score_80"].row_count == 1
    assert comparisons["quality_flag_good_only"].row_count == 1
    assert comparisons["has_complete_data_only"].row_count == 2

    assert report.overall_row_count == 3
    assert any(stat.label == "GOOD" for stat in report.quality_flag_stats)
    assert any(stat.label == "RECONCILED" for stat in report.bar_status_stats)
    assert any(
        stat.label == "trusted_history_days>=60"
        for stat in report.history_threshold_stats
    )
