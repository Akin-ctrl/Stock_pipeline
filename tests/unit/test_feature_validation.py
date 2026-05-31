from datetime import date

from app.services.modeling import ModelingDatasetRow
from app.services.modeling.feature_validation import (
    summarize_probability_features,
    validate_probability_feature_alignment,
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
        close_vs_ma_7_pct=4.7619047619,
        close_vs_ma_30_pct=10.0,
        close_vs_ma_90_pct=22.2222222222,
        ma_7_vs_ma_30_pct=5.0,
        ma_30_vs_ma_90_pct=11.1111111111,
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


def _snapshot_from_row(row: ModelingDatasetRow) -> dict[str, object]:
    return {
        "stock_id": row.stock_id,
        "stock_code": row.stock_code,
        "recommendation_date": row.anchor_date,
        "indicators": {
            "current_price": row.close_price,
            "ma_7": row.ma_7,
            "ma_30": row.ma_30,
            "ma_90": row.ma_90,
            "rsi_14": row.rsi_14,
            "macd": row.macd,
            "macd_signal": row.macd_signal,
            "volatility_10d": row.volatility_10d,
            "volatility_20d": row.volatility_20d,
            "volatility": row.volatility_30,
            "downside_volatility_20d": row.downside_volatility_20d,
            "volume_ratio": row.volume_ratio,
            "average_volume_20d": row.average_volume_20d,
            "volume_trend_ratio": row.volume_trend_ratio,
            "price_change_pct": row.price_change_pct,
            "price_change_3d": row.price_change_3d,
            "price_change_5d": row.price_change_5d,
            "price_change_10d": row.price_change_10d,
            "price_change_20d": row.price_change_20d,
            "price_change_30d": row.price_change_30d,
            "price_change_60d": row.price_change_60d,
            "price_confidence_score": row.price_confidence_score,
            "trusted_history_days": row.trusted_history_days,
            "trend_strength": row.trend_strength,
            "close_vs_20d_high_pct": row.close_vs_20d_high_pct,
            "close_vs_60d_high_pct": row.close_vs_60d_high_pct,
            "close_vs_20d_low_pct": row.close_vs_20d_low_pct,
            "close_vs_60d_low_pct": row.close_vs_60d_low_pct,
            "drawdown_20d_pct": row.drawdown_20d_pct,
            "drawdown_60d_pct": row.drawdown_60d_pct,
            "rebound_20d_pct": row.rebound_20d_pct,
            "rebound_60d_pct": row.rebound_60d_pct,
            "price_quality_flag": row.price_quality_flag,
            "bar_status": row.bar_status,
            "is_official": row.is_official,
            "has_complete_data": row.has_complete_data,
        },
    }


def test_summarize_probability_features_detects_missing_constant_and_zero_only():
    row_one = _make_row(
        price_change_60d=None,
        volume_trend_ratio=None,
        drawdown_20d_pct=0.0,
    )
    row_two = _make_row(
        anchor_date=date(2026, 1, 2),
        horizon_date=date(2026, 1, 16),
        price_change_60d=24.0,
        volume_trend_ratio=1.25,
        price_quality_flag="INCOMPLETE",
        drawdown_20d_pct=0.0,
        trend_strength=68.0,
    )

    summary = summarize_probability_features([row_one, row_two], high_missing_threshold=0.5)

    assert summary.row_count == 2
    assert summary.feature_count > 0
    assert "price_change_60d" in summary.high_missing_features
    assert "drawdown_20d_pct" in summary.zero_only_features
    assert "drawdown_20d_pct" in summary.constant_features
    assert summary.feature_stats["price_change_60d"].raw_missing_count == 1
    assert summary.feature_stats["quality_good_flag"].distinct_mapped_count == 2


def test_validate_probability_feature_alignment_accepts_matching_row_and_snapshot():
    row = _make_row()

    report = validate_probability_feature_alignment(row, _snapshot_from_row(row))

    assert report.is_aligned is True
    assert report.mismatches == {}
    assert report.missing_from_row == ()
    assert report.missing_from_snapshot == ()


def test_validate_probability_feature_alignment_reports_mismatched_snapshot_values():
    row = _make_row()
    snapshot = _snapshot_from_row(row)
    snapshot["indicators"]["price_change_10d"] = row.price_change_10d + 1.0

    report = validate_probability_feature_alignment(row, snapshot)

    assert report.is_aligned is False
    assert "price_change_10d" in report.mismatches
