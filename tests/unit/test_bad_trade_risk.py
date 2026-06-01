from datetime import date

import pytest

from app.services.modeling import BAD_TRADE_FEATURE_NAMES
from app.services.modeling.bad_trade_risk import (
    build_bad_trade_feature_vector_from_row,
    build_bad_trade_feature_vector_from_snapshot,
    calculate_heuristic_proxy_score,
    calculate_signal_agreement_proxy,
)
from app.services.modeling.dataset_builder import ModelingDatasetRow


def test_bad_trade_feature_vectors_keep_row_and_snapshot_contract_aligned():
    row = _row()
    snapshot = {
        "heuristic_score": calculate_heuristic_proxy_score(row),
        "signal_agreement": calculate_signal_agreement_proxy(row),
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

    row_vector = build_bad_trade_feature_vector_from_row(row)
    snapshot_vector = build_bad_trade_feature_vector_from_snapshot(snapshot)

    assert len(row_vector) == len(BAD_TRADE_FEATURE_NAMES)
    assert len(snapshot_vector) == len(BAD_TRADE_FEATURE_NAMES)
    assert snapshot_vector[-6:] == pytest.approx(row_vector[-6:])


def test_bad_trade_candidate_proxy_scores_positive_trend_setup():
    row = _row()

    assert calculate_heuristic_proxy_score(row) >= 70.0
    assert calculate_signal_agreement_proxy(row) == pytest.approx(1.0)


def _row() -> ModelingDatasetRow:
    return ModelingDatasetRow(
        stock_id=1,
        stock_code="TEST",
        anchor_date=date(2026, 1, 5),
        horizon_date=date(2026, 1, 19),
        close_price=100.0,
        horizon_close_price=108.0,
        volume=1_000_000,
        change_1d_pct=1.0,
        change_ytd_pct=5.0,
        price_confidence_score=90.0,
        price_quality_flag="GOOD",
        bar_status="RECONCILED",
        has_complete_data=True,
        is_official=True,
        trusted_history_days=90,
        volume_ratio=1.2,
        price_change_pct=1.0,
        price_change_3d=2.0,
        price_change_5d=3.0,
        price_change_10d=7.0,
        price_change_20d=13.0,
        price_change_30d=16.0,
        price_change_60d=22.0,
        ma_7=98.0,
        ma_30=94.0,
        ma_90=86.0,
        close_vs_ma_7_pct=2.0,
        close_vs_ma_30_pct=6.0,
        close_vs_ma_90_pct=16.0,
        ma_7_vs_ma_30_pct=4.0,
        ma_30_vs_ma_90_pct=9.0,
        close_vs_20d_high_pct=-1.0,
        close_vs_60d_high_pct=-3.0,
        close_vs_20d_low_pct=18.0,
        close_vs_60d_low_pct=32.0,
        drawdown_20d_pct=1.0,
        drawdown_60d_pct=3.0,
        rebound_20d_pct=22.0,
        rebound_60d_pct=32.0,
        volatility_10d=0.08,
        volatility_20d=0.10,
        downside_volatility_20d=0.05,
        average_volume_20d=950_000.0,
        volume_trend_ratio=1.1,
        rsi_14=58.0,
        macd=1.3,
        macd_signal=0.6,
        macd_histogram=0.7,
        volatility_30=0.12,
        atr_14=2.0,
        bollinger_upper=110.0,
        bollinger_middle=96.0,
        bollinger_lower=82.0,
        ma_crossover_signal="BULLISH",
        trend_strength=70.0,
        target_up_10d=1,
        forward_return_10d=8.0,
    )
