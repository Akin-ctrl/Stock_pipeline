from dataclasses import dataclass

import pytest

from app.services.modeling.feature_engineering import (
    average_volume,
    build_historical_feature_snapshot,
    build_probability_feature_mapping,
    calculate_backward_return,
    calculate_volume_ratio,
    downside_return_volatility,
    rolling_return_volatility,
    volume_trend_ratio,
)


@dataclass(frozen=True)
class PricePoint:
    close_price: float
    volume: int | None


def test_calculate_volume_ratio_uses_only_anchor_and_prior_history():
    history = [
        PricePoint(close_price=100.0 + day, volume=100 + day * 10)
        for day in range(22)
    ]

    volume_ratio = calculate_volume_ratio(history)

    assert volume_ratio == pytest.approx(310 / 205)


def test_calculate_backward_return_uses_requested_lookback_sessions():
    history = [
        PricePoint(close_price=100.0, volume=100),
        PricePoint(close_price=110.0, volume=110),
        PricePoint(close_price=121.0, volume=120),
    ]

    assert calculate_backward_return(history, 1) == pytest.approx(10.0)
    assert calculate_backward_return(history, 2) == pytest.approx(21.0)
    assert calculate_backward_return(history, 3) is None


def test_build_historical_feature_snapshot_adds_first_return_and_trend_batches():
    history = [
        PricePoint(close_price=100.0 + (day * 2.0), volume=1000 + (day * 50))
        for day in range(65)
    ]

    snapshot = build_historical_feature_snapshot(
        history_through_anchor=history,
        current_price=history[-1].close_price,
        ma_7=220.0,
        ma_30=210.0,
        ma_90=200.0,
    )

    assert snapshot.price_change_3d == pytest.approx(((228.0 - 222.0) / 222.0) * 100.0)
    assert snapshot.price_change_5d == pytest.approx(((228.0 - 218.0) / 218.0) * 100.0)
    assert snapshot.price_change_30d == pytest.approx(((228.0 - 168.0) / 168.0) * 100.0)
    assert snapshot.price_change_60d == pytest.approx(((228.0 - 108.0) / 108.0) * 100.0)
    assert snapshot.close_vs_ma_7_pct == pytest.approx(((228.0 - 220.0) / 220.0) * 100.0)
    assert snapshot.close_vs_ma_90_pct == pytest.approx(((228.0 - 200.0) / 200.0) * 100.0)
    assert snapshot.ma_7_vs_ma_30_pct == pytest.approx(((220.0 - 210.0) / 210.0) * 100.0)
    assert snapshot.close_vs_20d_high_pct == pytest.approx(0.0)
    assert snapshot.close_vs_60d_high_pct == pytest.approx(0.0)
    assert snapshot.close_vs_20d_low_pct == pytest.approx(((228.0 - 190.0) / 190.0) * 100.0)
    assert snapshot.close_vs_60d_low_pct == pytest.approx(((228.0 - 110.0) / 110.0) * 100.0)
    assert snapshot.drawdown_20d_pct == pytest.approx(0.0)
    assert snapshot.rebound_20d_pct == pytest.approx(((228.0 - 190.0) / 190.0) * 100.0)
    assert snapshot.volatility_10d > 0.0
    assert snapshot.volatility_20d > 0.0
    assert snapshot.average_volume_20d == pytest.approx(3725.0)
    assert snapshot.volume_trend_ratio > 1.0


def test_volatility_and_volume_helpers_add_the_next_feature_batch():
    history = [
        PricePoint(close_price=100.0, volume=1_000_000),
        PricePoint(close_price=95.0, volume=1_100_000),
        PricePoint(close_price=105.0, volume=1_200_000),
        PricePoint(close_price=90.0, volume=1_300_000),
        PricePoint(close_price=110.0, volume=1_400_000),
        PricePoint(close_price=100.0, volume=1_500_000),
    ]

    assert rolling_return_volatility(history, 5) > 0.0
    assert downside_return_volatility(history, 5) > 0.0
    assert average_volume(history, 5) == pytest.approx(1_300_000.0)
    assert volume_trend_ratio(history, 2, 5) == pytest.approx((1_450_000.0 / 1_300_000.0))


def test_build_probability_feature_mapping_keeps_current_normalization_contract():
    feature_mapping = build_probability_feature_mapping(
        current_price=110.0,
        ma_7=105.0,
        ma_30=100.0,
        ma_90=80.0,
        rsi_14=60.0,
        macd=1.2,
        macd_signal=0.7,
        volatility_10d=0.09,
        volatility_20d=0.13,
        volatility_30=0.15,
        downside_volatility_20d=0.08,
        volume_ratio=1.4,
        average_volume_20d=2_500_000.0,
        volume_trend_ratio=1.25,
        price_change_pct=3.5,
        price_change_3d=5.0,
        price_change_5d=6.5,
        price_change_10d=8.0,
        price_change_20d=12.0,
        price_change_30d=15.0,
        price_change_60d=20.0,
        price_confidence_score=85.0,
        trusted_history_days=45,
        trend_strength=70.0,
        close_vs_20d_high_pct=-2.0,
        close_vs_60d_high_pct=-5.0,
        close_vs_20d_low_pct=10.0,
        close_vs_60d_low_pct=22.0,
        drawdown_20d_pct=2.0,
        drawdown_60d_pct=5.0,
        rebound_20d_pct=10.0,
        rebound_60d_pct=22.0,
        price_quality_flag="GOOD",
        bar_status="RECONCILED",
        is_official=True,
        has_complete_data=False,
    )

    assert feature_mapping["rsi_14_centered"] == pytest.approx(0.2)
    assert feature_mapping["price_change_3d"] == pytest.approx(5.0)
    assert feature_mapping["price_change_60d"] == pytest.approx(20.0)
    assert feature_mapping["macd_signal_gap"] == pytest.approx(0.5)
    assert feature_mapping["volatility_10d"] == pytest.approx(0.09)
    assert feature_mapping["volatility_20d"] == pytest.approx(0.13)
    assert feature_mapping["downside_volatility_20d"] == pytest.approx(0.08)
    assert feature_mapping["volume_ratio_centered"] == pytest.approx(0.4)
    assert feature_mapping["avg_volume_20d_millions"] == pytest.approx(2.5)
    assert feature_mapping["volume_trend_ratio_centered"] == pytest.approx(0.25)
    assert feature_mapping["close_vs_ma_7_pct"] == pytest.approx(((110.0 - 105.0) / 105.0) * 100.0)
    assert feature_mapping["close_vs_ma_30_pct"] == pytest.approx(10.0)
    assert feature_mapping["close_vs_ma_90_pct"] == pytest.approx(37.5)
    assert feature_mapping["ma_7_vs_ma_30_pct"] == pytest.approx(5.0)
    assert feature_mapping["ma_30_vs_ma_90_pct"] == pytest.approx(25.0)
    assert feature_mapping["close_vs_20d_high_pct"] == pytest.approx(-2.0)
    assert feature_mapping["close_vs_60d_high_pct"] == pytest.approx(-5.0)
    assert feature_mapping["close_vs_20d_low_pct"] == pytest.approx(10.0)
    assert feature_mapping["close_vs_60d_low_pct"] == pytest.approx(22.0)
    assert feature_mapping["drawdown_20d_pct"] == pytest.approx(2.0)
    assert feature_mapping["drawdown_60d_pct"] == pytest.approx(5.0)
    assert feature_mapping["rebound_20d_pct"] == pytest.approx(10.0)
    assert feature_mapping["rebound_60d_pct"] == pytest.approx(22.0)
    assert feature_mapping["price_confidence_score_scaled"] == pytest.approx(0.85)
    assert feature_mapping["trusted_history_days_scaled"] == pytest.approx(0.45)
    assert feature_mapping["trend_strength_scaled"] == pytest.approx(0.7)
    assert feature_mapping["quality_good_flag"] == 1.0
    assert feature_mapping["quality_incomplete_flag"] == 0.0
    assert feature_mapping["bar_reconciled_flag"] == 1.0
    assert feature_mapping["bar_official_flag"] == 0.0
    assert feature_mapping["is_official"] == 1.0
    assert feature_mapping["has_complete_data"] == 0.0
