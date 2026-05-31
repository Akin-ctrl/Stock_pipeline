from datetime import date, timedelta
from decimal import Decimal

import pytest
from sqlalchemy.orm import Session

from app.models import DimSector, DimStock, FactDailyPrice, FactTechnicalIndicator
from app.services.modeling import ModelingDatasetBuilder, summarize_probability_features
from app.services.modeling.feature_extractor import (
    extract_probability_features_from_row,
    extract_probability_features_from_snapshot,
    feature_vector_from_mapping,
)


@pytest.mark.integration
@pytest.mark.database
def test_builder_creates_canonical_10d_dataset_rows(
    db_session: Session,
    sample_stocks,
    sample_prices,
    sample_indicators,
):
    builder = ModelingDatasetBuilder(db_session)

    rows = builder.build(stock_codes=["DANGCEM"])
    summary = builder.summarize(rows)

    assert len(rows) == 10
    assert all(row.stock_code == "DANGCEM" for row in rows)
    assert all(row.target_up_10d == 1 for row in rows)
    assert all(row.horizon_date > row.anchor_date for row in rows)
    assert rows[0].trusted_history_days == 11
    assert rows[0].price_change_3d is not None
    assert rows[0].price_change_5d is not None
    assert rows[0].price_change_10d is not None
    assert rows[0].close_vs_ma_7_pct != 0.0
    assert rows[0].ma_7_vs_ma_30_pct != 0.0
    assert rows[0].close_vs_20d_high_pct <= 0.0
    assert rows[0].close_vs_20d_low_pct >= 0.0
    assert rows[0].drawdown_20d_pct >= 0.0
    assert rows[0].rebound_20d_pct >= 0.0
    assert rows[0].volatility_10d >= 0.0
    assert rows[0].volatility_20d >= 0.0
    assert rows[0].downside_volatility_20d >= 0.0
    assert rows[0].average_volume_20d is not None
    assert rows[0].volume_trend_ratio is not None
    assert summary.row_count == 10
    assert summary.stock_count == 1
    assert summary.feature_count > 0
    assert summary.positive_rate == 1.0
    assert summary.duplicate_row_count == 0
    assert "stock_code" not in summary.all_null_fields
    assert summary.constant_value_fields["stock_code"] == "DANGCEM"
    assert "is_official" in summary.constant_value_fields

    feature_summary = summarize_probability_features(rows)
    assert feature_summary.row_count == len(rows)
    assert feature_summary.feature_count > 0
    assert "quality_good_flag" not in feature_summary.high_missing_features


@pytest.mark.integration
@pytest.mark.database
def test_probability_feature_extraction_matches_between_dataset_rows_and_live_snapshots(
    db_session: Session,
    sample_stocks,
    sample_prices,
    sample_indicators,
):
    builder = ModelingDatasetBuilder(db_session)
    rows = builder.build(stock_codes=["DANGCEM"])
    row = rows[0]

    snapshot_mapping = extract_probability_features_from_snapshot(
        {
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
            }
        }
    )
    row_mapping = extract_probability_features_from_row(row)

    assert snapshot_mapping == row_mapping
    assert feature_vector_from_mapping(snapshot_mapping) == feature_vector_from_mapping(
        row_mapping
    )


@pytest.mark.integration
@pytest.mark.database
def test_builder_excludes_rows_that_fail_anchor_price_eligibility(
    db_session: Session,
    clean_database,
):
    sector = DimSector(sector_name="Banking")
    db_session.add(sector)
    db_session.flush()

    stock = DimStock(
        stock_code="FILTERED",
        company_name="Filtered Plc",
        sector_id=sector.sector_id,
        exchange="NGX",
        is_active=True,
    )
    db_session.add(stock)
    db_session.flush()

    base_date = date.today() - timedelta(days=11)
    prices = []
    indicators = []

    for day_index in range(11):
        price_date = base_date + timedelta(days=day_index)
        prices.append(
            FactDailyPrice(
                stock_id=stock.stock_id,
                price_date=price_date,
                close_price=Decimal("100.00") + Decimal(str(day_index)),
                volume=100000 + day_index,
                change_1d_pct=Decimal("1.00") if day_index > 0 else Decimal("0.00"),
                change_ytd_pct=Decimal("5.00"),
                source="TEST",
                source_count=1,
                bar_status="RECONCILED",
                is_official=False,
                confidence_score=Decimal("90.00"),
                data_quality_flag="GOOD",
                has_complete_data=day_index != 0,
            )
        )
        indicators.append(
            FactTechnicalIndicator(
                stock_id=stock.stock_id,
                calculation_date=price_date,
                ma_7=Decimal("100.00"),
                ma_30=Decimal("99.00"),
                ma_90=Decimal("98.00"),
                rsi_14=Decimal("55.00"),
                macd=Decimal("0.50"),
                macd_signal=Decimal("0.30"),
                macd_histogram=Decimal("0.20"),
                volatility_30=Decimal("0.10"),
            )
        )

    db_session.add_all(prices)
    db_session.add_all(indicators)
    db_session.commit()

    builder = ModelingDatasetBuilder(db_session)

    rows = builder.build(stock_codes=["FILTERED"])

    assert rows == []
