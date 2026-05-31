from datetime import date, timedelta
from decimal import Decimal

import pytest
from sqlalchemy.orm import Session

from app.models import DimSector, DimStock, FactDailyPrice, FactTechnicalIndicator
from app.services.modeling import ModelingDatasetBuilder, ModelingDatasetConfig, TrustValidator


@pytest.mark.integration
@pytest.mark.database
def test_trust_validator_summarizes_mixed_trust_cohorts(
    db_session: Session,
    clean_database,
):
    sector = DimSector(sector_name="Banking")
    db_session.add(sector)
    db_session.flush()

    stock = DimStock(
        stock_code="TRUST1",
        company_name="Trust One Plc",
        sector_id=sector.sector_id,
        exchange="NGX",
        is_active=True,
    )
    db_session.add(stock)
    db_session.flush()

    base_date = date.today() - timedelta(days=24)
    prices = []
    indicators = []

    quality_cycle = ["GOOD", "INCOMPLETE", "GOOD", "POOR"]
    bar_cycle = ["RECONCILED", "OFFICIAL", "RECONCILED", "OBSERVED"]
    confidence_cycle = [
        Decimal("88.00"),
        Decimal("74.00"),
        Decimal("84.00"),
        Decimal("58.00"),
    ]

    for day_index in range(25):
        price_date = base_date + timedelta(days=day_index)
        close_price = Decimal("100.00") + Decimal(str(day_index))
        previous_close = (
            Decimal("100.00") + Decimal(str(day_index - 1))
            if day_index > 0
            else close_price
        )
        change_1d_pct = (
            ((close_price - previous_close) / previous_close) * Decimal("100")
            if previous_close > 0
            else Decimal("0.00")
        )

        cycle_index = day_index % 4
        prices.append(
            FactDailyPrice(
                stock_id=stock.stock_id,
                price_date=price_date,
                close_price=close_price,
                volume=900_000 + (day_index * 25_000),
                change_1d_pct=change_1d_pct,
                change_ytd_pct=Decimal("10.00"),
                source="TEST",
                source_count=1,
                bar_status=bar_cycle[cycle_index],
                is_official=bar_cycle[cycle_index] == "OFFICIAL",
                confidence_score=confidence_cycle[cycle_index],
                data_quality_flag=quality_cycle[cycle_index],
                has_complete_data=quality_cycle[cycle_index] != "POOR",
            )
        )
        indicators.append(
            FactTechnicalIndicator(
                stock_id=stock.stock_id,
                calculation_date=price_date,
                ma_7=close_price - Decimal("1.0"),
                ma_30=close_price - Decimal("2.0"),
                ma_90=close_price - Decimal("3.0"),
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

    broad_builder = ModelingDatasetBuilder(
        db_session,
        config=ModelingDatasetConfig(
            allowed_bar_statuses=("OBSERVED", "RECONCILED", "OFFICIAL", "ESTIMATED"),
            allowed_quality_flags=("GOOD", "INCOMPLETE", "POOR"),
            require_complete_data=False,
        ),
    )
    validator = TrustValidator(db_session, dataset_builder=broad_builder)

    report = validator.run(start_date=base_date, end_date=base_date + timedelta(days=14))

    assert report.overall_row_count > 0
    assert any(stat.label == "GOOD" for stat in report.quality_flag_stats)
    assert any(stat.label == "INCOMPLETE" for stat in report.quality_flag_stats)
    assert any(stat.label == "POOR" for stat in report.quality_flag_stats)
    comparisons = {stat.label: stat for stat in report.filter_comparisons}
    assert comparisons["baseline_all_rows"].row_count == report.overall_row_count
    assert comparisons["min_confidence_score_80"].row_count < report.overall_row_count
