from datetime import date, timedelta
from decimal import Decimal

import pytest
from sqlalchemy.orm import Session

from app.models import DimSector, DimStock, FactDailyPrice, FactTechnicalIndicator
from app.services.modeling import WalkForwardModelValidator


@pytest.mark.integration
@pytest.mark.database
def test_walk_forward_model_validator_produces_validation_report(
    db_session: Session,
    clean_database,
):
    sector = DimSector(sector_name="Financials")
    db_session.add(sector)
    db_session.flush()

    stock_specs = [
        ("UPA", Decimal("80.00"), Decimal("1.10"), Decimal("62.00"), Decimal("1.10"), Decimal("0.18"), Decimal("72.00")),
        ("UPB", Decimal("55.00"), Decimal("0.75"), Decimal("58.00"), Decimal("0.85"), Decimal("0.20"), Decimal("68.00")),
        ("DNA", Decimal("140.00"), Decimal("-1.00"), Decimal("41.00"), Decimal("-1.05"), Decimal("0.22"), Decimal("35.00")),
        ("DNB", Decimal("95.00"), Decimal("-0.65"), Decimal("44.00"), Decimal("-0.75"), Decimal("0.24"), Decimal("38.00")),
    ]

    stocks = []
    for stock_code, *_ in stock_specs:
        stock = DimStock(
            stock_code=stock_code,
            company_name=f"{stock_code} Plc",
            sector_id=sector.sector_id,
            exchange="NGX",
            is_active=True,
        )
        db_session.add(stock)
        stocks.append(stock)
    db_session.flush()

    stock_by_code = {stock.stock_code: stock for stock in stocks}
    base_date = date.today() - timedelta(days=89)
    prices = []
    indicators = []

    for (
        stock_code,
        base_price,
        daily_step,
        rsi_value,
        macd_value,
        volatility_value,
        trend_strength,
    ) in stock_specs:
        stock = stock_by_code[stock_code]

        for day_index in range(90):
            price_date = base_date + timedelta(days=day_index)
            close_price = base_price + (daily_step * day_index)
            previous_close = (
                base_price + (daily_step * (day_index - 1))
                if day_index > 0
                else close_price
            )
            change_1d_pct = (
                ((close_price - previous_close) / previous_close) * Decimal("100")
                if previous_close > 0
                else Decimal("0.00")
            )

            prices.append(
                FactDailyPrice(
                    stock_id=stock.stock_id,
                    price_date=price_date,
                    close_price=close_price,
                    volume=1_000_000 + (day_index * 7_500),
                    change_1d_pct=change_1d_pct,
                    change_ytd_pct=Decimal("10.00") + (daily_step * day_index),
                    source="TEST",
                    source_count=1,
                    bar_status="RECONCILED",
                    is_official=False,
                    confidence_score=Decimal("85.00"),
                    data_quality_flag="GOOD",
                    has_complete_data=True,
                )
            )

            indicators.append(
                FactTechnicalIndicator(
                    stock_id=stock.stock_id,
                    calculation_date=price_date,
                    ma_7=close_price - (daily_step * Decimal("2.0")),
                    ma_30=close_price - (daily_step * Decimal("5.0")),
                    ma_90=close_price - (daily_step * Decimal("8.0")),
                    rsi_14=rsi_value,
                    macd=macd_value,
                    macd_signal=(
                        macd_value - Decimal("0.25")
                        if daily_step > 0
                        else macd_value + Decimal("0.25")
                    ),
                    macd_histogram=Decimal("0.25"),
                    volatility_30=volatility_value,
                    bollinger_upper=close_price * Decimal("1.05"),
                    bollinger_middle=close_price,
                    bollinger_lower=close_price * Decimal("0.95"),
                    ma_crossover_signal="BULLISH" if daily_step > 0 else "BEARISH",
                    trend_strength=trend_strength,
                )
            )

    db_session.add_all(prices)
    db_session.add_all(indicators)
    db_session.commit()

    validator = WalkForwardModelValidator(
        db_session,
        training_window_days=45,
        evaluation_window_days=15,
        step_days=15,
        min_training_rows=40,
        min_class_count=10,
        iterations=350,
        top_k=2,
    )

    report = validator.run(
        start_date=base_date + timedelta(days=45),
        end_date=base_date + timedelta(days=74),
    )

    assert report.fold_count >= 1
    assert report.total_evaluated_rows > 0
    assert 0.0 <= report.overall_hit_rate <= 1.0
    assert report.overall_brier_score >= 0.0
    assert len(report.overall_bucket_stats) == 5
    assert report.overall_baseline_comparison.name == "price_change_10d_momentum"
