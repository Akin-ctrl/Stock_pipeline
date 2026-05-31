from datetime import date, timedelta
from decimal import Decimal

import pytest
from sqlalchemy.orm import Session

from app.models import DimSector, DimStock, FactDailyPrice, FactTechnicalIndicator
from app.services.advisory.advisor import StockScreener
from app.services.modeling import HistoricalLogisticProbabilityEstimator


@pytest.mark.integration
@pytest.mark.database
def test_historical_probability_estimator_returns_non_null_probabilities(
    db_session: Session,
    clean_database,
):
    sector = DimSector(sector_name="Financials")
    db_session.add(sector)
    db_session.flush()

    stock_specs = [
        ("UPA", Decimal("80.00"), Decimal("1.20"), Decimal("62.00"), Decimal("1.20"), Decimal("0.18"), Decimal("72.00")),
        ("UPB", Decimal("55.00"), Decimal("0.80"), Decimal("59.00"), Decimal("0.90"), Decimal("0.20"), Decimal("68.00")),
        ("DNA", Decimal("140.00"), Decimal("-1.10"), Decimal("41.00"), Decimal("-1.10"), Decimal("0.22"), Decimal("35.00")),
        ("DNB", Decimal("95.00"), Decimal("-0.70"), Decimal("44.00"), Decimal("-0.80"), Decimal("0.24"), Decimal("38.00")),
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
    base_date = date.today() - timedelta(days=69)
    recommendation_date = base_date + timedelta(days=69)

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

        for day_index in range(70):
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
                    volume=1_000_000 + (day_index * 5_000),
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

    estimator = HistoricalLogisticProbabilityEstimator(
        db_session,
        min_training_rows=80,
        min_class_count=20,
        training_window_days=365,
        iterations=400,
    )

    up_probability = estimator.estimate_probability_10d_up(
        {
            "stock_id": stock_by_code["UPA"].stock_id,
            "stock_code": "UPA",
            "recommendation_date": recommendation_date,
            "indicators": {
                "current_price": float(prices[69].close_price),
                "ma_30": float(indicators[69].ma_30),
                "ma_90": float(indicators[69].ma_90),
                "rsi_14": float(indicators[69].rsi_14),
                "macd": float(indicators[69].macd),
                "macd_signal": float(indicators[69].macd_signal),
                "volatility": float(indicators[69].volatility_30),
                "volume_ratio": 1.1,
                "price_change_pct": float(prices[69].change_1d_pct),
                "price_change_3d": 3.0,
                "price_change_5d": 5.0,
                "price_change_10d": 8.0,
                "price_change_20d": 18.0,
                "price_change_30d": 24.0,
                "price_change_60d": 48.0,
                "price_confidence_score": 85.0,
                "trusted_history_days": 70,
                "trend_strength": float(indicators[69].trend_strength),
                "is_official": False,
                "has_complete_data": True,
            },
        }
    )
    down_probability = estimator.estimate_probability_10d_up(
        {
            "stock_id": stock_by_code["DNA"].stock_id,
            "stock_code": "DNA",
            "recommendation_date": recommendation_date,
            "indicators": {
                "current_price": float(prices[209].close_price),
                "ma_30": float(indicators[209].ma_30),
                "ma_90": float(indicators[209].ma_90),
                "rsi_14": float(indicators[209].rsi_14),
                "macd": float(indicators[209].macd),
                "macd_signal": float(indicators[209].macd_signal),
                "volatility": float(indicators[209].volatility_30),
                "volume_ratio": 0.9,
                "price_change_pct": float(prices[209].change_1d_pct),
                "price_change_3d": -2.0,
                "price_change_5d": -4.0,
                "price_change_10d": -7.0,
                "price_change_20d": -14.0,
                "price_change_30d": -19.0,
                "price_change_60d": -36.0,
                "price_confidence_score": 85.0,
                "trusted_history_days": 70,
                "trend_strength": float(indicators[209].trend_strength),
                "is_official": False,
                "has_complete_data": True,
            },
        }
    )

    assert up_probability is not None
    assert down_probability is not None
    assert 0.0 < up_probability < 1.0
    assert 0.0 < down_probability < 1.0
    assert up_probability > down_probability


@pytest.mark.integration
@pytest.mark.database
def test_screener_populates_predicted_probability_when_history_is_sufficient(
    db_session: Session,
    clean_database,
):
    sector = DimSector(sector_name="Banking")
    db_session.add(sector)
    db_session.flush()

    stock = DimStock(
        stock_code="PROB1",
        company_name="Probability One Plc",
        sector_id=sector.sector_id,
        exchange="NGX",
        is_active=True,
    )
    peer = DimStock(
        stock_code="PROB2",
        company_name="Probability Two Plc",
        sector_id=sector.sector_id,
        exchange="NGX",
        is_active=True,
    )
    db_session.add_all([stock, peer])
    db_session.flush()

    base_date = date.today() - timedelta(days=69)
    recommendation_date = base_date + timedelta(days=69)
    prices = []
    indicators = []

    for entity, daily_step, rsi_value, macd_value, trend_strength in [
        (stock, Decimal("1.00"), Decimal("60.00"), Decimal("1.20"), Decimal("70.00")),
        (peer, Decimal("-0.90"), Decimal("43.00"), Decimal("-1.00"), Decimal("34.00")),
    ]:
        for day_index in range(70):
            price_date = base_date + timedelta(days=day_index)
            close_price = Decimal("100.00") + (daily_step * day_index)
            previous_close = (
                Decimal("100.00") + (daily_step * (day_index - 1))
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
                    stock_id=entity.stock_id,
                    price_date=price_date,
                    close_price=close_price,
                    volume=1_000_000 + (day_index * 8_000),
                    change_1d_pct=change_1d_pct,
                    change_ytd_pct=Decimal("12.00"),
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
                    stock_id=entity.stock_id,
                    calculation_date=price_date,
                    ma_7=close_price - (daily_step * Decimal("2.0")),
                    ma_30=close_price - (daily_step * Decimal("5.0")),
                    ma_90=close_price - (daily_step * Decimal("8.0")),
                    rsi_14=rsi_value,
                    macd=macd_value,
                    macd_signal=(
                        macd_value - Decimal("0.20")
                        if daily_step > 0
                        else macd_value + Decimal("0.20")
                    ),
                    macd_histogram=Decimal("0.20"),
                    volatility_30=Decimal("0.18") if daily_step > 0 else Decimal("0.24"),
                    bollinger_upper=close_price * Decimal("1.04"),
                    bollinger_middle=close_price,
                    bollinger_lower=close_price * Decimal("0.96"),
                    ma_crossover_signal="BULLISH" if daily_step > 0 else "BEARISH",
                    trend_strength=trend_strength,
                )
            )

    db_session.add_all(prices)
    db_session.add_all(indicators)
    db_session.commit()

    screener = StockScreener(
        db_session,
        probability_estimator=HistoricalLogisticProbabilityEstimator(
            db_session,
            min_training_rows=80,
            min_class_count=20,
            training_window_days=365,
            iterations=400,
        ),
    )
    recommendations = screener.generate_recommendations(
        recommendation_date=recommendation_date,
        stock_codes=["PROB1"],
        min_score=40.0,
        min_confidence=0.50,
    )

    assert len(recommendations) == 1
    assert recommendations[0].predicted_probability_10d_up is not None
    assert 0.0 < recommendations[0].predicted_probability_10d_up < 1.0

    filtered = screener.generate_recommendations(
        recommendation_date=recommendation_date,
        stock_codes=["PROB1"],
        min_score=40.0,
        min_confidence=0.50,
        min_predicted_probability=0.9999,
    )
    assert filtered == []
