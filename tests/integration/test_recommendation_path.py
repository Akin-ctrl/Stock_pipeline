"""
Integration test for the current recommendation generation path.

This test validates the live schema and recommendation flow:
1. Load trusted close-price facts into `fact_daily_prices`
2. Load one wide indicator row per stock into `fact_technical_indicators`
3. Run `StockScreener.generate_recommendations()`
4. Persist and verify the resulting recommendations
"""

import pytest
from datetime import date, timedelta
from decimal import Decimal
from sqlalchemy.orm import Session

from app.models import (
    DimSector,
    DimStock,
    FactDailyPrice,
    FactRecommendationAudit,
    FactTechnicalIndicator,
)
from app.repositories.price_repository import PriceRepository
from app.repositories.indicator_repository import IndicatorRepository
from app.repositories.recommendation_repository import RecommendationRepository
from app.services.advisory.advisor import (
    RecommendationAction,
    StockRecommendation,
    StockScreener,
)
from app.utils.logger import get_logger

logger = get_logger(__name__)


@pytest.mark.integration
class TestRecommendationPath:
    """Test complete recommendation generation path end-to-end."""

    @pytest.fixture
    def recommendation_test_data(self, db_session: Session):
        """
        Setup current-schema test data for recommendation path:
        - 3 sample stocks
        - 30 days of trusted close-price data with a mild uptrend
        - 1 wide technical-indicator row per stock on the recommendation date
        """
        sector = DimSector(sector_name="Banking")
        db_session.add(sector)
        db_session.flush()

        stocks = [
            DimStock(
                stock_code="TEST1",
                company_name="Test Company One",
                sector_id=sector.sector_id,
                exchange="NGX",
                is_active=True,
            ),
            DimStock(
                stock_code="TEST2",
                company_name="Test Company Two",
                sector_id=sector.sector_id,
                exchange="NGX",
                is_active=True,
            ),
            DimStock(
                stock_code="TEST3",
                company_name="Test Company Three",
                sector_id=sector.sector_id,
                exchange="NGX",
                is_active=True,
            ),
        ]
        db_session.add_all(stocks)
        db_session.flush()

        base_date = date.today() - timedelta(days=29)
        recommendation_date = base_date + timedelta(days=29)
        prices = []

        for stock in stocks:
            for i in range(30):
                price_date = base_date + timedelta(days=i)
                if i <= 26:
                    close_price = Decimal("100.00") + Decimal(str(i * 0.35))
                else:
                    pullback_prices = {
                        27: Decimal("108.50"),
                        28: Decimal("108.00"),
                        29: Decimal("107.80"),
                    }
                    close_price = pullback_prices[i]

                if i == 0:
                    previous_close = None
                elif i <= 27:
                    previous_close = Decimal("100.00") + Decimal(str((i - 1) * 0.35))
                else:
                    previous_close = {
                        28: Decimal("108.50"),
                        29: Decimal("108.00"),
                    }[i]
                change_1d_pct = None
                if previous_close is not None and previous_close > 0:
                    change_1d_pct = (
                        (close_price - previous_close) / previous_close
                    ) * Decimal("100")

                prices.append(
                    FactDailyPrice(
                        stock_id=stock.stock_id,
                        price_date=price_date,
                        close_price=close_price,
                        volume=1_000_000 + (i * 25_000),
                        change_1d_pct=change_1d_pct,
                        source="TEST",
                        source_count=1,
                        bar_status="RECONCILED",
                        is_official=False,
                        confidence_score=Decimal("85.00"),
                        data_quality_flag="GOOD",
                        has_complete_data=True,
                    )
                )

        db_session.add_all(prices)
        db_session.flush()

        indicators = []
        for stock in stocks:
            indicators.append(
                FactTechnicalIndicator(
                    stock_id=stock.stock_id,
                    calculation_date=recommendation_date,
                    ma_7=Decimal("108.00"),
                    ma_30=Decimal("104.00"),
                    ma_90=Decimal("99.00"),
                    rsi_14=Decimal("58.00"),
                    macd=Decimal("2.10"),
                    macd_signal=Decimal("1.40"),
                    macd_histogram=Decimal("0.70"),
                    volatility_30=Decimal("0.18"),
                    bollinger_upper=Decimal("118.00"),
                    bollinger_middle=Decimal("112.00"),
                    bollinger_lower=Decimal("106.00"),
                    ma_crossover_signal="BULLISH",
                    trend_strength=Decimal("72.00"),
                )
            )

        db_session.add_all(indicators)
        db_session.commit()

        return {
            "stocks": stocks,
            "recommendation_date": recommendation_date,
        }

    def test_recommendation_generation_complete_path(
        self,
        db_session: Session,
        recommendation_test_data,
    ):
        """Generate and persist recommendations using the live schema."""
        stocks = recommendation_test_data["stocks"]
        recommendation_date = recommendation_test_data["recommendation_date"]

        logger.info("=" * 80)
        logger.info("TEST: RECOMMENDATION GENERATION END-TO-END PATH")
        logger.info("=" * 80)

        price_repo = PriceRepository(db_session)
        indicator_repo = IndicatorRepository(db_session)

        logger.info("\n[CHECK 1] Verifying trusted price history...")
        for stock in stocks:
            stock_prices = price_repo.get_trusted_price_history(
                stock.stock_id,
                end_date=recommendation_date,
                limit=30,
            )
            assert len(stock_prices) == 30
            assert stock_prices[0].price_date == recommendation_date
            logger.info(f"✓ {stock.stock_code}: 30 trusted prices loaded")

        logger.info("\n[CHECK 2] Verifying current indicator rows...")
        for stock in stocks:
            indicators = indicator_repo.get_latest_by_code(
                stock.stock_code,
                recommendation_date,
            )
            assert indicators is not None
            assert indicators.calculation_date == recommendation_date
            assert indicators.rsi_14 is not None
            logger.info(f"✓ {stock.stock_code}: indicator row loaded")

        logger.info("\n[CHECK 3] Initializing StockScreener...")
        screener = StockScreener(db_session)
        logger.info("✓ StockScreener initialized successfully")

        logger.info(f"\n[CHECK 4] Generating recommendations for {recommendation_date}...")
        recommendations = screener.generate_recommendations(
            recommendation_date=recommendation_date,
            min_score=40.0,
            min_confidence=0.50,
            capture_audit=True,
        )

        assert len(recommendations) > 0, (
            "Expected recommendations but got zero. "
            "This indicates a regression in the current recommendation path."
        )

        logger.info(f"✓ Generated {len(recommendations)} recommendations")

        logger.info("\n[CHECK 5] Validating recommendation properties...")
        expected_codes = {stock.stock_code for stock in stocks}
        for rec in recommendations:
            assert isinstance(rec, StockRecommendation)
            assert rec.stock_code in expected_codes
            assert rec.recommendation_date == recommendation_date
            assert rec.score >= 40.0
            assert rec.signal_agreement >= 0.50
            assert rec.confidence == rec.signal_agreement
            assert rec.predicted_probability_10d_up is None
            assert rec.action_type in {
                RecommendationAction.BUY,
                RecommendationAction.STRONG_BUY,
            }
            assert rec.is_actionable is True
            assert rec.signal_type.value in {"BUY", "STRONG_BUY"}

            logger.info(
                f"✓ {rec.stock_code}: action={rec.action_type.value} "
                f"technical={rec.signal_type.value} | "
                f"Score: {rec.score:.1f} | Signal Agreement: {rec.signal_agreement:.2f}"
            )

        logger.info("\n[CHECK 6] Persisting recommendations to database...")
        rec_repo = RecommendationRepository(db_session)
        audit_count = rec_repo.replace_audit_entries(
            recommendation_date=recommendation_date,
            profile=screener.strategy_profile.value,
            audit_entries=screener.last_audit_entries,
        )
        saved_count = rec_repo.create_recommendations_bulk(recommendations)
        assert audit_count == len(stocks)
        assert saved_count == len(recommendations)

        logger.info("\n[CHECK 7] Verifying persisted recommendations...")
        persisted = rec_repo.get_recommendations_by_date(recommendation_date)
        assert len(persisted) == len(recommendations)
        persisted_audit = db_session.query(FactRecommendationAudit).filter(
            FactRecommendationAudit.recommendation_date == recommendation_date,
        ).all()
        assert len(persisted_audit) == len(stocks)
        assert {row.stage_reached for row in persisted_audit} == {"selected"}
        logger.info(f"✓ {len(persisted)} recommendations persisted successfully")

        logger.info("\n" + "=" * 80)
        logger.info("✅ RECOMMENDATION PATH TEST PASSED")
        logger.info("=" * 80)

    def test_recommendation_path_with_zero_recommendations_scenario(
        self,
        db_session: Session,
    ):
        """Stocks without trusted price history or indicators should be skipped cleanly."""
        logger.info("\n" + "=" * 80)
        logger.info("TEST: RECOMMENDATION PATH WITH INSUFFICIENT DATA")
        logger.info("=" * 80)

        sector = DimSector(sector_name="Tech")
        db_session.add(sector)
        db_session.flush()

        stock = DimStock(
            stock_code="EMPTY",
            company_name="Empty Stock",
            sector_id=sector.sector_id,
            exchange="NGX",
            is_active=True,
        )
        db_session.add(stock)
        db_session.commit()

        logger.info("\n[CHECK 1] Attempting recommendation generation with minimal data...")
        screener = StockScreener(db_session)
        recommendations = screener.generate_recommendations(
            recommendation_date=date.today(),
            min_score=40.0,
            min_confidence=0.50,
            stock_codes=[stock.stock_code],
            capture_audit=True,
        )

        logger.info(f"✓ Generated {len(recommendations)} recommendations (expected 0)")
        assert recommendations == []
        assert len(screener.last_audit_entries) == 1
        assert screener.last_audit_entries[0].stage_reached == "no_indicator"

        rec_repo = RecommendationRepository(db_session)
        audit_count = rec_repo.replace_audit_entries(
            recommendation_date=date.today(),
            profile=screener.strategy_profile.value,
            audit_entries=screener.last_audit_entries,
            full_snapshot=False,
        )
        assert audit_count == 1

        persisted_audit = db_session.query(FactRecommendationAudit).one()
        assert persisted_audit.stock_id == stock.stock_id
        assert persisted_audit.rejection_reason == "no_indicator"
        assert persisted_audit.selected is False

        logger.info("\n" + "=" * 80)
        logger.info("✅ EDGE CASE TEST PASSED")
        logger.info("=" * 80)
