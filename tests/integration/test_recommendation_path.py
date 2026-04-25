"""
Integration test for recommendation generation path (end-to-end).

This is a regression test that validates the complete recommendation workflow:
1. Load test price data into FactDailyPrice
2. Generate technical indicators from price data
3. Run stock screener to generate recommendations
4. Verify recommendations are generated with expected properties

This test catches regressions in the recommendation pipeline such as:
- AttributeError in StockScreener._analyze_stock() (e.g., stock.stock_name doesn't exist)
- Repository attribute errors (e.g., self.db vs self.session)
- Missing or invalid indicator data
- Broken signal generation or scoring logic
"""

import pytest
from datetime import date, datetime, timedelta
from decimal import Decimal
from sqlalchemy.orm import Session

from app.models import (
    DimStock, DimSector, FactDailyPrice, FactTechnicalIndicator, 
    FactRecommendation, StockRecommendation
)
from app.repositories.price_repository import PriceRepository
from app.repositories.indicator_repository import IndicatorRepository
from app.repositories.recommendation_repository import RecommendationRepository
from app.services.advisory.advisor import StockScreener
from app.services.advisory.signals import SignalGenerator
from app.services.advisory.scoring import StockScorer
from app.utils.logger import get_logger

logger = get_logger(__name__)


@pytest.mark.integration
class TestRecommendationPath:
    """Test complete recommendation generation path end-to-end."""
    
    @pytest.fixture
    def recommendation_test_data(self, db_session: Session):
        """
        Setup complete test data for recommendation path:
        - 3 sample stocks
        - 30 days of price data (trending up with realistic OHLC)
        - Technical indicators (RSI, MACD, Bollinger Bands)
        """
        # Create sectors
        sector = DimSector(sector_id=1, sector_name="Banking")
        db_session.add(sector)
        db_session.flush()
        
        # Create test stocks
        stocks = [
            DimStock(
                stock_code="TEST1",
                company_name="Test Company One",  # Use company_name, not stock_name
                sector_id=sector.sector_id,
                exchange="NGX",
                is_active=True
            ),
            DimStock(
                stock_code="TEST2",
                company_name="Test Company Two",
                sector_id=sector.sector_id,
                exchange="NGX",
                is_active=True
            ),
            DimStock(
                stock_code="TEST3",
                company_name="Test Company Three",
                sector_id=sector.sector_id,
                exchange="NGX",
                is_active=True
            ),
        ]
        db_session.add_all(stocks)
        db_session.flush()
        
        # Create 30 days of realistic price data with uptrend
        base_date = date.today() - timedelta(days=30)
        prices = []
        
        for stock in stocks:
            for i in range(30):
                price_date = base_date + timedelta(days=i)
                
                # Create realistic OHLC data with uptrend
                base_price = Decimal("100.00") + Decimal(str(i * 0.5))
                close = base_price + Decimal("0.75")
                
                price = FactDailyPrice(
                    stock_id=stock.stock_id,
                    price_date=price_date,
                    open_price=base_price,
                    high_price=base_price + Decimal("2.50"),
                    low_price=base_price - Decimal("1.50"),
                    close_price=close,
                    volume=Decimal("1000000"),
                    source="TEST"
                )
                prices.append(price)
        
        db_session.add_all(prices)
        db_session.flush()
        
        # Create technical indicators for each stock
        # RSI, MACD, Bollinger Bands (simulated)
        indicators = []
        recommendation_date = date.today()
        
        for stock in stocks:
            # Add RSI indicator
            rsi_indicator = FactTechnicalIndicator(
                stock_id=stock.stock_id,
                indicator_type="RSI",
                indicator_date=recommendation_date,
                value=Decimal("65.0"),  # Overbought territory (>70 = strong buy)
                period=14
            )
            indicators.append(rsi_indicator)
            
            # Add MACD indicator
            macd_indicator = FactTechnicalIndicator(
                stock_id=stock.stock_id,
                indicator_type="MACD",
                indicator_date=recommendation_date,
                value=Decimal("2.5"),  # Positive = bullish
                period=26
            )
            indicators.append(macd_indicator)
            
            # Add Bollinger Bands Middle Band
            bb_indicator = FactTechnicalIndicator(
                stock_id=stock.stock_id,
                indicator_type="BB_MIDDLE",
                indicator_date=recommendation_date,
                value=Decimal("110.0"),
                period=20
            )
            indicators.append(bb_indicator)
        
        db_session.add_all(indicators)
        db_session.commit()
        
        # Refresh all objects to ensure IDs are populated
        for stock in stocks:
            db_session.refresh(stock)
        
        return {
            "stocks": stocks,
            "prices": prices,
            "indicators": indicators,
            "recommendation_date": recommendation_date
        }
    
    def test_recommendation_generation_complete_path(
        self, 
        db_session: Session,
        recommendation_test_data
    ):
        """
        Test complete recommendation generation path:
        1. Verify test data is properly loaded
        2. Initialize StockScreener with repositories
        3. Generate recommendations
        4. Assert recommendations are created with expected properties
        
        This catches regression bugs like:
        - AttributeError: stock.stock_name (should be stock.company_name)
        - AttributeError: self.db (should be self.session in repositories)
        - Empty recommendations due to broken scoring logic
        """
        logger.info("=" * 80)
        logger.info("TEST: RECOMMENDATION GENERATION END-TO-END PATH")
        logger.info("=" * 80)
        
        stocks = recommendation_test_data["stocks"]
        recommendation_date = recommendation_test_data["recommendation_date"]
        
        # Verify test data setup
        price_repo = PriceRepository(db_session)
        indicator_repo = IndicatorRepository(db_session)
        
        # Check prices loaded
        logger.info("\n[CHECK 1] Verifying price data loaded...")
        for stock in stocks:
            stock_prices = price_repo.get_by_stock_id(stock.stock_id)
            assert len(stock_prices) == 30, f"Expected 30 prices for {stock.stock_code}, got {len(stock_prices)}"
            logger.info(f"✓ {stock.stock_code}: {len(stock_prices)} prices loaded")
        
        # Check indicators loaded
        logger.info("\n[CHECK 2] Verifying technical indicators loaded...")
        for stock in stocks:
            indicators = indicator_repo.get_latest_by_stock(stock.stock_id, limit=10)
            assert len(indicators) >= 3, f"Expected >=3 indicators for {stock.stock_code}, got {len(indicators)}"
            logger.info(f"✓ {stock.stock_code}: {len(indicators)} indicators loaded")
        
        # Initialize StockScreener with test repositories
        logger.info("\n[CHECK 3] Initializing StockScreener...")
        signal_gen = SignalGenerator()
        scorer = StockScorer()
        screener = StockScreener(
            price_repo=price_repo,
            indicator_repo=indicator_repo,
            signal_generator=signal_gen,
            stock_scorer=scorer
        )
        logger.info("✓ StockScreener initialized successfully")
        
        # Generate recommendations
        logger.info(f"\n[CHECK 4] Generating recommendations for {recommendation_date}...")
        recommendations = screener.generate_recommendations(
            recommendation_date=recommendation_date,
            min_score=40.0,
            min_confidence=0.50
        )
        
        # Assert recommendations were generated
        assert len(recommendations) > 0, (
            f"Expected recommendations but got zero. "
            f"This indicates a regression in the recommendation path. "
            f"Check StockScreener._analyze_stock() for AttributeErrors or "
            f"RecommendationRepository for session attribute errors."
        )
        
        logger.info(f"✓ Generated {len(recommendations)} recommendations")
        
        # Verify recommendation properties
        logger.info("\n[CHECK 5] Validating recommendation properties...")
        for rec in recommendations:
            assert isinstance(rec, StockRecommendation)
            assert rec.stock_code in [s.stock_code for s in stocks]
            assert rec.recommendation_date == recommendation_date
            assert rec.score >= 40.0, f"Expected score >= 40.0, got {rec.score}"
            assert rec.confidence >= 0.50, f"Expected confidence >= 0.50, got {rec.confidence}"
            assert rec.signal in ["BUY", "SELL", "HOLD"], f"Invalid signal: {rec.signal}"
            
            logger.info(
                f"✓ {rec.stock_code}: {rec.signal} | "
                f"Score: {rec.score:.1f} | Confidence: {rec.confidence:.2f}"
            )
        
        # Persist recommendations to database
        logger.info("\n[CHECK 6] Persisting recommendations to database...")
        rec_repo = RecommendationRepository(db_session)
        saved_count = rec_repo.create_recommendations_bulk(recommendations)
        
        assert saved_count == len(recommendations), (
            f"Expected to save {len(recommendations)} recommendations, "
            f"but only saved {saved_count}. "
            f"Check RecommendationRepository.create_recommendations_bulk() "
            f"for session attribute errors."
        )
        
        # Verify persisted recommendations
        logger.info("\n[CHECK 7] Verifying persisted recommendations...")
        persisted = rec_repo.get_recommendations_by_date(recommendation_date)
        assert len(persisted) == len(recommendations), (
            f"Expected {len(recommendations)} persisted recommendations, "
            f"got {len(persisted)}"
        )
        logger.info(f"✓ {len(persisted)} recommendations persisted successfully")
        
        logger.info("\n" + "=" * 80)
        logger.info("✅ RECOMMENDATION PATH TEST PASSED")
        logger.info("=" * 80)
    
    def test_recommendation_path_with_zero_recommendations_scenario(
        self,
        db_session: Session
    ):
        """
        Test edge case: recommendation generation with insufficient data (stocks but no indicators).
        
        This verifies the graceful handling of stocks without technical indicators.
        Regression: Previously would crash with AttributeError.
        Expected: Should return empty list or handle gracefully.
        """
        logger.info("\n" + "=" * 80)
        logger.info("TEST: RECOMMENDATION PATH WITH INSUFFICIENT DATA")
        logger.info("=" * 80)
        
        # Create sector and stocks but NO price data or indicators
        sector = DimSector(sector_id=2, sector_name="Tech")
        db_session.add(sector)
        db_session.flush()
        
        stock = DimStock(
            stock_code="EMPTY",
            company_name="Empty Stock",
            sector_id=sector.sector_id,
            exchange="NGX",
            is_active=True
        )
        db_session.add(stock)
        db_session.commit()
        
        # Try to generate recommendations with minimal data
        logger.info("\n[CHECK 1] Attempting recommendation generation with minimal data...")
        price_repo = PriceRepository(db_session)
        indicator_repo = IndicatorRepository(db_session)
        signal_gen = SignalGenerator()
        scorer = StockScorer()
        
        screener = StockScreener(
            price_repo=price_repo,
            indicator_repo=indicator_repo,
            signal_generator=signal_gen,
            stock_scorer=scorer
        )
        
        recommendations = screener.generate_recommendations(
            recommendation_date=date.today(),
            min_score=40.0,
            min_confidence=0.50
        )
        
        # Should handle gracefully (empty or filtered out)
        logger.info(f"✓ Generated {len(recommendations)} recommendations (expected 0 with minimal data)")
        assert isinstance(recommendations, list), "Should return a list"
        
        logger.info("\n" + "=" * 80)
        logger.info("✅ EDGE CASE TEST PASSED")
        logger.info("=" * 80)
