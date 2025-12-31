"""Integration tests for repositories - simplified version."""

import pytest
from datetime import date, timedelta
from decimal import Decimal
from sqlalchemy.orm import Session

from app.models import DimSector, DimStock, FactDailyPrice, FactTechnicalIndicator
from app.repositories.stock_repository import StockRepository
from app.repositories.price_repository import PriceRepository
from app.repositories.indicator_repository import IndicatorRepository


@pytest.mark.integration
@pytest.mark.database
class TestStockRepository:
    """Test StockRepository database operations."""
    
    def test_get_all(self, db_session: Session, sample_stocks):
        """Test retrieving all stocks."""
        repo = StockRepository(db_session)
        stocks = repo.get_all()
        assert len(stocks) == len(sample_stocks)
        assert all(isinstance(s, DimStock) for s in stocks)
    
    def test_get_by_code(self, db_session: Session, sample_stocks):
        """Test retrieving stock by code."""
        repo = StockRepository(db_session)
        stock = repo.get_by_code("DANGCEM")
        assert stock is not None
        assert stock.stock_code == "DANGCEM"
    
    def test_get_all_active(self, db_session: Session, sample_stocks):
        """Test filtering active stocks."""
        repo = StockRepository(db_session)
        active_stocks = repo.get_all_active()
        assert len(active_stocks) > 0
        assert all(s.is_active for s in active_stocks)
    
    def test_create_stock(self, db_session: Session, sample_sectors):
        """Test creating a new stock."""
        repo = StockRepository(db_session)
        new_stock = repo.create_stock(
            stock_code="TESTSTOCK",
            company_name="Test Company Plc",
            sector_id=sample_sectors[0].sector_id,
            exchange="NGX"
        )
        assert new_stock.stock_id is not None
        assert new_stock.stock_code == "TESTSTOCK"


@pytest.mark.integration
@pytest.mark.database
class TestPriceRepository:
    """Test PriceRepository database operations."""
    
    def test_get_latest(self, db_session: Session, sample_stocks, sample_prices):
        """Test getting latest price for a stock."""
        repo = PriceRepository(db_session)
        stock = sample_stocks[0]
        latest = repo.get_latest(stock.stock_id)
        assert latest is not None
        assert latest.stock_id == stock.stock_id
    
    def test_bulk_insert(self, db_session: Session, sample_stocks):
        """Test bulk inserting prices."""
        repo = PriceRepository(db_session)
        stock = sample_stocks[0]
        
        prices_data = []
        for i in range(5):
            prices_data.append({
                "stock_id": stock.stock_id,
                "price_date": date.today() - timedelta(days=i),
                "close_price": 100.0 + i,
                "source": "TEST"
            })
        
        count = repo.bulk_insert_prices(prices_data)
        assert count == 5
    
    def test_upsert_price(self, db_session: Session, sample_stocks):
        """Test upserting price data."""
        repo = PriceRepository(db_session)
        stock = sample_stocks[0]
        
        price = repo.upsert_price(
            stock_id=stock.stock_id,
            price_date=date.today(),
            close_price=float(Decimal("100.50")),
            source="TEST"
        )
        assert price is not None
        assert float(price.close_price) == 100.50


@pytest.mark.integration
@pytest.mark.database
class TestIndicatorRepository:
    """Test IndicatorRepository database operations."""
    
    def test_get_latest(self, db_session: Session, sample_stocks, sample_indicators):
        """Test getting latest indicator."""
        repo = IndicatorRepository(db_session)
        stock = sample_stocks[0]
        latest = repo.get_latest(stock.stock_id)
        assert latest is not None
        assert latest.stock_id == stock.stock_id
    
    def test_save_indicators(self, db_session: Session, sample_stocks):
        """Test saving indicators."""
        repo = IndicatorRepository(db_session)
        stock = sample_stocks[0]
        
        indicator = repo.save_indicators(
            stock_id=stock.stock_id,
            calculation_date=date.today(),
            indicators={
                "ma_7": Decimal("100.00"),
                "rsi_14": Decimal("55.50")
            }
        )
        assert indicator is not None
        assert indicator.ma_7 == Decimal("100.00")


@pytest.mark.integration
@pytest.mark.database
def test_repository_relationships(db_session: Session, sample_stocks, sample_prices, sample_indicators):
    """Test that repository relationships work correctly."""
    stock_repo = StockRepository(db_session)
    stock = sample_stocks[0]
    retrieved_stock = stock_repo.get_by_id(stock.stock_id)
    
    assert len(retrieved_stock.prices) > 0
    assert len(retrieved_stock.indicators) > 0
    assert all(isinstance(p, FactDailyPrice) for p in retrieved_stock.prices)
    assert all(isinstance(ind, FactTechnicalIndicator) for ind in retrieved_stock.indicators)
