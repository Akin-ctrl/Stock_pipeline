"""
Pytest configuration and fixtures for integration tests.

Provides database setup, teardown, and common test fixtures.
"""

import os
import pytest
from datetime import date, datetime, timedelta
from decimal import Decimal
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, Session

from app.models.base import Base
from app.models import DimSector, DimStock, FactDailyPrice, FactTechnicalIndicator, AlertRule, AlertHistory


# Test database configuration
TEST_DB_USER = os.getenv("POSTGRES_USER", "stock_user")
TEST_DB_PASSWORD = os.getenv("POSTGRES_PASSWORD", "stock_password")
TEST_DB_HOST = os.getenv("POSTGRES_HOST", "localhost")
TEST_DB_PORT = os.getenv("POSTGRES_PORT", "5432")
TEST_DB_NAME = "stock_pipeline_test"

TEST_DATABASE_URL = f"postgresql://{TEST_DB_USER}:{TEST_DB_PASSWORD}@{TEST_DB_HOST}:{TEST_DB_PORT}/{TEST_DB_NAME}"


@pytest.fixture(scope="session")
def engine():
    """Create test database engine for the entire test session."""
    # Create test database if it doesn't exist
    default_engine = create_engine(
        f"postgresql://{TEST_DB_USER}:{TEST_DB_PASSWORD}@{TEST_DB_HOST}:{TEST_DB_PORT}/postgres",
        isolation_level="AUTOCOMMIT"
    )
    
    with default_engine.connect() as conn:
        # Check if test database exists
        result = conn.execute(
            text(f"SELECT 1 FROM pg_database WHERE datname = '{TEST_DB_NAME}'")
        )
        exists = result.scalar()
        
        if not exists:
            conn.execute(text(f"CREATE DATABASE {TEST_DB_NAME}"))
            print(f"\n✅ Created test database: {TEST_DB_NAME}")
    
    default_engine.dispose()
    
    # Create engine for test database
    test_engine = create_engine(
        TEST_DATABASE_URL,
        echo=False,
        pool_pre_ping=True
    )
    
    # Create all tables
    Base.metadata.create_all(test_engine)
    print(f"✅ Created all tables in {TEST_DB_NAME}")
    
    yield test_engine
    
    # Cleanup: drop all tables after tests
    Base.metadata.drop_all(test_engine)
    test_engine.dispose()
    print(f"✅ Cleaned up test database: {TEST_DB_NAME}")


@pytest.fixture(scope="function")
def db_session(engine):
    """Create a new database session for each test function."""
    # Create session factory
    TestSessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)
    
    # Create session
    session = TestSessionLocal()
    
    yield session
    
    # Rollback any uncommitted changes
    session.rollback()
    session.close()
    
    # Clean up all data after each test
    for table in reversed(Base.metadata.sorted_tables):
        session.execute(table.delete())
    session.commit()


@pytest.fixture
def sample_sectors(db_session: Session):
    """Create sample sectors for testing."""
    sectors = [
        DimSector(sector_id=1, sector_name="Banking"),
        DimSector(sector_id=2, sector_name="Consumer Goods"),
        DimSector(sector_id=3, sector_name="Oil & Gas"),
        DimSector(sector_id=4, sector_name="Telecommunications"),
        DimSector(sector_id=5, sector_name="Industrial Goods"),
    ]
    
    db_session.add_all(sectors)
    db_session.commit()
    
    for sector in sectors:
        db_session.refresh(sector)
    
    return sectors


@pytest.fixture
def sample_stocks(db_session: Session, sample_sectors):
    """Create sample stocks for testing."""
    stocks = [
        DimStock(
            stock_code="DANGCEM",
            company_name="Dangote Cement Plc",
            sector_id=5,
            exchange="NGX",
            is_active=True
        ),
        DimStock(
            stock_code="GTCO",
            company_name="Guaranty Trust Holding Company Plc",
            sector_id=1,
            exchange="NGX",
            is_active=True
        ),
        DimStock(
            stock_code="MTNN",
            company_name="MTN Nigeria Communications Plc",
            sector_id=4,
            exchange="NGX",
            is_active=True
        ),
        DimStock(
            stock_code="NESTLE",
            company_name="Nestle Nigeria Plc",
            sector_id=2,
            exchange="NGX",
            is_active=True
        ),
    ]
    
    db_session.add_all(stocks)
    db_session.commit()
    
    for stock in stocks:
        db_session.refresh(stock)
    
    return stocks


@pytest.fixture
def sample_prices(db_session: Session, sample_stocks):
    """Create sample price data for testing."""
    prices = []
    base_date = date.today() - timedelta(days=30)
    
    for stock in sample_stocks[:2]:  # Only for first 2 stocks
        for i in range(30):
            price_date = base_date + timedelta(days=i)
            # Simple price generation (trending up slightly)
            base_price = Decimal("100.00")
            daily_price = base_price + Decimal(str(i * 0.5))
            
            price = FactDailyPrice(
                stock_id=stock.stock_id,
                price_date=price_date,
                open_price=daily_price,
                high_price=daily_price + Decimal("2.00"),
                low_price=daily_price - Decimal("1.50"),
                close_price=daily_price + Decimal("0.50"),
                volume=1000000 + (i * 10000),
                source="TEST"
            )
            prices.append(price)
    
    db_session.add_all(prices)
    db_session.commit()
    
    for price in prices:
        db_session.refresh(price)
    
    return prices


@pytest.fixture
def sample_indicators(db_session: Session, sample_stocks, sample_prices):
    """Create sample indicator data for testing."""
    indicators = []
    base_date = date.today() - timedelta(days=20)
    
    stock = sample_stocks[0]
    
    for i in range(20):
        indicator_date = base_date + timedelta(days=i)
        
        indicator = FactTechnicalIndicator(
            stock_id=stock.stock_id,
            calculation_date=indicator_date,
            ma_7=Decimal("100.00") + Decimal(str(i * 0.3)),
            ma_30=Decimal("99.00") + Decimal(str(i * 0.2)),
            ma_90=Decimal("98.00") + Decimal(str(i * 0.1)),
            rsi_14=Decimal("50.00") + Decimal(str(i)),
            macd=Decimal("0.50"),
            macd_signal=Decimal("0.30"),
            macd_histogram=Decimal("0.20"),
            bollinger_upper=Decimal("105.00"),
            bollinger_middle=Decimal("100.00"),
            bollinger_lower=Decimal("95.00")
        )
        indicators.append(indicator)
    
    db_session.add_all(indicators)
    db_session.commit()
    
    for indicator in indicators:
        db_session.refresh(indicator)
    
    return indicators


@pytest.fixture
def sample_alert_rules(db_session: Session):
    """Create sample alert rules for testing."""
    rules = [
        AlertRule(
            rule_name="RSI Oversold",
            rule_type="RSI",
            condition_sql="rsi_14 < 30",
            threshold_value=Decimal("30.00"),
            severity="WARNING",
            is_active=True
        ),
        AlertRule(
            rule_name="RSI Overbought",
            rule_type="RSI",
            condition_sql="rsi_14 > 70",
            threshold_value=Decimal("70.00"),
            severity="WARNING",
            is_active=True
        ),
        AlertRule(
            rule_name="Price Above SMA",
            rule_type="MA_CROSSOVER",
            condition_sql="close_price > ma_30",
            threshold_value=None,
            severity="INFO",
            is_active=True
        ),
    ]
    
    db_session.add_all(rules)
    db_session.commit()
    
    for rule in rules:
        db_session.refresh(rule)
    
    return rules


@pytest.fixture
def clean_database(db_session: Session):
    """Ensure database is clean before test."""
    # Delete all data in reverse order of foreign keys
    db_session.query(AlertHistory).delete()
    db_session.query(AlertRule).delete()
    db_session.query(FactTechnicalIndicator).delete()
    db_session.query(FactDailyPrice).delete()
    db_session.query(DimStock).delete()
    db_session.query(DimSector).delete()
    db_session.commit()
    
    return db_session
