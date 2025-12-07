#!/usr/bin/env python3
"""
Initialize database schema and seed reference data.

Creates all tables and populates dim_sectors and alert_rules.
"""

import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from app.config import get_db, get_settings
from app.models import Base, DimSector, AlertRule
from app.utils import get_logger
from decimal import Decimal

logger = get_logger("init_db")


def create_tables():
    """Create all database tables."""
    logger.info("Creating database tables...")
    
    db = get_db()
    Base.metadata.create_all(db.engine)
    
    logger.info("✅ All tables created successfully")


def seed_sectors():
    """Seed dim_sectors with reference data."""
    logger.info("Seeding sectors...")
    
    sectors = [
        ('Financials', 'Banks, Insurance, Mortgage, Asset Management'),
        ('Consumer Goods', 'Food, Beverages, Manufacturing'),
        ('Consumer Services', 'Transport, Hospitality, Media'),
        ('Technology', 'IT Services, Software, Telecommunications'),
        ('Basic Materials', 'Chemicals, Construction Materials, Mining'),
        ('Industrials', 'Manufacturing, Engineering, Construction'),
        ('Oil & Gas', 'Exploration, Production, Distribution'),
        ('Healthcare', 'Pharmaceuticals, Hospitals, Medical Equipment'),
        ('Utilities', 'Power Generation, Water, Infrastructure'),
    ]
    
    db = get_db()
    with db.get_session() as session:
        # Check if sectors already exist
        existing = session.query(DimSector).count()
        if existing > 0:
            logger.info(f"Sectors already seeded ({existing} records), skipping...")
            return
        
        for name, description in sectors:
            sector = DimSector(sector_name=name, description=description)
            session.add(sector)
        
        session.commit()
        logger.info(f"✅ Seeded {len(sectors)} sectors")


def seed_alert_rules():
    """Seed alert_rules with pre-defined investment rules."""
    logger.info("Seeding alert rules...")
    
    rules = [
        {
            'rule_name': 'Daily_Change_Significant',
            'rule_type': 'PRICE_MOVEMENT',
            'threshold_value': Decimal('4.0'),
            'severity': 'WARNING',
            'description': 'Daily price change exceeds ±4% - requires attention'
        },
        {
            'rule_name': 'Daily_Change_Extreme',
            'rule_type': 'PRICE_MOVEMENT',
            'threshold_value': Decimal('8.0'),
            'severity': 'CRITICAL',
            'description': 'Daily price change exceeds ±8% - immediate review needed'
        },
        {
            'rule_name': 'MA_Bullish_Crossover',
            'rule_type': 'MA_CROSSOVER',
            'threshold_value': Decimal('0'),
            'severity': 'INFO',
            'description': '7-day MA crosses above 30-day MA - potential buy signal'
        },
        {
            'rule_name': 'MA_Bearish_Crossover',
            'rule_type': 'MA_CROSSOVER',
            'threshold_value': Decimal('0'),
            'severity': 'WARNING',
            'description': '7-day MA crosses below 30-day MA - potential sell signal'
        },
        {
            'rule_name': 'Volatility_Spike',
            'rule_type': 'VOLATILITY',
            'threshold_value': Decimal('2.0'),
            'severity': 'WARNING',
            'description': 'Volatility exceeds 2x 30-day average - market uncertainty'
        },
        {
            'rule_name': 'Volume_Surge',
            'rule_type': 'VOLUME_SPIKE',
            'threshold_value': Decimal('2.5'),
            'severity': 'INFO',
            'description': 'Volume exceeds 2.5x average - unusual activity'
        },
        {
            'rule_name': 'RSI_Oversold',
            'rule_type': 'RSI',
            'threshold_value': Decimal('30'),
            'severity': 'INFO',
            'description': 'RSI below 30 - potential undervalued (buy opportunity)'
        },
        {
            'rule_name': 'RSI_Overbought',
            'rule_type': 'RSI',
            'threshold_value': Decimal('70'),
            'severity': 'WARNING',
            'description': 'RSI above 70 - potential overvalued (sell consideration)'
        },
        {
            'rule_name': 'MACD_Bullish_Cross',
            'rule_type': 'MACD',
            'threshold_value': Decimal('0'),
            'severity': 'INFO',
            'description': 'MACD crosses above signal line - bullish momentum'
        },
        {
            'rule_name': 'MACD_Bearish_Cross',
            'rule_type': 'MACD',
            'threshold_value': Decimal('0'),
            'severity': 'WARNING',
            'description': 'MACD crosses below signal line - bearish momentum'
        },
    ]
    
    db = get_db()
    with db.get_session() as session:
        # Check if rules already exist
        existing = session.query(AlertRule).count()
        if existing > 0:
            logger.info(f"Alert rules already seeded ({existing} records), skipping...")
            return
        
        for rule_data in rules:
            rule = AlertRule(**rule_data)
            session.add(rule)
        
        session.commit()
        logger.info(f"✅ Seeded {len(rules)} alert rules")


def verify_setup():
    """Verify database setup."""
    logger.info("Verifying database setup...")
    
    db = get_db()
    
    # Check connection
    if not db.health_check():
        logger.error("❌ Database health check failed")
        return False
    
    with db.get_session() as session:
        # Count records
        sector_count = session.query(DimSector).count()
        rule_count = session.query(AlertRule).count()
        
        logger.info(f"📊 Database statistics:")
        logger.info(f"  - Sectors: {sector_count}")
        logger.info(f"  - Alert Rules: {rule_count}")
        
        if sector_count < 9 or rule_count < 10:
            logger.warning("⚠️  Expected 9 sectors and 10 alert rules")
            return False
    
    logger.info("✅ Database setup verified successfully")
    return True


def main():
    """Main initialization function."""
    logger.info("=" * 60)
    logger.info("Stock Pipeline - Database Initialization")
    logger.info("=" * 60)
    
    try:
        settings = get_settings()
        logger.info(f"Environment: {settings.environment}")
        logger.info(f"Database: {settings.database.host}/{settings.database.database}")
        
        # Step 1: Create tables
        create_tables()
        
        # Step 2: Seed reference data
        seed_sectors()
        seed_alert_rules()
        
        # Step 3: Verify
        if verify_setup():
            logger.info("=" * 60)
            logger.info("✅ Database initialization complete!")
            logger.info("=" * 60)
            return 0
        else:
            logger.error("❌ Database verification failed")
            return 1
    
    except Exception as e:
        logger.critical("Database initialization failed", error=e)
        return 1


if __name__ == "__main__":
    sys.exit(main())
