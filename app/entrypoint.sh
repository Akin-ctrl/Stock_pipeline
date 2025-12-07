#!/bin/bash
set -e

echo "🔄 Waiting for PostgreSQL to be ready..."
while ! pg_isready -h postgres -U stock_user -d stock_pipeline > /dev/null 2>&1; do
  sleep 1
done
echo "✅ PostgreSQL is ready!"

echo "🔄 Initializing database schema..."
cd /
export PYTHONPATH=/:$PYTHONPATH
python3 << 'PYEOF'
import sys
import time

# Now we can use app. prefix since we're at root
from app.models import Base, DimSector, AlertRule
from app.config import get_db
from app.utils import get_logger

logger = get_logger("entrypoint")

def wait_for_db(max_attempts=30):
    """Wait for database to be ready."""
    for i in range(max_attempts):
        try:
            db = get_db()
            if db.health_check():
                logger.info("✅ Database connection established")
                return True
        except Exception as e:
            logger.debug(f"Waiting for database... attempt {i+1}/{max_attempts}")
            time.sleep(2)
    return False

def init_database():
    """Initialize database schema and seed data."""
    try:
        if not wait_for_db():
            logger.error("❌ Database connection failed")
            sys.exit(1)
        
        db = get_db()
        
        # Create all tables from models
        logger.info("📊 Creating database tables...")
        Base.metadata.create_all(db.engine)
        logger.info("✅ Tables created successfully")
        
        # Seed sectors (if not exists)
        with db.get_session() as session:
            sector_count = session.query(DimSector).count()
            if sector_count == 0:
                logger.info("🌱 Seeding sectors...")
                sectors = [
                    DimSector(sector_name='Financials', description='Banks, Insurance, Mortgage, Asset Management'),
                    DimSector(sector_name='Consumer Goods', description='Food, Beverages, Manufacturing'),
                    DimSector(sector_name='Consumer Services', description='Transport, Hospitality, Media'),
                    DimSector(sector_name='Technology', description='IT Services, Software, Telecommunications'),
                    DimSector(sector_name='Basic Materials', description='Chemicals, Construction Materials, Mining'),
                    DimSector(sector_name='Industrials', description='Manufacturing, Engineering, Construction'),
                    DimSector(sector_name='Oil & Gas', description='Exploration, Production, Distribution'),
                    DimSector(sector_name='Healthcare', description='Pharmaceuticals, Hospitals, Medical Equipment'),
                    DimSector(sector_name='Utilities', description='Power Generation, Water, Infrastructure')
                ]
                session.add_all(sectors)
                logger.info(f"✅ Seeded {len(sectors)} sectors")
            else:
                logger.info(f"ℹ️  Sectors already exist ({sector_count} records)")
        
        # Seed alert rules (if not exists)
        with db.get_session() as session:
            rule_count = session.query(AlertRule).count()
            if rule_count == 0:
                logger.info("🌱 Seeding alert rules...")
                rules = [
                    AlertRule(
                        rule_name='Daily_Change_Significant',
                        rule_type='PRICE_MOVEMENT',
                        threshold_value=4.0,
                        severity='WARNING',
                        description='Daily price change exceeds ±4%'
                    ),
                    AlertRule(
                        rule_name='Daily_Change_Extreme',
                        rule_type='PRICE_MOVEMENT',
                        threshold_value=8.0,
                        severity='CRITICAL',
                        description='Daily price change exceeds ±8%'
                    ),
                    AlertRule(
                        rule_name='MA_Bullish_Crossover',
                        rule_type='MA_CROSSOVER',
                        threshold_value=0,
                        severity='INFO',
                        description='7-day MA crosses above 30-day MA'
                    ),
                    AlertRule(
                        rule_name='MA_Bearish_Crossover',
                        rule_type='MA_CROSSOVER',
                        threshold_value=0,
                        severity='WARNING',
                        description='7-day MA crosses below 30-day MA'
                    ),
                    AlertRule(
                        rule_name='Volatility_Spike',
                        rule_type='VOLATILITY',
                        threshold_value=2.0,
                        severity='WARNING',
                        description='Volatility exceeds 2x 30-day average'
                    ),
                    AlertRule(
                        rule_name='Volume_Surge',
                        rule_type='VOLUME_SPIKE',
                        threshold_value=2.5,
                        severity='INFO',
                        description='Volume exceeds 2.5x average'
                    ),
                    AlertRule(
                        rule_name='RSI_Oversold',
                        rule_type='RSI',
                        threshold_value=30,
                        severity='INFO',
                        description='RSI below 30 - potential buy signal'
                    ),
                    AlertRule(
                        rule_name='RSI_Overbought',
                        rule_type='RSI',
                        threshold_value=70,
                        severity='WARNING',
                        description='RSI above 70 - potential sell signal'
                    )
                ]
                session.add_all(rules)
                logger.info(f"✅ Seeded {len(rules)} alert rules")
            else:
                logger.info(f"ℹ️  Alert rules already exist ({rule_count} records)")
        
        logger.info("🎉 Database initialization complete!")
        return True
        
    except Exception as e:
        logger.error(f"❌ Database initialization failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    init_database()
PYEOF

echo "✅ Database initialization complete!"
echo "🚀 Starting application..."

# Keep container running or execute CMD
exec "$@"
