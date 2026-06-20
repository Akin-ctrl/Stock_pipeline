#!/bin/bash
set -e

echo "🔄 Waiting for PostgreSQL to be ready..."
until python3 - <<'PYCHECK'
import os, sys
try:
    import psycopg2
    conn = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        dbname="postgres",
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        connect_timeout=3,
    )
    conn.close()
    sys.exit(0)
except Exception:
    sys.exit(1)
PYCHECK
do
  sleep 1
done
echo "✅ PostgreSQL is ready!"

echo "🔄 Initializing database schema..."
cd /
export PYTHONPATH=/:$PYTHONPATH
python3 << 'PYEOF'
import sys
import time
import os
from sqlalchemy import create_engine, text

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

def ensure_database_exists():
    """Create the application database if it does not exist."""
    db_name = os.getenv("POSTGRES_DB")
    db_user = os.getenv("POSTGRES_USER")
    db_password = os.getenv("POSTGRES_PASSWORD")
    db_host = os.getenv("POSTGRES_HOST", "postgres")
    db_port = os.getenv("POSTGRES_PORT", "5432")

    if not all([db_name, db_user, db_password]):
        logger.error("Missing database env vars for initialization")
        sys.exit(1)

    maintenance_url = (
        f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/postgres"
    )
    engine = create_engine(maintenance_url, isolation_level="AUTOCOMMIT")
    try:
        with engine.connect() as conn:
            exists = conn.execute(
                text("SELECT 1 FROM pg_database WHERE datname = :db"),
                {"db": db_name}
            ).scalar()
            if not exists:
                logger.info(f"Creating database: {db_name}")
                conn.execute(text(f"CREATE DATABASE {db_name}"))
                logger.info("Database created")
    finally:
        engine.dispose()

def init_database():
    """Initialize database schema and seed data."""
    try:
        ensure_database_exists()
        if not wait_for_db():
            logger.error("❌ Database connection failed")
            sys.exit(1)
        
        db = get_db()
        
        # Create all tables from models
        logger.info("📊 Creating database tables...")
        Base.metadata.create_all(db.engine)
        logger.info("✅ Tables created successfully")

        # Apply SQL migrations in filename order (date-prefixed names sort correctly).
        # Migrations use BEGIN/COMMIT internally; we connect with autocommit so
        # psycopg2 does not wrap each file in an extra implicit transaction.
        # _migration_history tracks which files have been applied so re-starts
        # never re-run destructive DDL (DROP TABLE / DROP VIEW) a second time.
        import glob
        migration_dir = "/app/db/migrations"
        migration_files = sorted(glob.glob(f"{migration_dir}/*.sql"))
        if migration_files:
            logger.info(f"📦 Found {len(migration_files)} SQL migration(s)")
            raw_conn = db.engine.raw_connection()
            try:
                raw_conn.autocommit = True
                cursor = raw_conn.cursor()

                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS _migration_history (
                        migration_name TEXT PRIMARY KEY,
                        applied_at TIMESTAMPTZ DEFAULT NOW()
                    )
                """)

                applied_count = 0
                skipped_count = 0
                for mf in migration_files:
                    migration_name = os.path.basename(mf)
                    cursor.execute(
                        "SELECT 1 FROM _migration_history WHERE migration_name = %s",
                        (migration_name,)
                    )
                    if cursor.fetchone():
                        logger.info(f"  ⏭  {migration_name} (already applied)")
                        skipped_count += 1
                        continue

                    logger.info(f"  → {migration_name}")
                    with open(mf, 'r') as f:
                        sql = f.read()
                    cursor.execute(sql)
                    cursor.execute(
                        "INSERT INTO _migration_history (migration_name) VALUES (%s) ON CONFLICT DO NOTHING",
                        (migration_name,)
                    )
                    applied_count += 1

                cursor.close()
                logger.info(f"✅ Migrations complete: {applied_count} applied, {skipped_count} skipped")
            except Exception as exc:
                logger.error(f"❌ Migration failed: {exc}")
                raw_conn.close()
                raise RuntimeError(f"Migration failed: {exc}") from exc
            finally:
                raw_conn.close()
        else:
            logger.info("ℹ️  No SQL migrations found in " + migration_dir)
        
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
        # Write sentinel so the Docker healthcheck can detect completion.
        with open("/tmp/init-complete", "w") as _f:
            _f.write("ok")
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
