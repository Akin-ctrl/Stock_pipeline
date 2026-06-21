-- =============================================================================
-- Migration: 2026_06_20_audit_integrity_and_schema_fixes.sql
--
-- Applies schema-level changes for the full audit bug-fix batch.
-- Safe to run idempotently (all DDL uses IF NOT EXISTS / DO NOTHING guards).
-- =============================================================================

BEGIN;

-- ---------------------------------------------------------------------------
-- H-1: backtest_runs — add unique constraint on (run_date, profile, run_type)
--      Prevents duplicate backtest run rows created by concurrent Airflow
--      workers or DAG retries that inflate win-rate / drawdown metrics.
--      Idempotent: skips silently if constraint already exists (e.g. created
--      by SQLAlchemy create_all on a fresh deployment).
-- ---------------------------------------------------------------------------
DO $$ BEGIN
    ALTER TABLE backtest_runs
        ADD CONSTRAINT ux_backtest_run_date_profile_type
        UNIQUE (run_date, profile, run_type);
EXCEPTION WHEN duplicate_object OR duplicate_table THEN
    RAISE NOTICE 'Constraint/index ux_backtest_run_date_profile_type already exists, skipping.';
END $$;

-- ---------------------------------------------------------------------------
-- H-2: staging_audit_log — add unique constraint on (stock_code, price_date)
--      Replaces the application-level SELECT-then-DELETE-duplicates approach
--      with a DB-enforced constraint + ON CONFLICT DO UPDATE upsert.
-- ---------------------------------------------------------------------------
-- Remove any existing duplicates before adding the constraint:
DELETE FROM staging_audit_log sal1
WHERE audit_id NOT IN (
    SELECT MAX(audit_id)
    FROM   staging_audit_log
    GROUP  BY stock_code, price_date
);

DO $$ BEGIN
    ALTER TABLE staging_audit_log
        ADD CONSTRAINT ux_audit_stock_date
        UNIQUE (stock_code, price_date);
EXCEPTION WHEN duplicate_object OR duplicate_table THEN
    RAISE NOTICE 'Constraint/index ux_audit_stock_date already exists, skipping.';
END $$;

-- ---------------------------------------------------------------------------
-- H-5: alert_rules — rename condition_sql → rule_config
--      Column was misleadingly named as if it held executable SQL; it stores
--      JSON rule parameter bags. Rename prevents accidental SQL injection.
--      Idempotent: skips if column was already created as rule_config by
--      SQLAlchemy create_all on a fresh deployment.
-- ---------------------------------------------------------------------------
DO $$ BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'alert_rules' AND column_name = 'condition_sql'
    ) THEN
        ALTER TABLE alert_rules RENAME COLUMN condition_sql TO rule_config;
    ELSE
        RAISE NOTICE 'Column condition_sql does not exist on alert_rules (already rule_config), skipping.';
    END IF;
END $$;

-- ---------------------------------------------------------------------------
-- H-6: fact_recommendation_audit — add explicit ON DELETE RESTRICT on FK
--      Default PostgreSQL FK behaviour without ON DELETE is already RESTRICT,
--      but making it explicit in DDL documents the intent and prevents
--      accidental schema changes from dropping the guard.
-- ---------------------------------------------------------------------------
ALTER TABLE fact_recommendation_audit
    DROP CONSTRAINT IF EXISTS fact_recommendation_audit_stock_id_fkey,
    ADD  CONSTRAINT fact_recommendation_audit_stock_id_fkey
         FOREIGN KEY (stock_id)
         REFERENCES dim_stocks (stock_id)
         ON DELETE RESTRICT;

-- Also apply to fact_recommendations (same pattern, same intent):
ALTER TABLE fact_recommendations
    DROP CONSTRAINT IF EXISTS fact_recommendations_stock_id_fkey,
    ADD  CONSTRAINT fact_recommendations_stock_id_fkey
         FOREIGN KEY (stock_id)
         REFERENCES dim_stocks (stock_id)
         ON DELETE RESTRICT;

-- ---------------------------------------------------------------------------
-- H-7: fact_daily_prices — ensure ingestion_timestamp has NOT NULL default
--      The column already has server_default=func.now() but was missing
--      NOT NULL, making NULL ingestion_timestamps silently possible from
--      ORM-level inserts that bypass the server default.
-- ---------------------------------------------------------------------------
ALTER TABLE fact_daily_prices
    ALTER COLUMN ingestion_timestamp SET NOT NULL,
    ALTER COLUMN ingestion_timestamp SET DEFAULT NOW();

COMMIT;
