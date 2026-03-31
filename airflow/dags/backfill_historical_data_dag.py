"""
Manual Backfill DAG for Historical Afrimarket Data

This DAG is NOT scheduled and runs only when manually triggered.
Use it to backfill historical price data into the staging table before
running the main pipeline.

Workflow:
1. Backfill historical data from Afrimarket API into staging_daily_prices
2. Validate schema and table structure compatibility
3. Verify loaded data meets requirements

Triggered manually via:
  airflow dags trigger backfill_historical_data \
    --conf '{"years": 5, "stocks": "DANGCEM,ZENITHBANK,GTCO"}'
  
  airflow dags trigger backfill_historical_data \
    --conf '{"start_date": "2020-01-01", "end_date": "2023-12-31"}'
  
  airflow dags trigger backfill_historical_data \
    --conf '{"test": true}'  # 1 year, 5 stocks
"""

from datetime import datetime, timedelta
import sys
import os
from pathlib import Path
import json

import pendulum
from airflow.decorators import dag, task, task_group
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.utils.trigger_rule import TriggerRule
from airflow.exceptions import AirflowException

# Add project root to path
sys.path.insert(0, '/home/Stock_pipeline')

from scripts.backfill_historical_afrimarket import HistoricalBackfill
from app.utils.logger import get_logger

logger = get_logger(__name__)

# Default arguments for all tasks
DEFAULT_ARGS = {
    "owner": "data_engineer",
    "depends_on_past": False,
    "email": ["alerts@stockpipeline.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
    "execution_timeout": timedelta(hours=4),  # Backfill can take longer
}


@dag(
    dag_id="backfill_historical_data",
    description="Manual backfill of historical Afrimarket data to staging",
    default_args=DEFAULT_ARGS,
    schedule_interval=None,  # NOT scheduled - manual trigger only
    start_date=pendulum.parse("2026-01-01"),
    catchup=False,
    tags=["manual", "backfill", "staging"],
    doc_md=__doc__,
)
def backfill_historical_data_dag():
    """Main DAG for historical data backfill."""
    
    @task(task_id="parse_config")
    def parse_backfill_config(**context):
        """
        Parse and validate backfill configuration from trigger params.
        
        Expected config (via --conf):
        {
            "years": 5,  # OR
            "start_date": "2020-01-01",
            "end_date": "2023-12-31",  # OR
            "test": true,
            
            "stocks": "DANGCEM,ZENITHBANK",  # Optional: specific stocks
            "batch_size": 1000  # Optional: default 1000
        }
        
        Returns:
            Dict with parsed configuration
        """
        try:
            # Get trigger params
            dag_run = context.get('dag_run')
            conf = dag_run.conf if dag_run and dag_run.conf else {}
            
            logger.info(f"Trigger config received: {conf}")
            
            # Default to 1 year if no configuration provided
            if not conf:
                logger.info("No configuration provided, defaulting to 1 year backfill")
                conf = {"years": 1}
            
            # Validate that at least one date config is provided
            has_years = 'years' in conf
            has_dates = 'start_date' in conf and 'end_date' in conf
            has_test = conf.get('test', False)
            
            if not (has_years or has_dates or has_test):
                raise AirflowException(
                    "Must provide one of: years, start_date+end_date, or test=true"
                )
            
            # Validate mutual exclusivity
            if sum([has_years, has_dates, has_test]) > 1:
                logger.warning("Multiple configs provided, using precedence: test > start_date+end_date > years")
            
            parsed_config = {
                "years": conf.get('years'),
                "start_date": conf.get('start_date'),
                "end_date": conf.get('end_date'),
                "test": conf.get('test', False),
                "stocks": conf.get('stocks'),  # Optional: comma-separated (None = all stocks)
                "batch_size": conf.get('batch_size', 1000),
            }
            
            logger.info(f"Parsed config: {parsed_config}")
            return parsed_config
            
        except Exception as e:
            logger.error(f"Config parsing failed: {str(e)}")
            raise AirflowException(f"Configuration error: {str(e)}")
    
    @task(task_id="backfill_afrimarket_data")
    def run_backfill(parsed_config: dict, **context) -> dict:
        """
        Execute the historical backfill process.
        
        Args:
            parsed_config: Configuration dict from parse_config task
            
        Returns:
            Dict with backfill results (success, record count, etc.)
        """
        try:
            logger.info("Starting historical backfill execution")
            
            # Parse dates
            start_date = None
            end_date = None
            stock_codes = None
            
            if parsed_config.get('test'):
                logger.info("Running in TEST MODE: 1 year, first 5 stocks")
                # Will be handled by HistoricalBackfill class
            elif parsed_config.get('start_date') and parsed_config.get('end_date'):
                from datetime import datetime
                start_date = datetime.strptime(parsed_config['start_date'], '%Y-%m-%d').date()
                end_date = datetime.strptime(parsed_config['end_date'], '%Y-%m-%d').date()
                logger.info(f"Using date range: {start_date} to {end_date}")
            elif parsed_config.get('years'):
                from datetime import timedelta, date
                end_date = date.today() - timedelta(days=1)
                start_date = date.today() - timedelta(days=365 * parsed_config['years'])
                logger.info(f"Using {parsed_config['years']} years: {start_date} to {end_date}")
            
            # Parse stocks if provided
            if parsed_config.get('stocks'):
                stock_codes = [s.strip().upper() for s in parsed_config['stocks'].split(',')]
                logger.info(f"Backfilling specific stocks: {stock_codes}")
            
            # Create backfill instance
            backfill = HistoricalBackfill(
                start_date=start_date,
                end_date=end_date,
                stock_codes=stock_codes,
                batch_size=parsed_config.get('batch_size', 1000),
                rate_limit_delay=0.5
            )
            
            # Run backfill
            result = backfill.run()
            
            logger.info(f"Backfill completed: {result}")
            
            # Push result to XCom for downstream tasks
            context['task_instance'].xcom_push(key='backfill_result', value=result)
            
            return result
            
        except Exception as e:
            logger.error(f"Backfill execution failed: {str(e)}", error=e)
            raise AirflowException(f"Backfill failed: {str(e)}")
    
    @task(task_id="verify_staging_data")
    def verify_staging_data(backfill_result: dict) -> dict:
        """
        Verify that data was successfully loaded to staging.
        
        Args:
            backfill_result: Result dict from backfill task
            
        Returns:
            Verification dict with staging counts and health checks
        """
        try:
            from app.config.database import get_db
            from app.repositories.staging_repository import StagingRepository
            from sqlalchemy import text
            
            logger.info("Verifying staged data...")
            
            db = get_db()
            
            # Check total records in staging
            with db.get_session() as session:
                staging_repo = StagingRepository(session)

                start_date = backfill_result.get('start_date')
                end_date = backfill_result.get('end_date')
                
                # Get staging stats
                result = session.execute(
                    text("""
                        SELECT 
                            COUNT(*) as total_records,
                            COUNT(DISTINCT stock_code) as unique_stocks,
                            MIN(price_date) as oldest_date,
                            MAX(price_date) as newest_date,
                            COUNT(CASE WHEN close_price IS NULL OR close_price <= 0 THEN 1 END) as invalid_prices,
                            COUNT(CASE WHEN change_1d_pct IS NULL THEN 1 END) as missing_1d_change,
                            COUNT(CASE WHEN change_ytd_pct IS NULL THEN 1 END) as missing_ytd_change
                        FROM staging_daily_prices
                        WHERE source = 'afrimarket'
                          AND (:start_date IS NULL OR price_date >= CAST(:start_date AS DATE))
                          AND (:end_date IS NULL OR price_date <= CAST(:end_date AS DATE))
                    """)
                    , {
                        "start_date": start_date,
                        "end_date": end_date,
                    }
                )
                
                stats = dict(result.fetchone()._mapping)
                logger.info(f"Staging stats: {stats}")
                
                # Validate minimum requirements
                if stats['total_records'] == 0:
                    logger.warning("No records found in staging after backfill!")
                    verification = {
                        "status": "WARNING",
                        "backfill_result": backfill_result,
                        "staging_stats": stats,
                        "issue": "No records staged",
                        "next_step": "Review failed/skipped stocks and rerun backfill or adjust source coverage"
                    }
                    logger.info(f"Staging verification: {verification}")
                    return verification
                
                if stats['invalid_prices'] > 0:
                    logger.warning(f"Found {stats['invalid_prices']} invalid prices in staging")
                
                if stats['unique_stocks'] < 5:
                    logger.warning(f"Only {stats['unique_stocks']} stocks staged (expected many more)")
                
                success = (
                    backfill_result.get('success', False) and
                    stats['total_records'] > 0 and
                    stats['unique_stocks'] > 0 and
                    stats['invalid_prices'] == 0
                )
                
                status = "SUCCESS" if success else "WARNING"
                
                verification = {
                    "status": status,
                    "backfill_result": backfill_result,
                    "staging_stats": stats,
                    "next_step": "Run main pipeline (nigerian_stock_pipeline_v2) to reconcile, transform, and load to production"
                }
                
                logger.info(f"Staging verification: {verification}")
                return verification
                
        except Exception as e:
            logger.error(f"Staging verification failed: {str(e)}", error=e)
            raise AirflowException(f"Verification failed: {str(e)}")
    
    @task(task_id="print_summary")
    def print_summary(verification: dict) -> str:
        """Print final summary and next steps."""
        backfill_result = verification.get('backfill_result', {})
        staging_stats = verification.get('staging_stats', verification.get('stats', {}))

        summary = f"""
        
        ╔════════════════════════════════════════════════════════╗
        ║      BACKFILL COMPLETED - {verification['status']}                 ║
        ╚════════════════════════════════════════════════════════╝
        
        Backfill Results:
        - Records loaded: {backfill_result.get('total_records', 0)}
        - Stocks processed: {backfill_result.get('processed_stocks', 0)}
        - Failed stocks: {backfill_result.get('failed_stocks', 0)}
        
        Staging Data:
        - Total records: {staging_stats.get('total_records', 0)}
        - Unique stocks: {staging_stats.get('unique_stocks', 0)}
        - Date range: {staging_stats.get('oldest_date')} to {staging_stats.get('newest_date')}
        - Invalid prices: {staging_stats.get('invalid_prices', 0)}
        
        Next Steps:
        1. Review the staging data in the PostgreSQL staging_daily_prices table
        2. Run the main pipeline: nigerian_stock_pipeline_v2
           (or trigger it manually if not scheduled)
        3. The pipeline will reconcile, transform, and load to production
        4. Indicators, alerts, and recommendations will be calculated
        
        ╔════════════════════════════════════════════════════════╗
        """
        logger.info(summary)
        return summary
    
    # Task dependencies
    config = parse_backfill_config()
    result = run_backfill(config)
    verification = verify_staging_data(result)
    summary = print_summary(verification)
    
    config >> result >> verification >> summary


# Instantiate DAG
backfill_dag = backfill_historical_data_dag()
