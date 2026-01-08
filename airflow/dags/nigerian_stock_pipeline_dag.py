"""
Nigerian Stock Exchange ETL Pipeline DAG

This DAG orchestrates the daily extraction, transformation, and loading of Nigerian
stock market data from NGX web source. It runs daily at 3PM WAT
(after market close at 2:30 PM WAT) to capture the day's trading data.

Pipeline Stages:
1. Fetch data from NGX web source and Yahoo Finance API
2. Validate data quality (nulls, ranges, OHLC consistency)
3. Transform and standardize data
4. Load stocks and prices to PostgreSQL
5. Calculate technical indicators (MA, RSI, MACD, Bollinger, Volatility)
6. Evaluate alert rules and generate notifications

Schedule: Daily at 3:00 PM WAT (14:00 UTC)
Retries: 3 attempts with 5-minute delays
SLA: 30 minutes total pipeline execution time
"""

from datetime import datetime, timedelta
import sys
import os
from pathlib import Path

import pendulum
from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.models.baseoperator import chain

# PYTHONPATH=/Stock_pipeline is set in docker-compose.yml
from app.pipelines.orchestrator import PipelineOrchestrator, PipelineConfig
from app.utils.logger import get_logger

logger = get_logger(__name__)

# Default arguments for all tasks
DEFAULT_ARGS = {
    "owner": "nigerian_equity_investor",
    "depends_on_past": False,
    "email": ["alerts@stockpipeline.com"], # Replace with actual alert email
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=30),
}


def run_pipeline(**context) -> dict:
    """
    Execute the complete Nigerian stock pipeline.
    
    This function:
    1. Initializes the PipelineOrchestrator with configuration
    2. Runs all 7 pipeline stages
    3. Returns execution metrics for monitoring
    
    Args:
        **context: Airflow context with execution date, run_id, etc.
    
    Returns:
        dict: Pipeline execution metrics including:
            - success: bool
            - execution_time: float (seconds)
            - stocks_processed: int
            - prices_loaded: int
            - indicators_calculated: int
            - alerts_generated: int
            - errors: list
    
    Raises:
        Exception: If pipeline execution fails critically
    """
    execution_date_str = context.get("ds")  # YYYY-MM-DD format
    run_id = context.get("run_id")
    
    logger.info(f"Starting Nigerian Stock Pipeline for {execution_date_str}")
    logger.info(f"Run ID: {run_id}")
    
    # Parse execution date
    from datetime import datetime as dt
    execution_date = dt.strptime(execution_date_str, "%Y-%m-%d").date()
    
    # Configure pipeline for full execution
    config = PipelineConfig(
        fetch_ngx=True,
        validate_data=True,
        load_stocks=True,
        load_prices=True,
        calculate_indicators=True,
        evaluate_alerts=True,
        generate_recommendations=True,
        batch_size=50,
        max_errors=10,
        lookback_days=30
    )
    
    # Initialize and run orchestrator with execution_date
    orchestrator = PipelineOrchestrator(config=config)
    result = orchestrator.run(execution_date=execution_date)
    
    # Log results
    logger.info(f"Pipeline completed: {result.success}")
    logger.info(f"Total duration: {result.execution_time:.2f}s")
    logger.info(f"Stocks processed: {result.stocks_processed}")
    logger.info(f"Prices loaded: {result.prices_loaded}")
    logger.info(f"Indicators calculated: {result.indicators_calculated}")
    logger.info(f"Alerts generated: {result.alerts_generated}")
    logger.info(f"Recommendations generated: {result.recommendations_generated}")
    
    if result.errors:
        logger.warning(f"Errors encountered: {len(result.errors)}")
        for error in result.errors[:5]:  # Log first 5 errors
            logger.error(error)
    
    if result.warnings:
        logger.warning(f"Warnings: {len(result.warnings)}")
    
    # Push metrics to XCom for downstream tasks
    context["ti"].xcom_push(key="pipeline_metrics", value={
        "success": result.success,
        "execution_time": result.execution_time,
        "stocks_processed": result.stocks_processed,
        "prices_loaded": result.prices_loaded,
        "indicators_calculated": result.indicators_calculated,
        "alerts_generated": result.alerts_generated,
        "recommendations_generated": result.recommendations_generated,
        "errors_count": len(result.errors),
        "warnings_count": len(result.warnings),
        "execution_date": execution_date,
    })
    
    # Raise exception if pipeline failed
    if not result.success:
        raise Exception(
            f"Pipeline failed with {len(result.errors)} errors. "
            f"Check logs for details."
        )
    
    return {
        "status": "success",
        "message": f"Processed {result.stocks_processed} stocks, "
                   f"loaded {result.prices_loaded} prices, "
                   f"calculated {result.indicators_calculated} indicators, "
                   f"generated {result.alerts_generated} alerts, "
                   f"created {result.recommendations_generated} recommendations",
    }


def check_pipeline_sla(**context) -> None:
    """
    Check if pipeline execution exceeded SLA and log warning.
    
    SLA: 30 minutes total execution time
    """
    ti = context["ti"]
    metrics = ti.xcom_pull(key="pipeline_metrics")
    
    if metrics:
        duration = metrics.get("execution_time", 0)
        sla_minutes = 30
        sla_seconds = sla_minutes * 60
        
        if duration > sla_seconds:
            logger.warning(
                f"Pipeline exceeded SLA! "
                f"Duration: {duration:.2f}s ({duration/60:.2f} min) "
                f"SLA: {sla_minutes} min"
            )
        else:
            logger.info(f"Pipeline completed within SLA: {duration:.2f}s")


def generate_daily_summary(**context) -> str:
    """
    Generate a human-readable summary of pipeline execution.
    
    Returns:
        str: Formatted summary for email notifications
    """
    ti = context["ti"]
    metrics = ti.xcom_pull(key="pipeline_metrics")
    
    if not metrics:
        return "Pipeline metrics not available."
    
    summary = f"""
    Nigerian Stock Pipeline Daily Summary
    ═══════════════════════════════════════
    
    Execution Date: {metrics['execution_date']}
    Status: {'✅ SUCCESS' if metrics['success'] else '❌ FAILED'}
    Duration: {metrics['execution_time']:.2f} seconds ({metrics['execution_time']/60:.2f} minutes)
    
    📊 Processing Metrics:
    ─────────────────────
    • Stocks Processed: {metrics['stocks_processed']}
    • Prices Loaded: {metrics['prices_loaded']}
    • Indicators Calculated: {metrics['indicators_calculated']}
    • Alerts Generated: {metrics['alerts_generated']}
    
    🔔 Recommendations:
    ────────────────────────────────────
    • Recommendations Generated: {metrics.get('recommendations_generated', 0)}
    
    ⚠️ Issues:
    ──────────
    • Errors: {metrics['errors_count']}
    • Warnings: {metrics['warnings_count']}
    
    🎯 Data Quality:
    ────────────────
    • Data Completeness: {(metrics['prices_loaded'] / max(metrics['stocks_processed'], 1) * 100):.1f}%
    • Indicator Coverage: {(metrics['indicators_calculated'] / max(metrics['prices_loaded'], 1) * 100):.1f}%
    
    ───────────────────────────────────────
    Next run: Tomorrow at 3:00 PM WAT
    """
    
    logger.info(summary)
    return summary


@dag(
    dag_id="nigerian_stock_pipeline",
    description="Daily ETL pipeline for Nigerian Stock Exchange data with alerts and investment recommendations",
    schedule="0 14 * * *",  # 2PM UTC = 3PM WAT (West Africa Time is UTC+1)
    start_date=pendulum.datetime(2026, 1, 5, tz="UTC"),
    catchup=True,  # Run missed schedules automatically
    max_active_runs=1,  # Only one pipeline run at a time
    max_active_tasks=10,  # Allow parallel task execution
    default_args=DEFAULT_ARGS,
    tags=["nigerian_stocks", "etl", "daily", "production"],
    doc_md=__doc__,
    default_view="graph",
    orientation="LR",  # Left-to-right graph layout
    is_paused_upon_creation=False,  # Start unpaused automatically
)
def nigerian_stock_pipeline_dag():
    """
    Daily Nigerian Stock Exchange ETL Pipeline
    
    Orchestrates data ingestion from NGX, validates data quality,
    transforms and loads to PostgreSQL, calculates technical indicators, and
    evaluates alert conditions for 154 Nigerian stocks.
    """
    
    # Task 1: Run the complete pipeline
    run_etl_pipeline = PythonOperator(
        task_id="run_etl_pipeline",
        python_callable=run_pipeline,
        provide_context=True,
        doc_md="""
        ### Run ETL Pipeline
        
        Executes all stages of the Nigerian stock pipeline:
        1. **Fetch**: NGX web scraping from african-markets.com
        2. **Validate**: Data quality checks (nulls, ranges, OHLC)
        3. **Transform**: Standardization and cleaning
        4. **Load Stocks**: Insert/update stock master data
        5. **Load Prices**: Batch insert OHLCV data
        6. **Calculate Indicators**: MA(20/50), RSI, MACD, Bollinger, Volatility
        7. **Evaluate Alerts**: Check 5 rule types + send Email/Slack notifications
        8. **Generate Recommendations**: BUY/SELL/HOLD signals with scoring and targets
        
        **SLA**: MAX 30 minutes  
        **Retries**: 3 attempts with 5-minute delays  
        **Output**: Pipeline metrics pushed to XCom
        """,
    )
    
    # Task 2: Check SLA compliance
    check_sla = PythonOperator(
        task_id="check_sla",
        python_callable=check_pipeline_sla,
        provide_context=True,
        trigger_rule="all_success",  # Only run if pipeline succeeds
        doc_md="""
        ### Check SLA Compliance
        
        Validates that pipeline execution completed within the 30-minute SLA.
        Logs warnings if SLA is exceeded for monitoring and optimization.
        
        **Trigger**: Only runs if pipeline succeeds
        """,
    )
    
    # Task 3: Generate daily summary
    generate_summary = PythonOperator(
        task_id="generate_daily_summary",
        python_callable=generate_daily_summary,
        provide_context=True,
        trigger_rule="all_success",  # Only run if pipeline succeeds
        doc_md="""
        ### Generate Daily Summary
        
        Creates a human-readable summary of pipeline execution including:
        - Processing metrics (stocks, prices, indicators, alerts)
        - Error and warning counts
        - Data quality percentages
        - Duration and SLA compliance
        
        **Trigger**: Only runs if pipeline succeeds
        **Output**: Formatted summary for email notifications
        """,
    )
    
    # Define task dependencies using chain for linear flow
    chain(
        run_etl_pipeline,
        [check_sla, generate_summary],  # These run in parallel after pipeline
    )


# Instantiate the DAG
nigerian_stock_dag = nigerian_stock_pipeline_dag()
