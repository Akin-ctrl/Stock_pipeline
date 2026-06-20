"""
Nigerian Stock Exchange ETL Pipeline DAG V2 (Afrimarket + Staging)

This DAG orchestrates the weekday extraction, transformation, and loading of Nigerian
stock market data from Afrimarket with staging and reconciliation.
Runs Monday-Friday at 5PM WAT (UTC+1) to capture the day's trading data.

Pipeline Stages (Staging Workflow):
1. Fetch data from Afrimarket
2. Load stocks and raw data to staging tables
3. Reconcile prices in staging (variance analysis)
4. Pull reconciled data from staging
5. Transform and standardize data
6. Load prices to PostgreSQL production tables
7. Calculate technical indicators (MA, RSI, MACD, Bollinger, Volatility)
8. Evaluate alert rules and generate notifications
9. Generate investment recommendations

Schedule: Weekdays at 5:00 PM WAT (16:00 UTC)
Retries: 3 attempts with 5-minute delays
SLA: 30 minutes total pipeline execution time
"""

from datetime import datetime, timedelta, date
import os

import pendulum
from airflow.decorators import dag
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.timetables.trigger import CronTriggerTimetable
from airflow.models.baseoperator import chain
from sqlalchemy import text

# PYTHONPATH=/Stock_pipeline is set in docker-compose.yml
import logging
logger = logging.getLogger("airflow.task")
LOCAL_TZ = pendulum.timezone("Africa/Lagos")

# Default arguments for all tasks
DEFAULT_ARGS = {
    "owner": "Emmanuel Akingbade",
    "depends_on_past": False,
    "email": [os.getenv("AIRFLOW_ALERT_EMAIL", "")],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=90),
}


def _get_execution_date(context) -> date:
    data_interval_end = context.get("data_interval_end")
    from datetime import datetime as dt
    if data_interval_end:
        return data_interval_end.date()
    execution_date_str = context.get("ds")
    return dt.strptime(execution_date_str, "%Y-%m-%d").date()


def _build_config():
    # Afrimarket-only with staging
    from app.config.settings import get_settings
    from app.pipelines.orchestrator import PipelineConfig
    settings = get_settings()
    return PipelineConfig(
        fetch_afrimarket=True,
        use_staging=True,
        validate_data=True,
        load_stocks=True,
        calculate_indicators=True,
        evaluate_alerts=True,
        generate_recommendations=True,
        recommendation_profiles=settings.active_recommendation_profiles,
        batch_size=settings.batch_size,
        max_errors=10,
        lookback_days=settings.lookback_days
    )


def _get_latest_fact_price_date() -> date | None:
    """Return the latest promoted market date, if any."""
    from app.config.database import get_db
    db = get_db()
    with db.get_session() as session:
        latest_date = session.execute(
            text("SELECT MAX(price_date) FROM fact_daily_prices")
        ).scalar()
    return latest_date


def _get_execution_day_market_metrics(execution_date: date) -> dict:
    """Get same-day production metrics used for the summary email."""
    from app.config.database import get_db
    db = get_db()
    with db.get_session() as session:
        total_prices = session.execute(
            text(
                "SELECT COUNT(*) FROM fact_daily_prices WHERE price_date = :execution_date"
            ),
            {"execution_date": execution_date},
        ).scalar() or 0

        complete_prices = session.execute(
            text(
                "SELECT COUNT(*) "
                "FROM fact_daily_prices "
                "WHERE price_date = :execution_date AND has_complete_data = true"
            ),
            {"execution_date": execution_date},
        ).scalar() or 0

        indicator_stocks = session.execute(
            text(
                "SELECT COUNT(DISTINCT stock_id) "
                "FROM fact_technical_indicators "
                "WHERE calculation_date = :execution_date"
            ),
            {"execution_date": execution_date},
        ).scalar() or 0

    return {
        "total_prices": int(total_prices),
        "complete_prices": int(complete_prices),
        "indicator_stocks": int(indicator_stocks),
    }


def _get_staging_backlog_metrics() -> dict:
    """Return simple backlog counts so the DAG can distinguish outage vs backlog processing."""
    from app.config.database import get_db
    db = get_db()
    with db.get_session() as session:
        unreconciled_count = session.execute(
            text("SELECT COUNT(*) FROM staging_daily_prices WHERE reconciled = false")
        ).scalar() or 0

        pending_fact_count = session.execute(
            text(
                """
                SELECT COUNT(*)
                FROM staging_daily_prices s
                JOIN dim_stocks ds ON ds.stock_code = s.stock_code
                LEFT JOIN fact_daily_prices f
                    ON f.stock_id = ds.stock_id
                   AND f.price_date = s.price_date
                WHERE s.reconciled = true
                  AND f.price_id IS NULL
                """
            )
        ).scalar() or 0

    return {
        "unreconciled_count": int(unreconciled_count),
        "pending_fact_count": int(pending_fact_count),
    }


def fetch_and_reconcile_v2(**context) -> dict:
    from app.pipelines.orchestrator import PipelineOrchestrator
    execution_date = _get_execution_date(context)
    run_id = context.get("run_id")
    # start_ts is only used for SLA measurement — capture it now to time the
    # full task duration. reconciliation_ts (set after reconciliation completes)
    # is the boundary used for promoted_after filtering in transform_and_alert_v2.
    start_ts = datetime.now().timestamp()
    logger.info(f"Stage 1-3: Fetch + Stage + Reconcile for {execution_date} (run_id={run_id})")

    config = _build_config()
    orchestrator = PipelineOrchestrator(config=config)

    raw_data = orchestrator.fetch_data(execution_date, None)
    if raw_data.empty and orchestrator.errors:
        backlog_metrics = _get_staging_backlog_metrics()
        if (
            backlog_metrics["unreconciled_count"] == 0
            and backlog_metrics["pending_fact_count"] == 0
        ):
            raise Exception(
                "No data fetched from any source and no staging backlog is available. "
                f"Fetch errors: {orchestrator.errors}"
            )

        logger.warning(
            "Source fetch failed, but staging backlog exists; continuing so pending "
            "reconciliation/promotion work can still be processed"
        )

    stocks_processed = 0
    if config.load_stocks and not raw_data.empty:
        stocks_processed = orchestrator.load_stocks(raw_data)

    staging_loaded = 0
    if not raw_data.empty:
        staging_loaded = orchestrator.load_to_staging(raw_data, execution_date)
        # Fail the task if staging load failed
        if staging_loaded == 0 and len(raw_data) > 0:
            sources = raw_data['source'].unique() if 'source' in raw_data.columns else []
            existing = sum(
                orchestrator.get_staging_count(execution_date, str(source))
                for source in sources
            )
            if existing > 0:
                logger.warning(
                    f"Staging already has {existing} records for {execution_date}; "
                    "treating as success"
                )
                staging_loaded = existing
            else:
                raise Exception(f"Failed to load staging data. Errors: {orchestrator.errors}")

    unreconciled_dates = orchestrator.get_unreconciled_dates()

    reconciliation_ts = None
    if unreconciled_dates:
        orchestrator.reconcile_all_staging(unreconciled_dates)
        # Use the DB-server timestamp recorded by staging_manager after the commit
        # rather than a Python wall-clock value captured before reconciliation started.
        rws = orchestrator.staging_manager.reconciliation_window_start
        if rws is not None:
            reconciliation_ts = rws.timestamp() if hasattr(rws, "timestamp") else float(rws)

    return {
        "execution_date": execution_date.isoformat(),
        "raw_records": len(raw_data) if not raw_data.empty else 0,
        "stocks_processed": stocks_processed,
        "staging_loaded": staging_loaded,
        "start_ts": start_ts,
        "reconciliation_ts": reconciliation_ts,
        "reconciled_count": orchestrator.reconciled_count,
        "reconciled_dates": [d.isoformat() for d in unreconciled_dates]
    }


def transform_and_alert_v2(**context) -> dict:
    from app.pipelines.orchestrator import PipelineOrchestrator
    execution_date = _get_execution_date(context)
    logger.info(f"Stage 4-8: Pull, Transform, Load, Calculate Indicators, Evaluate Alerts for {execution_date}")

    config = _build_config()
    orchestrator = PipelineOrchestrator(config=config)
    ti = context["ti"]
    fetch_metrics = ti.xcom_pull(task_ids="fetch_and_reconcile_v2") or {}
    # Use the DB-server reconciliation timestamp when available; fall back to
    # the task start_ts only when no reconciliation was performed this run.
    reconciliation_ts = fetch_metrics.get("reconciliation_ts") or fetch_metrics.get("start_ts")
    promoted_after_dt = datetime.fromtimestamp(reconciliation_ts) if reconciliation_ts else None
    staged_records = int(fetch_metrics.get("staging_loaded", 0) or 0)
    reconciled_count = int(fetch_metrics.get("reconciled_count", 0) or 0)

    reconciled_data = orchestrator.get_fact_sync_data(
        promoted_after=promoted_after_dt,
    )
    
    skipped_no_new_data = False
    if reconciled_data.empty:
        latest_price_date = _get_latest_fact_price_date()
        if staged_records == 0 and reconciled_count == 0:
            logger.info(
                "No staged or reconciled rows available for promotion; "
                "treating transform/load as a clean no-op"
            )
            return {
                "reconciled_records": 0,
                "transformed_records": 0,
                "prices_loaded": 0,
                "latest_price_date": latest_price_date.isoformat() if latest_price_date else None,
                "skipped_no_new_data": True,
                "indicators_calculated": 0,
                "alerts_generated": 0,
            }

        raise Exception(
            "No reconciled data available from staging after fetch/reconcile activity. "
            "Check promotion handoff and staging state."
        )

    transformed_data = reconciled_data
    if not reconciled_data.empty:
        orchestrator.logger.info("Skipping validation for staging workflow (stocks already validated)")
        transformed_data = orchestrator.transform_data(reconciled_data)

    prices_loaded = 0
    if not transformed_data.empty:
        prices_loaded = orchestrator.load_prices(transformed_data)
    
    # Fail if transformation produced empty data from non-empty reconciled data
    if transformed_data.empty and not reconciled_data.empty:
        raise Exception("Data transformation failed: produced empty dataset from valid reconciled data")

    market_date = max(reconciled_data["price_date"]) if not reconciled_data.empty else execution_date

    calculated = 0
    if config.calculate_indicators:
        calculated = orchestrator.calculate_indicators(market_date, None)

    alerts = 0
    if config.evaluate_alerts:
        alerts = orchestrator.evaluate_alerts(market_date)

    return {
        "reconciled_records": len(reconciled_data),
        "transformed_records": len(transformed_data),
        "prices_loaded": prices_loaded,
        "latest_price_date": market_date.isoformat(),
        "skipped_no_new_data": False,
        "indicators_calculated": calculated,
        "alerts_generated": alerts,
    }


def generate_recommendations_v2(**context) -> dict:
    from app.pipelines.orchestrator import PipelineOrchestrator
    execution_date = _get_execution_date(context)
    ti = context["ti"]
    load_metrics = ti.xcom_pull(task_ids="transform_and_alert_v2") or {}
    market_date = date.fromisoformat(load_metrics["latest_price_date"]) if load_metrics.get("latest_price_date") else execution_date
    logger.info(f"Stage 9: Generate recommendations for {market_date}")

    config = _build_config()
    orchestrator = PipelineOrchestrator(config=config)
    recs = 0
    if load_metrics.get("skipped_no_new_data"):
        logger.info("Skipping recommendation generation because no new market data was promoted")
    elif config.generate_recommendations:
        recs = orchestrator.generate_recommendations(market_date, None)

    return {
        "recommendations_generated": recs,
        "market_date": market_date.isoformat(),
    }


def check_pipeline_sla_v2(**context) -> dict:
    ti = context["ti"]
    fetch_metrics = ti.xcom_pull(task_ids="fetch_and_reconcile_v2") or {}
    start_ts = fetch_metrics.get("start_ts")
    
    if not start_ts:
        logger.warning("Pipeline V2 SLA check skipped: missing start timestamp")
        return {
            "sla_checked": False,
            "reason": "missing_start_ts"
        }

    duration = datetime.now().timestamp() - start_ts
    sla_minutes = 30
    sla_seconds = sla_minutes * 60
    exceeded = duration > sla_seconds

    if exceeded:
        logger.warning(
            f"Pipeline V2 exceeded SLA! "
            f"Duration: {duration:.2f}s ({duration/60:.2f} min) "
            f"SLA: {sla_minutes} min"
        )
    else:
        logger.info(f"Pipeline V2 completed within SLA: {duration:.2f}s")

    return {
        "sla_checked": True,
        "duration_seconds": duration,
        "duration_minutes": duration / 60,
        "sla_minutes": sla_minutes,
        "exceeded": exceeded
    }


def generate_daily_summary_v2(**context) -> str:
    ti = context["ti"]
    fetch_metrics = ti.xcom_pull(task_ids="fetch_and_reconcile_v2") or {}
    load_metrics = ti.xcom_pull(task_ids="transform_and_alert_v2") or {}
    rec_metrics = ti.xcom_pull(task_ids="generate_recommendations_v2") or {}
    
    if not fetch_metrics:
        return "Pipeline V2 metrics not available."
    
    execution_date = fetch_metrics.get("execution_date")
    prices_loaded = load_metrics.get("prices_loaded", 0)
    stocks_processed = fetch_metrics.get("stocks_processed", 0)
    indicators_calculated = load_metrics.get("indicators_calculated", 0)
    staged_records = fetch_metrics.get("staging_loaded", 0)
    reconciled_records = load_metrics.get("reconciled_records", 0)
    market_date_str = (
        load_metrics.get("latest_price_date")
        or rec_metrics.get("market_date")
        or execution_date
    )
    market_date_obj = date.fromisoformat(market_date_str)
    market_metrics = _get_execution_day_market_metrics(market_date_obj)

    completeness_pct = (
        (market_metrics["complete_prices"] / market_metrics["total_prices"]) * 100
        if market_metrics["total_prices"] > 0 else 0.0
    )
    indicator_coverage_pct = (
        (market_metrics["indicator_stocks"] / market_metrics["total_prices"]) * 100
        if market_metrics["total_prices"] > 0 else 0.0
    )
    reconciliation_rate_pct = (
        (reconciled_records / staged_records) * 100
        if staged_records > 0 else 0.0
    )
    
    summary = f"""
    Nigerian Stock Pipeline V2 Daily Summary (Afrimarket + Staging)
    ═══════════════════════════════════════════════════════
    
    Execution Date: {execution_date}
    Market Date: {market_date_str}
    Status:  SUCCESS
    
     Processing Metrics:
    ─────────────────────
    • Stocks Processed: {stocks_processed}
    • Prices Loaded: {prices_loaded}
    • Indicators Calculated: {indicators_calculated}
    • Alerts Generated: {load_metrics.get('alerts_generated', 0)}
    
     Staging & Reconciliation:
    ────────────────────────────
    • Records Staged: {staged_records}
    • Records Reconciled: {reconciled_records}
    
     Recommendations:
    ────────────────────────────────────
    • Recommendations Generated: {rec_metrics.get('recommendations_generated', 0)}
    
     Data Quality:
    ────────────────
    • Data Completeness: {completeness_pct:.1f}%
    • Indicator Coverage: {indicator_coverage_pct:.1f}%
    • Reconciliation Rate: {reconciliation_rate_pct:.1f}%
    
    ───────────────────────────────────────
    Next run: Next weekday at 5:00 PM WAT
    """
    
    logger.info(summary)
    return summary


@dag(
    dag_id="nigerian_stock_pipeline_v2",
    description="V2: Afrimarket ETL pipeline with staging, reconciliation, alerts and recommendations",
    schedule=CronTriggerTimetable("0 17 * * 1-5", timezone=LOCAL_TZ),
    start_date=pendulum.datetime(2026, 1, 23, tz=LOCAL_TZ),
    catchup=False,
    max_active_runs=1,
    max_active_tasks=4,
    default_args=DEFAULT_ARGS,
    tags=["nigerian_stocks", "etl", "weekday", "v2", "afrimarket", "staging"],
    doc_md=__doc__,
    default_view="graph",
    orientation="LR",
    is_paused_upon_creation=False,
)
def nigerian_stock_pipeline_v2_dag():
    fetch_and_reconcile = PythonOperator(
        task_id="fetch_and_reconcile_v2",
        python_callable=fetch_and_reconcile_v2,
        provide_context=True,
        do_xcom_push=True,
    )

    transform_and_alert = PythonOperator(
        task_id="transform_and_alert_v2",
        python_callable=transform_and_alert_v2,
        provide_context=True,
        do_xcom_push=True,
    )

    generate_recommendations = PythonOperator(
        task_id="generate_recommendations_v2",
        python_callable=generate_recommendations_v2,
        provide_context=True,
        do_xcom_push=True,
    )
    
    check_sla = PythonOperator(
        task_id="check_sla_v2",
        python_callable=check_pipeline_sla_v2,
        provide_context=True,
        do_xcom_push=True,
        trigger_rule="all_success",
    )
    
    generate_summary = PythonOperator(
        task_id="generate_daily_summary_v2",
        python_callable=generate_daily_summary_v2,
        provide_context=True,
        do_xcom_push=True,
        trigger_rule="all_success",
    )

    trigger_daily_snapshot = TriggerDagRunOperator(
        task_id="trigger_daily_steady_snapshot",
        trigger_dag_id="daily_steady_snapshot",
        trigger_run_id="daily_steady_snapshot__{{ dag_run.run_id }}",
        conf={
            "source_dag_id": "{{ dag.dag_id }}",
            "source_run_id": "{{ dag_run.run_id }}",
            "market_date": "{{ ti.xcom_pull(task_ids='transform_and_alert_v2').get('latest_price_date') if ti.xcom_pull(task_ids='transform_and_alert_v2') else data_interval_end.strftime('%Y-%m-%d') }}",
        },
        reset_dag_run=True,
        wait_for_completion=False,
        trigger_rule="all_success",
    )
    
    chain(
        fetch_and_reconcile,
        transform_and_alert,
        generate_recommendations,
        [check_sla, generate_summary],
        trigger_daily_snapshot,
    )


# Instantiate the DAG
nigerian_stock_v2_dag = nigerian_stock_pipeline_v2_dag()

