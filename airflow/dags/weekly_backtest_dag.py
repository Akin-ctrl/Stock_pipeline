"""Weekly backtest and recommendation snapshot for steady profile."""

from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator

LOCAL_TZ = pendulum.timezone("Africa/Lagos")


default_args = {
    "owner": "stock_pipeline",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}


with DAG(
    dag_id="weekly_steady_backtest",
    default_args=default_args,
    start_date=pendulum.datetime(2025, 1, 1, tz=LOCAL_TZ),
    schedule="0 20 * * 5",  # Fridays 20:00 Africa/Lagos, after the daily chain
    catchup=False,
    max_active_runs=1,
    tags=["backtest", "steady", "weekly"],
) as dag:
    run_backtest = BashOperator(
        task_id="run_weekly_backtest",
        bash_command=(
            "python /Stock_pipeline/scripts/weekly_backtest_report.py "
            "--min-score 60 --min-confidence 0.70 --lookback-runs 4 --min-trades 80"
        ),
    )

    run_backtest
