"""Daily steady-profile recommendation snapshot."""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator


default_args = {
    "owner": "stock_pipeline",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="daily_steady_snapshot",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule_interval="0 17 * * 1-5",  # Weekdays 17:00 Africa/Lagos
    catchup=False,
    tags=["recommendations", "steady", "daily"],
) as dag:
    run_snapshot = BashOperator(
        task_id="run_daily_snapshot",
        bash_command="python /Stock_pipeline/scripts/daily_recommendation_snapshot.py",
    )

    run_snapshot
