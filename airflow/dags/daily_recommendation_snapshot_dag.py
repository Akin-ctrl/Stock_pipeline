"""Daily steady-profile recommendation snapshot triggered by the stock pipeline."""

from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator

LOCAL_TZ = pendulum.timezone("Africa/Lagos")


default_args = {
    "owner": "stock_pipeline",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="daily_steady_snapshot",
    default_args=default_args,
    start_date=pendulum.datetime(2025, 1, 1, tz=LOCAL_TZ),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["recommendations", "steady", "daily"],
) as dag:
    run_snapshot = BashOperator(
        task_id="run_daily_snapshot",
        bash_command=(
            "python /Stock_pipeline/scripts/daily_recommendation_snapshot.py "
            "{% if dag_run and dag_run.conf and dag_run.conf.get('market_date') %}"
            "--snapshot-date {{ dag_run.conf.get('market_date') }}"
            "{% endif %}"
        ),
    )

    run_snapshot
