"""Weekly validation and recommendation board for the steady profile."""

import os
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator

LOCAL_TZ = pendulum.timezone("Africa/Lagos")

# Thresholds read from env so they stay consistent with the live pipeline.
# PIPELINE_MIN_SCORE is also used by the live recommendation run — the backtest
# must evaluate the same bar to produce comparable win-rate / profit-factor
# metrics.  Default 68 matches the live recommendation default.
_MIN_SCORE = os.getenv("PIPELINE_MIN_SCORE", "68")
_MIN_CONFIDENCE = os.getenv("PIPELINE_MIN_CONFIDENCE", "0.70")
_BACKTEST_LOOKBACK_RUNS = os.getenv("BACKTEST_LOOKBACK_RUNS", "4")
_BACKTEST_MIN_TRADES = os.getenv("BACKTEST_MIN_TRADES", "80")
_ACTIVE_PROFILES = os.getenv("ACTIVE_RECOMMENDATION_PROFILES", "steady_20p_10d")
# Use first profile for weekly board (the primary production profile)
_PRIMARY_PROFILE = _ACTIVE_PROFILES.split(",")[0].strip()

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
            f"python -m app.services.reports.weekly_backtest "
            f"--min-score {_MIN_SCORE} "
            f"--min-confidence {_MIN_CONFIDENCE} "
            f"--lookback-runs {_BACKTEST_LOOKBACK_RUNS} "
            f"--min-trades {_BACKTEST_MIN_TRADES}"
        ),
    )

    run_weekly_recommendations = BashOperator(
        task_id="run_weekly_recommendations",
        bash_command=(
            f"python -m app.services.reports.weekly_recommendations "
            f"--strategy-profile {_PRIMARY_PROFILE} "
            f"--disable-probability "
            f"--top-n 15 "
            f"--min-score {_MIN_SCORE}"
        ),
    )

    run_backtest >> run_weekly_recommendations
