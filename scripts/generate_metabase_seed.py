#!/usr/bin/env python3
"""
Generate Metabase seed JSON for steady-profile dashboards.
"""

import json


def main() -> None:
    data = {
        "name": "Stock Pipeline - Steady Dashboard",
        "description": "Backtest summary and weekly recommendation snapshots",
        "parameters": [],
        "cards": [
            {
                "name": "Backtest Summary",
                "description": "Key metrics from weekly backtests",
                "collection_position": 1,
                "display": "table",
                "dataset_query": {
                    "type": "native",
                    "native": {
                        "query": (
                            "SELECT run_date, total_trades, win_rate_pct, average_return_pct, "
                            "profit_factor, directional_accuracy_pct, max_drawdown_pct "
                            "FROM backtest_runs ORDER BY run_date DESC"
                        )
                    },
                    "database": None,
                },
            },
            {
                "name": "Win Rate Over Time",
                "description": "Weekly win rate trend",
                "collection_position": 2,
                "display": "line",
                "dataset_query": {
                    "type": "native",
                    "native": {
                        "query": (
                            "SELECT run_date, win_rate_pct FROM backtest_runs "
                            "ORDER BY run_date"
                        )
                    },
                    "database": None,
                },
            },
            {
                "name": "Average Return Over Time",
                "description": "Weekly average return trend",
                "collection_position": 3,
                "display": "line",
                "dataset_query": {
                    "type": "native",
                    "native": {
                        "query": (
                            "SELECT run_date, average_return_pct FROM backtest_runs "
                            "ORDER BY run_date"
                        )
                    },
                    "database": None,
                },
            },
            {
                "name": "Top Recommendations (Latest)",
                "description": "Latest steady picks",
                "collection_position": 4,
                "display": "table",
                "dataset_query": {
                    "type": "native",
                    "native": {
                        "query": (
                            "SELECT snapshot_date, stock_code, company_name, signal_type, "
                            "confidence, score, current_price, target_price, stop_loss "
                            "FROM recommendation_snapshots "
                            "WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM recommendation_snapshots) "
                            "ORDER BY score DESC"
                        )
                    },
                    "database": None,
                },
            },
            {
                "name": "Daily Recommendations (Latest)",
                "description": "Daily steady snapshot",
                "collection_position": 5,
                "display": "table",
                "dataset_query": {
                    "type": "native",
                    "native": {
                        "query": (
                            "SELECT recommendation_date, stock_code, company_name, action_type, "
                            "technical_signal_type, signal_agreement_pct, heuristic_score, "
                            "predicted_probability_10d_up_pct, current_price, policy_target_price, "
                            "policy_stop_loss, policy_upside_pct, risk_reward_ratio "
                            "FROM vw_daily_recommendation_board "
                            "ORDER BY recommendation_rank"
                        )
                    },
                    "database": None,
                },
            },
            {
                "name": "Decision Signal (Latest)",
                "description": "Weekly decision flag",
                "collection_position": 6,
                "display": "table",
                "dataset_query": {
                    "type": "native",
                    "native": {
                        "query": (
                            "SELECT run_date, status, win_rate_pct, average_return_pct, "
                            "profit_factor, max_drawdown_pct, lookback_runs "
                            "FROM decision_signals ORDER BY run_date DESC"
                        )
                    },
                    "database": None,
                },
            },
        ],
        "notes": [
            "After importing, update each card's database ID in Metabase to point at the stock_pipeline database."
        ],
    }

    with open("metabase_seed_dashboard.json", "w", encoding="utf-8") as handle:
        json.dump(data, handle, indent=2)


if __name__ == "__main__":
    main()
