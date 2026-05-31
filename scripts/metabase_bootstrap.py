#!/usr/bin/env python3
"""
Create a professional Metabase dashboard and cards for the steady model.

Requires:
  METABASE_URL (e.g., http://localhost:3000)
  METABASE_EMAIL
  METABASE_PASSWORD
"""

from __future__ import annotations

import json
import os
import sys
from typing import Any, Dict, List, Optional
from urllib import request, error


def _env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise RuntimeError(f"Missing required env var: {name}")
    return value


def _http(
    method: str,
    url: str,
    payload: Optional[Dict[str, Any]] = None,
    session: Optional[str] = None,
) -> Any:
    data = None
    headers = {"Content-Type": "application/json"}
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
    if session:
        headers["X-Metabase-Session"] = session
    req = request.Request(url, data=data, headers=headers, method=method)
    try:
        with request.urlopen(req, timeout=30) as resp:
            body = resp.read().decode("utf-8")
            return json.loads(body) if body else None
    except error.HTTPError as exc:
        detail = exc.read().decode("utf-8") if exc.fp else str(exc)
        raise RuntimeError(f"Metabase API error {exc.code} for {url}: {detail}") from exc
    except error.URLError as exc:
        raise RuntimeError(f"Metabase request failed for {url}: {exc}") from exc


def login(base_url: str, email: str, password: str) -> str:
    data = _http(
        "POST",
        f"{base_url}/api/session",
        {"username": email, "password": password},
    )
    session = data.get("id") if isinstance(data, dict) else None
    if not session:
        raise RuntimeError("Failed to obtain Metabase session.")
    return session


def get_database_id(base_url: str, session: str, preferred_name: str) -> int:
    databases = _http("GET", f"{base_url}/api/database", session=session)
    if isinstance(databases, dict) and "data" in databases:
        databases = databases["data"]
    for db in databases or []:
        if db.get("name") == preferred_name:
            return int(db["id"])
    # Fallback: first non-sample database
    for db in databases or []:
        if db.get("is_sample") is False:
            return int(db["id"])
    raise RuntimeError("No suitable Metabase database found.")


def get_or_create_collection(base_url: str, session: str, name: str) -> int:
    collections = _http("GET", f"{base_url}/api/collection", session=session)
    if isinstance(collections, dict) and "data" in collections:
        collections = collections["data"]
    for coll in collections or []:
        if coll.get("name") == name:
            return int(coll["id"])
    payload = {"name": name, "color": "#2D6A4F", "description": "Stock pipeline analytics"}
    created = _http("POST", f"{base_url}/api/collection", payload, session=session)
    return int(created["id"])


def get_or_create_dashboard(
    base_url: str,
    session: str,
    collection_id: int,
    name: str,
    description: str,
) -> int:
    dashboards = _http("GET", f"{base_url}/api/dashboard", session=session)
    if isinstance(dashboards, dict) and "data" in dashboards:
        dashboards = dashboards["data"]
    for dash in dashboards or []:
        if dash.get("name") == name:
            return int(dash["id"])
    payload = {
        "name": name,
        "description": description,
        "collection_id": collection_id,
        "parameters": [],
    }
    created = _http("POST", f"{base_url}/api/dashboard", payload, session=session)
    return int(created["id"])


def create_card(
    base_url: str,
    session: str,
    collection_id: int,
    name: str,
    description: str,
    display: str,
    database_id: int,
    query: str,
    visualization_settings: Optional[Dict[str, Any]] = None,
) -> int:
    payload = {
        "name": name,
        "description": description,
        "display": display,
        "collection_id": collection_id,
        "dataset_query": {
            "type": "native",
            "native": {"query": query},
            "database": database_id,
        },
        "visualization_settings": visualization_settings or {},
    }
    created = _http("POST", f"{base_url}/api/card", payload, session=session)
    return int(created["id"])


def add_cards_to_dashboard(
    base_url: str,
    session: str,
    dashboard_id: int,
    cards: List[Dict[str, Any]],
) -> None:
    dashboard = _http("GET", f"{base_url}/api/dashboard/{dashboard_id}", session=session)
    dashcards = []
    for index, card in enumerate(cards, start=1):
        dashcards.append(
            {
                "id": -index,
                "card_id": card["card_id"],
                "row": card["row"],
                "col": card["col"],
                "size_x": card["size_x"],
                "size_y": card["size_y"],
                "parameter_mappings": [],
                "visualization_settings": {},
            }
        )

    payload = {
        "name": dashboard.get("name"),
        "description": dashboard.get("description"),
        "parameters": dashboard.get("parameters", []),
        "dashcards": dashcards,
    }
    _http("PUT", f"{base_url}/api/dashboard/{dashboard_id}", payload, session=session)


def main() -> None:
    base_url = _env("METABASE_URL").rstrip("/")
    email = _env("METABASE_EMAIL")
    password = _env("METABASE_PASSWORD")

    session = login(base_url, email, password)
    print("Logged into Metabase.")
    database_id = get_database_id(base_url, session, preferred_name="stock_pipeline")
    print(f"Using database id {database_id}.")
    collection_id = get_or_create_collection(base_url, session, "Stock Pipeline - Steady")
    print(f"Using collection id {collection_id}.")
    dashboard_id = get_or_create_dashboard(
        base_url,
        session,
        collection_id,
        "Steady Model Command Center",
        "Backtests, decision signals, and recommendation tracking for steady profile.",
    )
    print(f"Using dashboard id {dashboard_id}.")

    cards = []

    cards.append(
        {
            "name": "Decision Signal (Latest)",
            "description": "Latest weekly decision flag",
            "display": "table",
            "query": (
                "SELECT run_date, status, win_rate_pct, average_return_pct, profit_factor, "
                "max_drawdown_pct, lookback_runs "
                "FROM decision_signals ORDER BY run_date DESC"
            ),
            "size_x": 12,
            "size_y": 5,
            "row": 0,
            "col": 0,
        }
    )
    cards.append(
        {
            "name": "Backtest Summary",
            "description": "Key metrics from weekly backtests",
            "display": "table",
            "query": (
                "SELECT run_date, total_trades, win_rate_pct, average_return_pct, profit_factor, "
                "directional_accuracy_pct, max_drawdown_pct "
                "FROM backtest_runs ORDER BY run_date DESC"
            ),
            "size_x": 12,
            "size_y": 5,
            "row": 0,
            "col": 12,
        }
    )
    cards.append(
        {
            "name": "Win Rate Trend",
            "description": "Weekly win rate over time",
            "display": "line",
            "query": "SELECT run_date, win_rate_pct FROM backtest_runs ORDER BY run_date",
            "viz": {"graph.dimensions": ["run_date"], "graph.metrics": ["win_rate_pct"]},
            "size_x": 8,
            "size_y": 6,
            "row": 5,
            "col": 0,
        }
    )
    cards.append(
        {
            "name": "Average Return Trend",
            "description": "Weekly average return over time",
            "display": "line",
            "query": "SELECT run_date, average_return_pct FROM backtest_runs ORDER BY run_date",
            "viz": {"graph.dimensions": ["run_date"], "graph.metrics": ["average_return_pct"]},
            "size_x": 8,
            "size_y": 6,
            "row": 5,
            "col": 8,
        }
    )
    cards.append(
        {
            "name": "Max Drawdown Trend",
            "description": "Weekly max drawdown over time",
            "display": "line",
            "query": "SELECT run_date, max_drawdown_pct FROM backtest_runs ORDER BY run_date",
            "viz": {"graph.dimensions": ["run_date"], "graph.metrics": ["max_drawdown_pct"]},
            "size_x": 8,
            "size_y": 6,
            "row": 5,
            "col": 16,
        }
    )
    cards.append(
        {
            "name": "Latest Weekly Recommendations",
            "description": "Top steady picks from weekly run",
            "display": "table",
            "query": (
                "SELECT snapshot_date, stock_code, company_name, signal_type, confidence, score, "
                "current_price, target_price, stop_loss "
                "FROM recommendation_snapshots "
                "WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM recommendation_snapshots) "
                "ORDER BY score DESC"
            ),
            "size_x": 12,
            "size_y": 6,
            "row": 11,
            "col": 0,
        }
    )
    cards.append(
        {
            "name": "Latest Daily Recommendations",
            "description": "Top daily steady snapshot",
            "display": "table",
            "query": (
                "SELECT recommendation_date, stock_code, company_name, action_type, "
                "technical_signal_type, signal_agreement_pct, heuristic_score, "
                "predicted_probability_10d_up_pct, current_price, policy_target_price, "
                "policy_stop_loss, policy_upside_pct, risk_reward_ratio "
                "FROM vw_daily_recommendation_board "
                "ORDER BY recommendation_rank"
            ),
            "size_x": 12,
            "size_y": 6,
            "row": 11,
            "col": 12,
        }
    )
    cards.append(
        {
            "name": "Trades By Stock (Win Rate)",
            "description": "Backtest win rate and return by stock",
            "display": "table",
            "query": (
                "SELECT stock_code, COUNT(*) AS trades, "
                "ROUND(AVG(net_return_pct)::numeric, 2) AS avg_return_pct, "
                "ROUND(100.0 * SUM(CASE WHEN net_return_pct > 0 THEN 1 ELSE 0 END) / COUNT(*), 2) AS win_rate_pct "
                "FROM backtest_trades "
                "GROUP BY stock_code "
                "ORDER BY avg_return_pct DESC"
            ),
            "size_x": 12,
            "size_y": 7,
            "row": 17,
            "col": 0,
        }
    )
    cards.append(
        {
            "name": "Trades By Signal Type",
            "description": "Win rate and average return by signal",
            "display": "table",
            "query": (
                "SELECT signal_type, COUNT(*) AS trades, "
                "ROUND(AVG(net_return_pct)::numeric, 2) AS avg_return_pct, "
                "ROUND(100.0 * SUM(CASE WHEN net_return_pct > 0 THEN 1 ELSE 0 END) / COUNT(*), 2) AS win_rate_pct "
                "FROM backtest_trades "
                "GROUP BY signal_type "
                "ORDER BY trades DESC"
            ),
            "size_x": 12,
            "size_y": 7,
            "row": 17,
            "col": 12,
        }
    )
    cards.append(
        {
            "name": "Recent Winners / Losers",
            "description": "Top and bottom trades in last 90 days",
            "display": "table",
            "query": (
                "SELECT stock_code, entry_date, exit_date, signal_type, net_return_pct "
                "FROM backtest_trades "
                "WHERE exit_date >= (CURRENT_DATE - INTERVAL '90 days') "
                "ORDER BY net_return_pct DESC"
            ),
            "size_x": 12,
            "size_y": 6,
            "row": 24,
            "col": 0,
        }
    )
    cards.append(
        {
            "name": "Coverage: Active Stocks With Trusted Price Today",
            "description": "How many active stocks have trusted prices for today",
            "display": "table",
            "query": (
                "SELECT COUNT(DISTINCT s.stock_id) AS active_stocks, "
                "COUNT(DISTINCT p.stock_id) AS stocks_with_trusted_price "
                "FROM dim_stocks s "
                "LEFT JOIN fact_daily_prices p "
                "ON p.stock_id = s.stock_id "
                "AND p.price_date = CURRENT_DATE "
                "AND p.bar_status IN ('RECONCILED', 'OFFICIAL') "
                "AND p.data_quality_flag IN ('GOOD', 'INCOMPLETE') "
                "AND (p.confidence_score IS NULL OR p.confidence_score >= 65) "
                "WHERE s.is_active = true"
            ),
            "size_x": 12,
            "size_y": 6,
            "row": 24,
            "col": 12,
        }
    )

    created_cards = []
    for card in cards:
        print(f"Creating card: {card['name']}")
        card_id = create_card(
            base_url=base_url,
            session=session,
            collection_id=collection_id,
            name=card["name"],
            description=card["description"],
            display=card["display"],
            database_id=database_id,
            query=card["query"],
            visualization_settings=card.get("viz"),
        )
        created_cards.append(
            {
                "card_id": card_id,
                "row": card["row"],
                "col": card["col"],
                "size_x": card["size_x"],
                "size_y": card["size_y"],
            }
        )

    print("Attaching cards to dashboard.")
    add_cards_to_dashboard(base_url, session, dashboard_id, created_cards)
    print({"dashboard_id": dashboard_id, "cards": len(created_cards)})


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"Metabase bootstrap failed: {exc}", file=sys.stderr)
        sys.exit(1)
