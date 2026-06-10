"""Bootstrap Superset metadata for the stock pipeline dashboards."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import quote_plus

from superset.app import create_app


DATABASE_NAME = "Stock Pipeline Analytics"
FINANCE_COLOR_SCHEME = "supersetColors"


@dataclass(frozen=True)
class DatasetConfig:
    view_name: str
    label: str
    main_dttm_col: str | None = None
    description: str = ""


@dataclass(frozen=True)
class ChartConfig:
    name: str
    dataset: str
    viz_type: str
    params: dict[str, Any]
    description: str = ""
    width: int = 4
    height: int = 28
    legacy_names: tuple[str, ...] = ()


@dataclass(frozen=True)
class DashboardConfig:
    title: str
    slug: str
    description: str
    chart_names: tuple[str, ...]
    legacy_titles: tuple[str, ...] = ()
    row_message: str = ""


DATASETS = [
    DatasetConfig(
        "vw_dashboard_command_center",
        "Command Center",
        "market_date",
        "Top-level dashboard readiness, freshness, model status, and risk posture.",
    ),
    DatasetConfig(
        "vw_daily_recommendation_board",
        "Daily Recommendation Board",
        "recommendation_date",
        "Current approved and rejected recommendation candidates.",
    ),
    DatasetConfig(
        "vw_weekly_recommendation_board",
        "Weekly Recommendation Board",
        "week_end_date",
        "Weekly candidate board for watchlist and setup-monitoring decisions.",
    ),
    DatasetConfig(
        "vw_recommendation_board",
        "Recommendation History",
        "recommendation_date",
        "Historical recommendation decisions and model rationale.",
    ),
    DatasetConfig(
        "vw_market_overview",
        "Market Overview",
        "market_date",
        "Market breadth, liquidity, and index-like summary indicators.",
    ),
    DatasetConfig(
        "vw_sector_performance",
        "Sector Performance",
        "market_date",
        "Sector-level price and performance analytics.",
    ),
    DatasetConfig(
        "vw_model_health",
        "Model Health",
        "run_date",
        "Persisted validation and portfolio-level model metrics.",
    ),
    DatasetConfig(
        "vw_latest_model_verdict",
        "Latest Model Verdict",
        "run_date",
        "Latest validation verdict for executive dashboard status.",
    ),
    DatasetConfig(
        "vw_model_yearly_performance",
        "Model Yearly Performance",
        None,
        "Year-by-year validation performance for drift review.",
    ),
    DatasetConfig(
        "vw_backtest_equity_curve",
        "Backtest Equity Curve",
        "trade_date",
        "Backtest equity and performance curve for the selected run.",
    ),
    DatasetConfig(
        "vw_portfolio_equity_curve",
        "Portfolio Equity Curve",
        "event_date",
        "Portfolio-level equity curve persisted from validation runs.",
    ),
    DatasetConfig(
        "vw_portfolio_drawdown_curve",
        "Portfolio Drawdown Curve",
        "event_date",
        "Portfolio drawdown curve persisted from validation runs.",
    ),
    DatasetConfig(
        "vw_trade_distribution",
        "Trade Distribution",
        None,
        "Trade outcome distribution for model-quality review.",
    ),
    DatasetConfig(
        "vw_sector_model_performance",
        "Sector Model Performance",
        None,
        "Model performance grouped by sector.",
    ),
    DatasetConfig(
        "vw_stock_model_performance",
        "Stock Model Performance",
        None,
        "Model performance grouped by stock.",
    ),
    DatasetConfig(
        "vw_stock_price_panel",
        "Stock Price Panel",
        "price_date",
        "Stock drillthrough price and indicator panel.",
    ),
    DatasetConfig(
        "vw_data_quality_monitor",
        "Data Quality Monitor",
        "market_date",
        "Freshness, coverage, and pipeline-readiness checks.",
    ),
]


def _env(name: str, default: str) -> str:
    value = os.getenv(name)
    return value if value not in (None, "") else default


def _database_uri() -> str:
    user = quote_plus(_env("POSTGRES_USER", "stock_user"))
    password = quote_plus(_env("POSTGRES_PASSWORD", "stock_password"))
    host = _env("POSTGRES_HOST", "postgres")
    port = _env("POSTGRES_PORT", "5432")
    name = _env("POSTGRES_DB", "stock_pipeline")
    return f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{name}"


def _simple_metric(column: str, aggregate: str = "SUM", label: str | None = None) -> dict[str, Any]:
    return {
        "expressionType": "SIMPLE",
        "column": {"column_name": column},
        "aggregate": aggregate,
        "label": label or column.replace("_", " ").title(),
        "optionName": f"metric_{aggregate.lower()}_{column}",
    }


def _sql_metric(expression: str, label: str) -> dict[str, Any]:
    return {
        "expressionType": "SQL",
        "sqlExpression": expression,
        "label": label,
        "optionName": f"metric_sql_{label.lower().replace(' ', '_').replace('%', 'pct')}",
    }


def _no_time_filter(column: str) -> list[dict[str, Any]]:
    return [
        {
            "clause": "WHERE",
            "subject": column,
            "operator": "TEMPORAL_RANGE",
            "comparator": "No filter",
            "expressionType": "SIMPLE",
        }
    ]


def _hex_color_picker(hex_color: str) -> dict[str, float | int]:
    color = hex_color.lstrip("#")
    return {
        "r": int(color[0:2], 16),
        "g": int(color[2:4], 16),
        "b": int(color[4:6], 16),
        "a": 1,
    }


def _big_number_params(
    dataset_ref: str,
    metric: dict[str, Any],
    date_col: str | None = None,
    y_axis_format: str = "SMART_NUMBER",
    accent: str = "#67e8f9",
) -> dict[str, Any]:
    params: dict[str, Any] = {
        "datasource": dataset_ref,
        "viz_type": "big_number_total",
        "metric": metric,
        "color_picker": _hex_color_picker(accent),
        "header_font_size": 0.32,
        "subheader_font_size": 0.13,
        "y_axis_format": y_axis_format,
        "time_format": "smart_date",
        "extra_form_data": {},
    }
    if date_col:
        params["adhoc_filters"] = _no_time_filter(date_col)
    return params


def _table_params(
    dataset_ref: str,
    columns: list[str],
    date_col: str | None = None,
    row_limit: int = 100,
    server_page_length: int = 20,
) -> dict[str, Any]:
    params: dict[str, Any] = {
        "datasource": dataset_ref,
        "viz_type": "table",
        "query_mode": "raw",
        "groupby": [],
        "all_columns": columns,
        "percent_metrics": [],
        "order_by_cols": [],
        "row_limit": row_limit,
        "server_page_length": server_page_length,
        "order_desc": True,
        "table_timestamp_format": "smart_date",
        "show_cell_bars": True,
        "color_pn": True,
        "allow_render_html": True,
        "extra_form_data": {},
    }
    if date_col:
        params["time_grain_sqla"] = "P1D"
        params["adhoc_filters"] = _no_time_filter(date_col)
    return params


def _single_value_params(
    dataset_ref: str,
    column: str,
    date_col: str | None = None,
) -> dict[str, Any]:
    return _table_params(
        dataset_ref,
        [column],
        date_col=date_col,
        row_limit=1,
        server_page_length=1,
    )


def _line_params(
    dataset_ref: str,
    x_axis: str,
    metrics: list[dict[str, Any]],
    groupby: list[str] | None = None,
    row_limit: int = 10000,
    y_axis_format: str = "SMART_NUMBER",
) -> dict[str, Any]:
    return {
        "datasource": dataset_ref,
        "viz_type": "echarts_timeseries_line",
        "x_axis": x_axis,
        "time_grain_sqla": "P1D",
        "x_axis_sort_asc": True,
        "x_axis_sort_series": "name",
        "x_axis_sort_series_ascending": True,
        "metrics": metrics,
        "groupby": groupby or [],
        "adhoc_filters": _no_time_filter(x_axis),
        "order_desc": True,
        "row_limit": row_limit,
        "truncate_metric": True,
        "show_empty_columns": False,
        "comparison_type": "values",
        "annotation_layers": [],
        "forecastPeriods": 0,
        "forecastInterval": 0.8,
        "x_axis_title_margin": 15,
        "y_axis_title_margin": 15,
        "y_axis_title_position": "Left",
        "sort_series_type": "sum",
        "color_scheme": FINANCE_COLOR_SCHEME,
        "seriesType": "line",
        "only_total": False,
        "markerSize": 4,
        "show_legend": True,
        "legendType": "scroll",
        "legendOrientation": "top",
        "x_axis_time_format": "smart_date",
        "rich_tooltip": True,
        "tooltipTimeFormat": "smart_date",
        "y_axis_format": y_axis_format,
        "truncateXAxis": True,
        "y_axis_bounds": [None, None],
        "extra_form_data": {},
    }


def _bar_params(
    dataset_ref: str,
    x_axis: str,
    metric: dict[str, Any],
    groupby: list[str] | None = None,
    date_col: str | None = None,
    row_limit: int = 25,
    y_axis_format: str = "SMART_NUMBER",
) -> dict[str, Any]:
    params: dict[str, Any] = {
        "datasource": dataset_ref,
        "viz_type": "echarts_timeseries_bar",
        "x_axis": x_axis,
        "metrics": [metric],
        "groupby": groupby or [],
        "order_desc": True,
        "row_limit": row_limit,
        "truncate_metric": True,
        "show_empty_columns": False,
        "comparison_type": "values",
        "annotation_layers": [],
        "orientation": "vertical",
        "x_axis_title_margin": 15,
        "y_axis_title_margin": 15,
        "y_axis_title_position": "Left",
        "sort_series_type": "sum",
        "color_scheme": FINANCE_COLOR_SCHEME,
        "only_total": False,
        "show_legend": True,
        "legendType": "scroll",
        "legendOrientation": "top",
        "x_axis_time_format": "smart_date",
        "y_axis_format": y_axis_format,
        "truncateXAxis": True,
        "y_axis_bounds": [None, None],
        "rich_tooltip": True,
        "tooltipTimeFormat": "smart_date",
        "extra_form_data": {},
    }
    if date_col:
        params["time_grain_sqla"] = "P1D"
        params["adhoc_filters"] = _no_time_filter(date_col)
    return params


def _chart_configs(dataset_refs: dict[str, str]) -> list[ChartConfig]:
    command = dataset_refs["vw_dashboard_command_center"]
    daily = dataset_refs["vw_daily_recommendation_board"]
    weekly = dataset_refs["vw_weekly_recommendation_board"]
    history = dataset_refs["vw_recommendation_board"]
    market = dataset_refs["vw_market_overview"]
    sector = dataset_refs["vw_sector_performance"]
    model = dataset_refs["vw_model_health"]
    verdict = dataset_refs["vw_latest_model_verdict"]
    yearly = dataset_refs["vw_model_yearly_performance"]
    equity = dataset_refs["vw_portfolio_equity_curve"]
    drawdown = dataset_refs["vw_portfolio_drawdown_curve"]
    sector_model = dataset_refs["vw_sector_model_performance"]
    stock_model = dataset_refs["vw_stock_model_performance"]
    trades = dataset_refs["vw_trade_distribution"]
    quality = dataset_refs["vw_data_quality_monitor"]
    prices = dataset_refs["vw_stock_price_panel"]

    return [
        ChartConfig(
            "Decision Status",
            "vw_latest_model_verdict",
            "table",
            _single_value_params(verdict, "decision_status", "run_date"),
            "Latest model status.",
            width=2,
            height=20,
        ),
        ChartConfig(
            "Run Date",
            "vw_latest_model_verdict",
            "table",
            _single_value_params(verdict, "run_date", "run_date"),
            "Latest model validation run date.",
            width=2,
            height=20,
        ),
        ChartConfig(
            "Market Date",
            "vw_dashboard_command_center",
            "table",
            _single_value_params(command, "market_date", "market_date"),
            "Latest available market date.",
            width=2,
            height=20,
        ),
        ChartConfig(
            "Approved Picks",
            "vw_dashboard_command_center",
            "big_number_total",
            _big_number_params(command, _simple_metric("approved_recommendations", "SUM", "Approved Picks"), "market_date", accent="#34d399"),
            "Approved recommendations available for the latest market date.",
            width=2,
            height=20,
            legacy_names=("CC - Approved Picks",),
        ),
        ChartConfig(
            "Rejected Candidates",
            "vw_daily_recommendation_board",
            "big_number_total",
            _big_number_params(
                daily,
                _sql_metric("SUM(CASE WHEN portfolio_approved = false THEN 1 ELSE 0 END)", "Rejected Candidates"),
                "recommendation_date",
                accent="#fb7185",
            ),
            "Candidates rejected by the portfolio gate.",
            width=2,
            height=20,
        ),
        ChartConfig(
            "Average Probability",
            "vw_daily_recommendation_board",
            "big_number_total",
            _big_number_params(daily, _simple_metric("predicted_probability_10d_up_pct", "AVG", "Average Probability"), "recommendation_date", accent="#22d3ee"),
            "Average 10-day up probability across current candidates.",
            width=2,
            height=20,
        ),
        ChartConfig(
            "Average Risk-Reward",
            "vw_daily_recommendation_board",
            "big_number_total",
            _big_number_params(daily, _simple_metric("risk_reward_ratio", "AVG", "Average Risk-Reward"), "recommendation_date", accent="#facc15"),
            "Average risk-reward ratio across current candidates.",
            width=2,
            height=20,
        ),
        ChartConfig(
            "Average Upside",
            "vw_daily_recommendation_board",
            "big_number_total",
            _big_number_params(daily, _simple_metric("policy_upside_pct", "AVG", "Average Upside"), "recommendation_date", accent="#34d399"),
            "Average policy upside across current candidates.",
            width=2,
            height=20,
        ),
        ChartConfig(
            "Average Downside",
            "vw_daily_recommendation_board",
            "big_number_total",
            _big_number_params(daily, _simple_metric("policy_downside_pct", "AVG", "Average Downside"), "recommendation_date", accent="#fb7185"),
            "Average policy downside across current candidates.",
            width=2,
            height=20,
        ),
        ChartConfig(
            "Portfolio Return",
            "vw_dashboard_command_center",
            "big_number_total",
            _big_number_params(command, _simple_metric("portfolio_return_pct", "SUM", "Portfolio Return"), "market_date", accent="#34d399"),
            "Latest persisted portfolio validation return.",
            width=2,
            height=20,
            legacy_names=("CC - Portfolio Return %",),
        ),
        ChartConfig(
            "Max Drawdown",
            "vw_dashboard_command_center",
            "big_number_total",
            _big_number_params(command, _simple_metric("portfolio_max_drawdown_pct", "SUM", "Max Drawdown"), "market_date", accent="#f472b6"),
            "Latest persisted portfolio max drawdown.",
            width=2,
            height=20,
            legacy_names=("CC - Max Drawdown %",),
        ),
        ChartConfig(
            "Win Rate",
            "vw_dashboard_command_center",
            "big_number_total",
            _big_number_params(command, _simple_metric("portfolio_win_rate_pct", "SUM", "Win Rate"), "market_date", accent="#60a5fa"),
            "Latest persisted portfolio win rate.",
            width=2,
            height=20,
            legacy_names=("CC - Win Rate %",),
        ),
        ChartConfig(
            "Profit Factor",
            "vw_dashboard_command_center",
            "big_number_total",
            _big_number_params(command, _simple_metric("portfolio_profit_factor", "SUM", "Profit Factor"), "market_date", accent="#facc15"),
            "Latest persisted portfolio profit factor.",
            width=2,
            height=20,
            legacy_names=("CC - Profit Factor",),
        ),
        ChartConfig(
            "Data Quality",
            "vw_dashboard_command_center",
            "big_number_total",
            _big_number_params(command, _simple_metric("good_quality_pct", "SUM", "Data Quality"), "market_date", accent="#34d399"),
            "Latest daily market-data quality score.",
            width=2,
            height=20,
            legacy_names=("CC - Data Quality %",),
        ),
        ChartConfig(
            "Priced Stocks",
            "vw_market_overview",
            "big_number_total",
            _big_number_params(market, _simple_metric("priced_stocks", "SUM", "Priced Stocks"), "market_date", accent="#67e8f9"),
            "Number of stocks priced on the latest market date.",
            width=2,
            height=20,
        ),
        ChartConfig(
            "Advancers",
            "vw_market_overview",
            "big_number_total",
            _big_number_params(market, _simple_metric("advancers", "SUM", "Advancers"), "market_date", accent="#34d399"),
            "Stocks with positive daily movement.",
            width=2,
            height=20,
        ),
        ChartConfig(
            "Decliners",
            "vw_market_overview",
            "big_number_total",
            _big_number_params(market, _simple_metric("decliners", "SUM", "Decliners"), "market_date", accent="#fb7185"),
            "Stocks with negative daily movement.",
            width=2,
            height=20,
        ),
        ChartConfig(
            "Average Return",
            "vw_market_overview",
            "big_number_total",
            _big_number_params(market, _simple_metric("average_1d_return_pct", "AVG", "Average Return"), "market_date", accent="#a78bfa"),
            "Average one-day return across priced stocks.",
            width=2,
            height=20,
        ),
        ChartConfig(
            "Total Volume",
            "vw_market_overview",
            "big_number_total",
            _big_number_params(market, _simple_metric("total_volume", "SUM", "Total Volume"), "market_date", accent="#60a5fa"),
            "Total market volume for the latest market date.",
            width=2,
            height=20,
        ),
        ChartConfig(
            "Trade Count",
            "vw_model_health",
            "big_number_total",
            _big_number_params(model, _simple_metric("total_trades", "SUM", "Trade Count"), "run_date", accent="#67e8f9"),
            "Total trades in persisted validation runs.",
            width=2,
            height=20,
        ),
        ChartConfig(
            "Reconciliation",
            "vw_data_quality_monitor",
            "big_number_total",
            _big_number_params(quality, _simple_metric("reconciliation_pct", "AVG", "Reconciliation"), "market_date", accent="#22d3ee"),
            "Average reconciliation coverage.",
            width=2,
            height=20,
        ),
        ChartConfig(
            "Completeness",
            "vw_data_quality_monitor",
            "big_number_total",
            _big_number_params(quality, _simple_metric("completeness_pct", "AVG", "Completeness"), "market_date", accent="#60a5fa"),
            "Average completeness coverage.",
            width=2,
            height=20,
        ),
        ChartConfig(
            "Good Quality",
            "vw_data_quality_monitor",
            "big_number_total",
            _big_number_params(quality, _simple_metric("good_quality_pct", "AVG", "Good Quality"), "market_date", accent="#34d399"),
            "Average good-quality coverage.",
            width=2,
            height=20,
        ),
        ChartConfig(
            "Staged Rows",
            "vw_data_quality_monitor",
            "big_number_total",
            _big_number_params(quality, _simple_metric("staged_rows", "SUM", "Staged Rows"), "market_date", accent="#a78bfa"),
            "Rows present in staging.",
            width=2,
            height=20,
        ),
        ChartConfig(
            "Fact Rows",
            "vw_data_quality_monitor",
            "big_number_total",
            _big_number_params(quality, _simple_metric("fact_rows", "SUM", "Fact Rows"), "market_date", accent="#67e8f9"),
            "Rows present in the production price fact.",
            width=2,
            height=20,
        ),
        ChartConfig(
            "Invalid Prices",
            "vw_data_quality_monitor",
            "big_number_total",
            _big_number_params(
                quality,
                _sql_metric("SUM(invalid_staging_prices + invalid_fact_prices)", "Invalid Prices"),
                "market_date",
                accent="#fb7185",
            ),
            "Invalid staging and fact prices.",
            width=2,
            height=20,
        ),
        ChartConfig(
            "Recommendation Board",
            "vw_daily_recommendation_board",
            "table",
            _table_params(
                daily,
                [
                    "recommendation_rank",
                    "stock_code",
                    "company_name",
                    "sector_name",
                    "action_type",
                    "portfolio_approved",
                    "portfolio_rejection_reason",
                    "predicted_probability_10d_up_pct",
                    "signal_agreement_pct",
                    "heuristic_score",
                    "policy_upside_pct",
                    "policy_downside_pct",
                    "risk_reward_ratio",
                    "portfolio_position_size_pct",
                ],
                "recommendation_date",
                row_limit=100,
            ),
            "Decision board for the latest daily advisory run.",
            width=12,
            height=46,
            legacy_names=("Today - Recommendation Board",),
        ),
        ChartConfig(
            "Recommendation Preview",
            "vw_daily_recommendation_board",
            "table",
            _table_params(
                daily,
                ["recommendation_rank", "stock_code", "sector_name", "portfolio_approved", "predicted_probability_10d_up_pct", "risk_reward_ratio"],
                "recommendation_date",
                row_limit=20,
                server_page_length=10,
            ),
            "Short preview of current recommendations.",
            width=6,
            height=30,
        ),
        ChartConfig(
            "Weekly Recommendation Board",
            "vw_weekly_recommendation_board",
            "table",
            _table_params(
                weekly,
                [
                    "rank",
                    "weekly_status",
                    "stock_code",
                    "company_name",
                    "sector_name",
                    "action_type",
                    "heuristic_score",
                    "signal_agreement",
                    "rejection_reason",
                    "current_price",
                    "price_change_20d",
                    "drawdown_20d_pct",
                    "volume_ratio",
                ],
                "week_end_date",
                row_limit=50,
                server_page_length=15,
            ),
            "Weekly watchlist candidates ranked by model score with daily gate context.",
            width=12,
            height=46,
        ),
        ChartConfig(
            "Weekly Status Mix",
            "vw_weekly_recommendation_board",
            "echarts_timeseries_bar",
            _bar_params(
                weekly,
                "weekly_status",
                _sql_metric("COUNT(*)", "Candidates"),
                [],
                None,
                row_limit=20,
            ),
            "Weekly candidates grouped by action label.",
            width=6,
            height=34,
        ),
        ChartConfig(
            "Weekly Sector Mix",
            "vw_weekly_recommendation_board",
            "echarts_timeseries_bar",
            _bar_params(
                weekly,
                "sector_name",
                _sql_metric("COUNT(*)", "Candidates"),
                ["weekly_status"],
                None,
                row_limit=30,
            ),
            "Sector concentration of weekly candidates.",
            width=6,
            height=34,
        ),
        ChartConfig(
            "Rejection Reasons",
            "vw_daily_recommendation_board",
            "echarts_timeseries_bar",
            _bar_params(
                daily,
                "portfolio_rejection_reason",
                _sql_metric("COUNT(*)", "Rejected Candidates"),
                [],
                None,
                row_limit=20,
            ),
            "Reasons candidates failed the portfolio gate.",
            width=4,
            height=34,
        ),
        ChartConfig(
            "Sector Exposure",
            "vw_daily_recommendation_board",
            "echarts_timeseries_bar",
            _bar_params(daily, "sector_name", _sql_metric("COUNT(*)", "Candidates"), [], None, row_limit=20),
            "Recommendation exposure by sector.",
            width=4,
            height=34,
        ),
        ChartConfig(
            "Upside vs Downside",
            "vw_daily_recommendation_board",
            "echarts_timeseries_bar",
            _bar_params(
                daily,
                "stock_code",
                _simple_metric("policy_upside_pct", "AVG", "Average Upside"),
                ["portfolio_approved"],
                None,
                row_limit=30,
            ),
            "Upside ranking with approval context.",
            width=4,
            height=34,
        ),
        ChartConfig(
            "Probability vs Signal Agreement",
            "vw_daily_recommendation_board",
            "table",
            _table_params(
                daily,
                ["stock_code", "sector_name", "predicted_probability_10d_up_pct", "signal_agreement_pct", "heuristic_score", "portfolio_approved"],
                "recommendation_date",
                row_limit=50,
            ),
            "Probability and signal agreement side by side.",
            width=6,
            height=34,
        ),
        ChartConfig(
            "Market Breadth",
            "vw_market_overview",
            "echarts_timeseries_line",
            _line_params(
                market,
                "market_date",
                [_simple_metric("advancers", "SUM", "Advancers"), _simple_metric("decliners", "SUM", "Decliners")],
            ),
            "Advancers against decliners for market breadth.",
            width=6,
            height=34,
            legacy_names=("Market - Breadth",),
        ),
        ChartConfig(
            "Market Return Trend",
            "vw_market_overview",
            "echarts_timeseries_line",
            _line_params(market, "market_date", [_simple_metric("average_1d_return_pct", "AVG", "Average Return")]),
            "Average daily market return trend.",
            width=6,
            height=34,
        ),
        ChartConfig(
            "Sector Pulse",
            "vw_sector_performance",
            "echarts_timeseries_bar",
            _bar_params(
                sector,
                "sector_name",
                _simple_metric("average_1d_return_pct", "AVG", "Average 1D Return"),
                [],
                None,
                row_limit=30,
            ),
            "Sector-level latest daily return pulse.",
            width=6,
            height=34,
            legacy_names=("Market - Sector Pulse",),
        ),
        ChartConfig(
            "Sector Volume",
            "vw_sector_performance",
            "echarts_timeseries_bar",
            _bar_params(sector, "sector_name", _simple_metric("total_volume", "SUM", "Total Volume"), [], None, row_limit=30),
            "Total volume by sector.",
            width=6,
            height=34,
        ),
        ChartConfig(
            "Sector Recommendations",
            "vw_sector_performance",
            "echarts_timeseries_bar",
            _bar_params(sector, "sector_name", _simple_metric("recommendation_count", "SUM", "Recommendations"), [], None, row_limit=30),
            "Recommendation count by sector.",
            width=6,
            height=34,
        ),
        ChartConfig(
            "Strongest Sectors",
            "vw_sector_performance",
            "table",
            _table_params(
                sector,
                ["market_date", "sector_name", "average_1d_return_pct", "average_ytd_return_pct", "total_volume", "actionable_count", "average_probability_pct"],
                "market_date",
                row_limit=30,
            ),
            "Ranked sector strength and advisory concentration.",
            width=6,
            height=34,
        ),
        ChartConfig(
            "Latest Model Verdict",
            "vw_latest_model_verdict",
            "table",
            _table_params(
                verdict,
                [
                    "run_date",
                    "profile",
                    "decision_status",
                    "portfolio_trade_count",
                    "portfolio_return_pct",
                    "portfolio_max_drawdown_pct",
                    "portfolio_win_rate_pct",
                    "portfolio_profit_factor",
                    "raw_trade_count",
                    "raw_win_rate_pct",
                    "raw_profit_factor",
                ],
                "run_date",
                row_limit=20,
            ),
            "Latest persisted model verdict and raw-versus-portfolio comparison.",
            width=6,
            height=34,
            legacy_names=("Model - Latest Verdict",),
        ),
        ChartConfig(
            "Validation Runs",
            "vw_model_health",
            "table",
            _table_params(
                model,
                ["run_date", "profile", "decision_status", "total_trades", "win_rate_pct", "profit_factor", "max_drawdown_pct", "portfolio_return_pct", "portfolio_profit_factor"],
                "run_date",
                row_limit=50,
            ),
            "Persisted validation runs for model-health inspection.",
            width=6,
            height=34,
            legacy_names=("Model - Validation Runs",),
        ),
        ChartConfig(
            "Yearly Performance",
            "vw_model_yearly_performance",
            "table",
            _table_params(
                yearly,
                [
                    "run_date",
                    "profile",
                    "calendar_year",
                    "trade_count",
                    "win_rate_pct",
                    "average_return_pct",
                    "profit_factor",
                    "portfolio_return_pct",
                    "portfolio_max_drawdown_pct",
                    "ending_equity",
                ],
                None,
                row_limit=100,
            ),
            "Year-by-year model performance.",
            width=6,
            height=34,
        ),
        ChartConfig(
            "Equity Curve",
            "vw_portfolio_equity_curve",
            "echarts_timeseries_line",
            _line_params(equity, "event_date", [_simple_metric("equity", "MAX", "Equity")]),
            "Portfolio equity curve for the persisted validation run.",
            width=8,
            height=40,
            legacy_names=("Portfolio - Equity Curve",),
        ),
        ChartConfig(
            "Drawdown Curve",
            "vw_portfolio_drawdown_curve",
            "echarts_timeseries_line",
            _line_params(drawdown, "event_date", [_simple_metric("drawdown_pct", "MAX", "Drawdown")]),
            "Portfolio drawdown curve for the persisted validation run.",
            width=4,
            height=40,
            legacy_names=("Portfolio - Drawdown Curve",),
        ),
        ChartConfig(
            "Return Distribution",
            "vw_trade_distribution",
            "echarts_timeseries_bar",
            _bar_params(trades, "return_bucket", _sql_metric("COUNT(*)", "Trades"), [], None, row_limit=20),
            "Distribution of realized trade returns.",
            width=4,
            height=34,
            legacy_names=("Trades - Return Distribution",),
        ),
        ChartConfig(
            "Sector Model Performance",
            "vw_sector_model_performance",
            "echarts_timeseries_bar",
            _bar_params(sector_model, "sector_name", _simple_metric("average_return_pct", "AVG", "Average Return"), [], None, row_limit=30),
            "Model performance by sector.",
            width=4,
            height=34,
            legacy_names=("Model - Sector Performance",),
        ),
        ChartConfig(
            "Stock Performance",
            "vw_stock_model_performance",
            "table",
            _table_params(
                stock_model,
                ["stock_code", "sector_name", "trade_count", "win_rate_pct", "average_return_pct", "best_trade_pct", "worst_trade_pct", "profit_factor"],
                None,
                row_limit=75,
            ),
            "Stock-level model performance drilldown.",
            width=4,
            height=34,
            legacy_names=("Model - Best Stocks",),
        ),
        ChartConfig(
            "Best Stocks",
            "vw_stock_model_performance",
            "table",
            _table_params(
                stock_model,
                ["stock_code", "sector_name", "trade_count", "win_rate_pct", "average_return_pct", "profit_factor"],
                None,
                row_limit=30,
            ),
            "Best stock-level model outcomes.",
            width=6,
            height=34,
        ),
        ChartConfig(
            "Price Drilldown",
            "vw_stock_price_panel",
            "table",
            _table_params(
                prices,
                ["price_date", "stock_code", "company_name", "sector_name", "close_price", "change_1d_pct", "change_ytd_pct", "volume", "rsi_14", "macd", "trend_strength", "data_quality_flag"],
                "price_date",
                row_limit=200,
            ),
            "Stock drilldown panel for price, trend, and indicator context.",
            width=12,
            height=38,
            legacy_names=("Stocks - Price Drilldown",),
        ),
        ChartConfig(
            "Price Trend",
            "vw_stock_price_panel",
            "echarts_timeseries_line",
            _line_params(prices, "price_date", [_simple_metric("close_price", "AVG", "Close Price")], ["stock_code"], row_limit=5000),
            "Close-price trend by stock.",
            width=8,
            height=40,
        ),
        ChartConfig(
            "Moving Averages",
            "vw_stock_price_panel",
            "echarts_timeseries_line",
            _line_params(
                prices,
                "price_date",
                [
                    _simple_metric("ma_7", "AVG", "MA 7"),
                    _simple_metric("ma_30", "AVG", "MA 30"),
                    _simple_metric("ma_90", "AVG", "MA 90"),
                ],
                [],
                row_limit=5000,
            ),
            "Moving-average trend context.",
            width=4,
            height=40,
        ),
        ChartConfig(
            "RSI",
            "vw_stock_price_panel",
            "echarts_timeseries_line",
            _line_params(prices, "price_date", [_simple_metric("rsi_14", "AVG", "RSI 14")], ["stock_code"], row_limit=5000),
            "Relative strength trend.",
            width=4,
            height=32,
        ),
        ChartConfig(
            "MACD",
            "vw_stock_price_panel",
            "echarts_timeseries_line",
            _line_params(
                prices,
                "price_date",
                [_simple_metric("macd", "AVG", "MACD"), _simple_metric("macd_signal", "AVG", "Signal")],
                ["stock_code"],
                row_limit=5000,
            ),
            "MACD and signal-line context.",
            width=4,
            height=32,
        ),
        ChartConfig(
            "Volatility",
            "vw_stock_price_panel",
            "echarts_timeseries_line",
            _line_params(prices, "price_date", [_simple_metric("volatility_30", "AVG", "Volatility 30")], ["stock_code"], row_limit=5000),
            "30-day volatility trend.",
            width=4,
            height=32,
        ),
        ChartConfig(
            "Recommendation History",
            "vw_recommendation_board",
            "table",
            _table_params(
                history,
                ["recommendation_date", "stock_code", "sector_name", "action_type", "portfolio_approved", "predicted_probability_10d_up_pct", "signal_agreement_pct", "risk_reward_ratio"],
                "recommendation_date",
                row_limit=100,
            ),
            "Historical recommendation decisions for stock review.",
            width=6,
            height=34,
        ),
        ChartConfig(
            "Sector Context",
            "vw_sector_performance",
            "table",
            _table_params(
                sector,
                ["market_date", "sector_name", "average_1d_return_pct", "average_ytd_return_pct", "total_volume", "actionable_count", "average_probability_pct"],
                "market_date",
                row_limit=50,
            ),
            "Sector context for selected stocks.",
            width=6,
            height=34,
        ),
        ChartConfig(
            "Quality Monitor",
            "vw_data_quality_monitor",
            "table",
            _table_params(
                quality,
                ["market_date", "staged_rows", "reconciled_rows", "fact_rows", "invalid_staging_prices", "invalid_fact_prices", "reconciliation_pct", "good_quality_pct", "completeness_pct"],
                "market_date",
                row_limit=30,
            ),
            "Pipeline data-quality monitor for the latest market dates.",
            width=6,
            height=34,
            legacy_names=("Data - Quality Monitor",),
        ),
        ChartConfig(
            "Reconciliation Trend",
            "vw_data_quality_monitor",
            "echarts_timeseries_line",
            _line_params(quality, "market_date", [_simple_metric("reconciliation_pct", "AVG", "Reconciliation")]),
            "Reconciliation trend over time.",
            width=4,
            height=32,
        ),
        ChartConfig(
            "Completeness Trend",
            "vw_data_quality_monitor",
            "echarts_timeseries_line",
            _line_params(quality, "market_date", [_simple_metric("completeness_pct", "AVG", "Completeness")]),
            "Completeness trend over time.",
            width=4,
            height=32,
        ),
        ChartConfig(
            "Invalid Price Trend",
            "vw_data_quality_monitor",
            "echarts_timeseries_line",
            _line_params(quality, "market_date", [_sql_metric("SUM(invalid_staging_prices + invalid_fact_prices)", "Invalid Prices")]),
            "Invalid price trend over time.",
            width=4,
            height=32,
        ),
        ChartConfig(
            "Missing Change Fields",
            "vw_data_quality_monitor",
            "echarts_timeseries_bar",
            _bar_params(
                quality,
                "market_date",
                _sql_metric("SUM(missing_staging_1d_change + missing_staging_ytd_change)", "Missing Change Fields"),
                [],
                "market_date",
                row_limit=60,
            ),
            "Missing one-day and YTD change fields.",
            width=6,
            height=34,
        ),
    ]


DASHBOARDS = [
    DashboardConfig(
        "Command Center",
        "command-center",
        "Executive readiness dashboard for model verdict, market freshness, portfolio safety, and today's action count.",
        (
            "Decision Status",
            "Run Date",
            "Market Date",
            "Approved Picks",
            "Data Quality",
            "Portfolio Return",
            "Max Drawdown",
            "Win Rate",
            "Profit Factor",
            "Equity Curve",
            "Drawdown Curve",
            "Market Breadth",
            "Sector Pulse",
            "Recommendation Preview",
        ),
        legacy_titles=("NGX Advisory Command Center",),
        row_message="Trust, freshness, and actionability before any trade decision.",
    ),
    DashboardConfig(
        "Today's Decision Board",
        "todays-decision-board",
        "Action workspace for approved recommendations, rejected candidates, risk-reward, probability, and sector exposure.",
        (
            "Approved Picks",
            "Rejected Candidates",
            "Average Probability",
            "Average Risk-Reward",
            "Average Upside",
            "Average Downside",
            "Recommendation Board",
            "Rejection Reasons",
            "Sector Exposure",
            "Upside vs Downside",
            "Probability vs Signal Agreement",
        ),
        row_message="Trading blotter for the current advisory run.",
    ),
    DashboardConfig(
        "Weekly Recommendation Board",
        "weekly-recommendation-board",
        "Weekly watchlist workspace for slower setups, pullback waits, volume waits, and high-risk candidates.",
        (
            "Weekly Recommendation Board",
            "Weekly Status Mix",
            "Weekly Sector Mix",
            "Recommendation Board",
            "Market Breadth",
            "Sector Pulse",
        ),
        row_message="Weekly candidates are not automatic buys; each label explains what must improve before daily approval.",
    ),
    DashboardConfig(
        "Market & Sector Pulse",
        "market-sector-pulse",
        "Market breadth, sector rotation, volume concentration, and recommendation clustering.",
        (
            "Priced Stocks",
            "Advancers",
            "Decliners",
            "Average Return",
            "Total Volume",
            "Market Breadth",
            "Market Return Trend",
            "Sector Pulse",
            "Sector Volume",
            "Sector Recommendations",
            "Strongest Sectors",
        ),
        row_message="A market map for deciding whether sector strength supports the model.",
    ),
    DashboardConfig(
        "Model Health",
        "model-health",
        "Risk committee view of validation safety, drawdown, distribution, sector behavior, and stock-level robustness.",
        (
            "Decision Status",
            "Portfolio Return",
            "Max Drawdown",
            "Win Rate",
            "Profit Factor",
            "Trade Count",
            "Equity Curve",
            "Drawdown Curve",
            "Return Distribution",
            "Sector Model Performance",
            "Stock Performance",
            "Validation Runs",
            "Yearly Performance",
        ),
        row_message="Evidence that the model is safe enough to trust.",
    ),
    DashboardConfig(
        "Stock Drilldown",
        "stock-drilldown",
        "Bloomberg-style stock tear sheet for price, indicators, recommendation history, and sector context.",
        (
            "Price Drilldown",
            "Price Trend",
            "Moving Averages",
            "RSI",
            "MACD",
            "Volatility",
            "Recommendation History",
            "Stock Performance",
            "Sector Context",
        ),
        row_message="One-stock-at-a-time context before action.",
    ),
    DashboardConfig(
        "Data Quality",
        "data-quality",
        "Operational trust dashboard for reconciliation, completeness, invalid prices, and pipeline readiness.",
        (
            "Reconciliation",
            "Completeness",
            "Good Quality",
            "Staged Rows",
            "Fact Rows",
            "Invalid Prices",
            "Quality Monitor",
            "Reconciliation Trend",
            "Completeness Trend",
            "Invalid Price Trend",
            "Missing Change Fields",
        ),
        row_message="If this page is red, the financial dashboards are not trustworthy.",
    ),
]


def _upsert_database(db: Any, database_model: Any) -> Any:
    database = (
        db.session.query(database_model)
        .filter(database_model.database_name == DATABASE_NAME)
        .one_or_none()
    )
    if database is None:
        database = database_model(database_name=DATABASE_NAME)
        db.session.add(database)

    database.set_sqlalchemy_uri(_database_uri())
    database.expose_in_sqllab = True
    database.allow_ctas = False
    database.allow_cvas = False
    database.allow_dml = False
    database.allow_file_upload = False
    database.select_as_create_table_as = False
    db.session.flush()
    return database


def _upsert_dataset(db: Any, dataset_model: Any, database: Any, config: DatasetConfig) -> str:
    dataset = (
        db.session.query(dataset_model)
        .filter(
            dataset_model.database_id == database.id,
            dataset_model.schema == "public",
            dataset_model.table_name == config.view_name,
        )
        .one_or_none()
    )
    action = "updated"
    if dataset is None:
        dataset = dataset_model(database=database, schema="public", table_name=config.view_name)
        db.session.add(dataset)
        db.session.flush()
        action = "created"

    dataset.description = config.description
    dataset.default_endpoint = None
    dataset.filter_select_enabled = True
    if config.main_dttm_col:
        dataset.main_dttm_col = config.main_dttm_col

    result = dataset.fetch_metadata()
    db.session.merge(dataset)
    return f"{config.label}: {action}, +{len(result.added)}, ~{len(result.modified)}, -{len(result.removed)} columns"


def _find_chart(db: Any, chart_model: Any, config: ChartConfig) -> Any | None:
    names = (config.name, *config.legacy_names)
    return db.session.query(chart_model).filter(chart_model.slice_name.in_(names)).one_or_none()


def _upsert_chart(
    db: Any,
    chart_model: Any,
    datasets: dict[str, Any],
    config: ChartConfig,
) -> tuple[Any, ChartConfig, str]:
    dataset = datasets[config.dataset]
    chart = _find_chart(db, chart_model, config)
    action = "updated"
    if chart is None:
        chart = chart_model(slice_name=config.name)
        db.session.add(chart)
        action = "created"

    params = dict(config.params)
    params["datasource"] = f"{dataset.id}__table"
    chart.slice_name = config.name
    chart.datasource_id = dataset.id
    chart.datasource_type = "table"
    chart.datasource_name = dataset.table_name
    chart.viz_type = config.viz_type
    chart.params = json.dumps(params, sort_keys=True)
    chart.query_context = None
    chart.description = config.description
    db.session.flush()
    return chart, config, f"{config.name}: {action}"


def _color_picker_to_hex(color_picker: dict[str, Any] | None) -> str:
    if not color_picker:
        return "#67e8f9"
    return "#{:02x}{:02x}{:02x}".format(
        int(color_picker.get("r", 103)),
        int(color_picker.get("g", 232)),
        int(color_picker.get("b", 249)),
    )


def _kpi_value_css(charts: list[tuple[Any, ChartConfig]]) -> str:
    rules = []
    for chart, chart_config in charts:
        if chart_config.viz_type != "big_number_total":
            continue
        accent = _color_picker_to_hex(chart_config.params.get("color_picker"))
        rules.append(
            f""".dashboard-chart-id-{chart.id} .superset-legacy-chart-big-number .header-line {{
  color: {accent} !important;
  fill: {accent} !important;
  text-shadow: 0 0 20px {accent}33 !important;
}}"""
        )
    return "\n".join(rules)


def _dashboard_css(charts: list[tuple[Any, ChartConfig]]) -> str:
    base_css = """
body, .dashboard, .dashboard-content, .grid-container {
  background: radial-gradient(circle at top left, #17324f 0, #0b1220 38%, #050814 100%) !important;
  color: #e5eef8;
}
.dashboard-header, .dashboard-component-header {
  color: #f8fafc !important;
  font-weight: 800;
  letter-spacing: -0.02em;
}
.dashboard-component-chart-holder {
  background: linear-gradient(180deg, rgba(15, 23, 42, 0.98), rgba(7, 12, 24, 0.98)) !important;
  border: 1px solid rgba(148, 163, 184, 0.22);
  border-radius: 16px;
  box-shadow: 0 18px 48px rgba(0, 0, 0, 0.34);
}
.dashboard-component-chart-holder .chart-container,
.dashboard-component-chart-holder .slice_container,
.dashboard-component-chart-holder .echarts-for-react {
  background: #0f172a !important;
  border-radius: 12px;
}
.dashboard-component-chart-holder .slice_container {
  padding: 8px;
}
.dashboard-component-chart-holder svg text,
.dashboard-component-chart-holder canvas + div,
.dashboard-component-chart-holder .recharts-text,
.dashboard-component-chart-holder .nv-axis text {
  fill: #dbeafe !important;
  color: #dbeafe !important;
}
.dashboard-component-chart-holder .big-number,
.dashboard-component-chart-holder .big-number-total,
.dashboard-component-chart-holder .header-line,
.dashboard-component-chart-holder .subheader-line,
.dashboard-component-chart-holder .metric-value,
.dashboard-component-chart-holder [class*="BigNumber"],
.dashboard-component-chart-holder [class*="big-number"] {
  color: #67e8f9 !important;
  fill: #67e8f9 !important;
  text-shadow: 0 0 18px rgba(103, 232, 249, 0.18) !important;
}
.dashboard-component-chart-holder .header-line {
  color: #67e8f9 !important;
  fill: #67e8f9 !important;
  font-weight: 800;
}
.chart-header .header-title, .slice-header-title, .dashboard-component-chart-holder a {
  color: #f8fafc !important;
}
.dashboard-component-chart-holder .header-title {
  font-weight: 750;
}
.dashboard-component-chart-holder table {
  background: #0f172a !important;
  color: #dbeafe !important;
  font-size: 13px;
}
.dashboard-component-chart-holder th {
  background: #152238 !important;
  color: #f8fafc !important;
}
.dashboard-component-chart-holder td {
  border-color: rgba(148, 163, 184, 0.18) !important;
  color: #dbeafe !important;
}
.dashboard-component-chart-holder .table-condensed > tbody > tr > td {
  color: #dbeafe !important;
}
.dashboard-markdown {
  color: #cbd5e1 !important;
  background: linear-gradient(90deg, rgba(250, 204, 21, 0.12), rgba(20, 184, 166, 0.10));
  border: 1px solid rgba(250, 204, 21, 0.20);
  border-radius: 16px;
  padding: 14px 18px;
}
""".strip()
    kpi_css = _kpi_value_css(charts)
    return f"{base_css}\n{kpi_css}".strip()


def _dashboard_metadata() -> str:
    return json.dumps(
        {
            "color_namespace": "stock_pipeline",
            "color_scheme": FINANCE_COLOR_SCHEME,
            "default_filters": "{}",
            "label_colors": {
                "Advancers": "#14b8a6",
                "Approved": "#22c55e",
                "Decliners": "#fb7185",
                "Drawdown": "#fb7185",
                "GREEN": "#22c55e",
                "Rejected": "#fb7185",
                "RED": "#ef4444",
                "YELLOW": "#f59e0b",
                "Equity": "#38bdf8",
                "Average Return": "#facc15",
                "Average 1D Return": "#facc15",
                "Trades": "#94a3b8",
            },
            "map_label_colors": {
                "Advancers": "#14b8a6",
                "Decliners": "#fb7185",
                "Equity": "#60a5fa",
                "Drawdown": "#f472b6",
                "Average Return": "#a78bfa",
                "Average 1D Return": "#a78bfa",
                "Average Upside": "#34d399",
                "Average Downside": "#fb7185",
                "Average Probability": "#22d3ee",
                "Average Risk-Reward": "#facc15",
                "Close Price": "#38bdf8",
                "MA 7": "#34d399",
                "MA 30": "#a78bfa",
                "MA 90": "#facc15",
                "RSI 14": "#22d3ee",
                "MACD": "#60a5fa",
                "Signal": "#f472b6",
                "Approved": "#22c55e",
                "Rejected": "#fb7185",
            },
            "refresh_frequency": 0,
            "timed_refresh_immune_slices": [],
            "expanded_slices": {},
        },
        sort_keys=True,
    )


def _build_position(charts: list[tuple[Any, ChartConfig]], config: DashboardConfig) -> dict[str, Any]:
    position: dict[str, Any] = {
        "DASHBOARD_VERSION_KEY": "v2",
        "ROOT_ID": {"type": "ROOT", "id": "ROOT_ID", "children": ["GRID_ID"]},
        "GRID_ID": {"type": "GRID", "id": "GRID_ID", "children": ["ROW-INTRO"]},
        "ROW-INTRO": {
            "type": "ROW",
            "id": "ROW-INTRO",
            "children": ["MARKDOWN-INTRO"],
            "meta": {"background": "BACKGROUND_TRANSPARENT"},
        },
        "MARKDOWN-INTRO": {
            "type": "MARKDOWN",
            "id": "MARKDOWN-INTRO",
            "children": [],
            "meta": {"code": f"### {config.title}\n{config.row_message}", "width": 12, "height": 14},
        },
    }

    row_index = 1
    current_width = 0
    current_row_id = ""

    for index, (chart, chart_config) in enumerate(charts, start=1):
        if not current_row_id or current_width + chart_config.width > 12:
            current_row_id = f"ROW-{row_index:02d}"
            row_index += 1
            current_width = 0
            position["GRID_ID"]["children"].append(current_row_id)
            position[current_row_id] = {
                "type": "ROW",
                "id": current_row_id,
                "children": [],
                "meta": {"background": "BACKGROUND_TRANSPARENT"},
            }

        chart_id = f"CHART-{index:02d}"
        position[current_row_id]["children"].append(chart_id)
        position[chart_id] = {
            "type": "CHART",
            "id": chart_id,
            "children": [],
            "meta": {
                "chartId": chart.id,
                "height": chart_config.height,
                "sliceName": chart.slice_name,
                "uuid": str(chart.uuid),
                "width": chart_config.width,
            },
        }
        current_width += chart_config.width

    return position


def _find_dashboard(db: Any, dashboard_model: Any, config: DashboardConfig) -> Any | None:
    titles = (config.title, *config.legacy_titles)
    return db.session.query(dashboard_model).filter(dashboard_model.dashboard_title.in_(titles)).one_or_none()


def _upsert_dashboard(
    db: Any,
    dashboard_model: Any,
    chart_by_name: dict[str, tuple[Any, ChartConfig]],
    config: DashboardConfig,
) -> str:
    dashboard = _find_dashboard(db, dashboard_model, config)
    action = "updated"
    if dashboard is None:
        dashboard = dashboard_model(dashboard_title=config.title)
        db.session.add(dashboard)
        action = "created"

    dashboard_charts = [chart_by_name[name] for name in config.chart_names]
    dashboard.dashboard_title = config.title
    dashboard.slug = config.slug
    dashboard.description = config.description
    dashboard.css = _dashboard_css(dashboard_charts)
    dashboard.published = True
    dashboard.position_json = json.dumps(_build_position(dashboard_charts, config), sort_keys=True)
    dashboard.json_metadata = _dashboard_metadata()
    dashboard.slices = [chart for chart, _ in dashboard_charts]
    db.session.flush()
    return f"{config.title}: {action}, {len(dashboard_charts)} charts attached"


def _remove_empty_placeholder_dashboards(db: Any, dashboard_model: Any) -> list[str]:
    removed: list[str] = []
    placeholders = (
        db.session.query(dashboard_model)
        .filter(dashboard_model.dashboard_title == "[ untitled dashboard ]")
        .all()
    )
    for dashboard in placeholders:
        if dashboard.slug is None and not dashboard.slices:
            removed.append(f"Removed empty placeholder dashboard id {dashboard.id}")
            db.session.delete(dashboard)
    return removed


def main() -> None:
    app = create_app()
    with app.app_context():
        from superset import db
        from superset.connectors.sqla.models import SqlaTable
        from superset.models.core import Database
        from superset.models.dashboard import Dashboard
        from superset.models.slice import Slice

        database = _upsert_database(db, Database)
        dataset_messages = [_upsert_dataset(db, SqlaTable, database, dataset) for dataset in DATASETS]
        db.session.flush()

        datasets = {
            dataset.table_name: dataset
            for dataset in db.session.query(SqlaTable)
            .filter(SqlaTable.database_id == database.id, SqlaTable.schema == "public")
            .all()
        }
        dataset_refs = {table_name: f"{dataset.id}__table" for table_name, dataset in datasets.items()}
        chart_results = [_upsert_chart(db, Slice, datasets, chart_config) for chart_config in _chart_configs(dataset_refs)]
        chart_by_name = {chart_config.name: (chart, chart_config) for chart, chart_config, _ in chart_results}
        dashboard_messages = [_upsert_dashboard(db, Dashboard, chart_by_name, dashboard) for dashboard in DASHBOARDS]
        cleanup_messages = _remove_empty_placeholder_dashboards(db, Dashboard)
        db.session.commit()

    print(f"Bootstrapped Superset database: {DATABASE_NAME}")
    for message in dataset_messages:
        print(message)
    for _, _, message in chart_results:
        print(message)
    for message in dashboard_messages:
        print(message)
    for message in cleanup_messages:
        print(message)


if __name__ == "__main__":
    main()
