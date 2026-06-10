from types import SimpleNamespace

from scripts.backtest_recommendations import _build_readiness_gate


def _args() -> SimpleNamespace:
    return SimpleNamespace(
        readiness_min_total_trades=50,
        readiness_min_portfolio_return_pct=20.0,
        readiness_min_portfolio_win_rate_pct=55.0,
        readiness_min_portfolio_profit_factor=2.0,
        readiness_max_portfolio_drawdown_pct=10.0,
    )


def _passing_payload() -> dict:
    return {
        "total_trades": 69,
        "portfolio": {
            "total_return_pct": 40.56,
            "win_rate_pct": 58.82,
            "profit_factor": 2.23,
            "max_drawdown_pct": 6.21,
        },
        "runtime_warnings": [],
    }


def test_readiness_gate_passes_when_metrics_are_green_and_warnings_absent():
    gate = _build_readiness_gate(_passing_payload(), _args())

    assert gate["status"] == "PASS"
    assert gate["failed_checks"] == []


def test_readiness_gate_fails_when_runtime_warnings_are_present():
    payload = _passing_payload()
    payload["runtime_warnings"] = [
        {
            "logger": "stock_screener",
            "level": "WARNING",
            "message": "Failed to analyze TEST: bad indicator",
        }
    ]

    gate = _build_readiness_gate(payload, _args())

    assert gate["status"] == "FAIL"
    assert "runtime_warnings_absent" in gate["failed_checks"]
