from datetime import date
from types import SimpleNamespace

from scripts.weekly_recommendations import _candidate_rows, _week_start, _weekly_status


def test_week_start_returns_monday_for_market_date():
    assert _week_start(date(2026, 6, 9)) == date(2026, 6, 8)


def test_weekly_status_preserves_approved_candidates():
    assert _weekly_status(
        candidate_tier="approved",
        rejection_reason=None,
    ) == "APPROVED"


def test_weekly_status_turns_daily_gate_reasons_into_actionable_labels():
    assert _weekly_status(
        candidate_tier="watchlist",
        rejection_reason="below_min_drawdown_20d",
    ) == "WAIT_FOR_PULLBACK"
    assert _weekly_status(
        candidate_tier="watchlist",
        rejection_reason="below_min_volume_ratio",
    ) == "WAIT_FOR_VOLUME"
    assert _weekly_status(
        candidate_tier="watchlist",
        rejection_reason="below_min_price",
    ) == "SPECULATIVE_WATCHLIST"


def test_candidate_rows_rank_by_score_before_status(monkeypatch):
    high_score_pullback = SimpleNamespace(
        candidate_tier="watchlist",
        rejection_reason="below_min_drawdown_20d",
        heuristic_score=92.0,
        signal_agreement=0.60,
    )
    lower_score_watchlist = SimpleNamespace(
        candidate_tier="watchlist",
        rejection_reason="below_min_signal_agreement",
        heuristic_score=76.0,
        signal_agreement=0.45,
    )

    class QueryStub:
        def join(self, *args, **kwargs):
            return self

        def outerjoin(self, *args, **kwargs):
            return self

        def filter(self, *args, **kwargs):
            return self

        def order_by(self, *args, **kwargs):
            return self

        def all(self):
            return [
                (lower_score_watchlist, "NEM", "NEM Insurance", "Financial Services"),
                (high_score_pullback, "INFINITY", "Infinity Trust", "Financial Services"),
            ]

    session = SimpleNamespace(query=lambda *args, **kwargs: QueryStub())

    rows = _candidate_rows(
        session,
        recommendation_date=date(2026, 6, 9),
        profile="steady_20p_10d",
        min_score=68.0,
    )

    assert rows[0][1] == "INFINITY"
