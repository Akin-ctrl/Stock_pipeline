from datetime import date

import pytest

from scripts.backfill_historical_indicators import (
    DEFAULT_WARMUP_CALENDAR_DAYS,
    _history_start_for,
)


def test_history_start_uses_long_sparse_trading_warmup():
    assert _history_start_for(
        date(2026, 6, 1),
        DEFAULT_WARMUP_CALENDAR_DAYS,
    ) == date(2016, 6, 3)


def test_history_start_is_unbounded_without_start_date():
    assert _history_start_for(None, DEFAULT_WARMUP_CALENDAR_DAYS) is None


def test_history_start_rejects_negative_warmup():
    with pytest.raises(ValueError):
        _history_start_for(date(2026, 6, 1), -1)
