from datetime import date

from app.services.modeling.targets import (
    DEFAULT_DIRECTION_TARGET,
    build_forward_return_label,
    calculate_forward_return_pct,
)


def test_calculate_forward_return_pct_handles_positive_and_negative_moves():
    assert round(calculate_forward_return_pct(100.0, 110.0), 4) == 10.0
    assert round(calculate_forward_return_pct(100.0, 90.0), 4) == -10.0


def test_build_forward_return_label_uses_canonical_positive_direction_rule():
    label = build_forward_return_label(
        anchor_date=date(2026, 1, 2),
        anchor_close_price=100.0,
        horizon_date=date(2026, 1, 16),
        horizon_close_price=100.0,
        target_definition=DEFAULT_DIRECTION_TARGET,
    )

    assert label.forward_return_pct == 0.0
    assert label.target_up == 0


def test_build_forward_return_label_marks_positive_returns_as_up():
    label = build_forward_return_label(
        anchor_date=date(2026, 1, 2),
        anchor_close_price=100.0,
        horizon_date=date(2026, 1, 16),
        horizon_close_price=103.5,
        target_definition=DEFAULT_DIRECTION_TARGET,
    )

    assert round(label.forward_return_pct, 4) == 3.5
    assert label.target_up == 1
