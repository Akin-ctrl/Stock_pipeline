from datetime import date

from app.services.modeling.model_validation import (
    ValidationPrediction,
    build_probability_bucket_stats,
    compare_momentum_baseline,
    generate_walk_forward_windows,
)


def test_generate_walk_forward_windows_respects_chronology():
    windows = generate_walk_forward_windows(
        start_date=date(2026, 1, 10),
        end_date=date(2026, 2, 8),
        training_window_days=30,
        evaluation_window_days=10,
        step_days=10,
    )

    assert len(windows) == 3
    assert windows[0].training_end == date(2026, 1, 9)
    assert windows[0].evaluation_start == date(2026, 1, 10)
    assert windows[0].evaluation_end == date(2026, 1, 19)
    assert windows[1].evaluation_start == date(2026, 1, 20)
    assert windows[2].evaluation_end == date(2026, 2, 8)


def test_probability_bucket_stats_and_momentum_baseline_are_computed_consistently():
    predictions = [
        ValidationPrediction("AAA", date(2026, 1, 1), 0.80, 1, 9.0, 7.0),
        ValidationPrediction("BBB", date(2026, 1, 2), 0.65, 1, 5.0, 4.0),
        ValidationPrediction("CCC", date(2026, 1, 3), 0.35, 0, -3.0, -2.0),
        ValidationPrediction("DDD", date(2026, 1, 4), 0.20, 0, -6.0, -5.0),
    ]

    bucket_stats = build_probability_bucket_stats(
        predictions,
        bucket_edges=(0.0, 0.4, 0.7, 1.0),
    )
    baseline = compare_momentum_baseline(predictions, top_k=2)

    assert [bucket.row_count for bucket in bucket_stats] == [2, 1, 1]
    assert bucket_stats[0].positive_rate == 0.0
    assert bucket_stats[2].positive_rate == 1.0
    assert baseline.hit_rate == 1.0
    assert baseline.top_k_hit_rate == 1.0
    assert baseline.top_k_average_forward_return_10d == 7.0
