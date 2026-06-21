from app.services.advisory.scoring import StockScorer


def test_stock_scorer_handles_missing_price_change_pct_neutrally():
    scorer = StockScorer()

    score = scorer.calculate_score(
        {
            "current_price": 10.0,
            "volume_ratio": 1.2,
            "price_change_pct": None,
        }
    )

    assert score.volume_score == 55
    assert score.breakdown["volume_score"] == 55
