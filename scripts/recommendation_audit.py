#!/usr/bin/env python3
"""Generate and persist recommendation candidate-funnel audit rows."""

from __future__ import annotations

import argparse
from collections import Counter
from datetime import date
import logging

from app.config.database import get_db
from app.models import FactDailyPrice
from app.repositories.recommendation_repository import RecommendationRepository
from app.services.advisory import ProductionPortfolioPolicy, StockScreener
from app.services.modeling import NullProbabilityEstimator
from sqlalchemy import func


def _parse_iso_date(raw_value: str) -> date:
    try:
        return date.fromisoformat(raw_value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            f"Expected YYYY-MM-DD date, got {raw_value!r}"
        ) from exc


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Persist the recommendation candidate-funnel audit for a market date."
    )
    parser.add_argument(
        "--recommendation-date",
        "--market-date",
        dest="recommendation_date",
        type=_parse_iso_date,
        help="Market/recommendation date to audit. Defaults to latest price date.",
    )
    parser.add_argument(
        "--stocks",
        default="",
        help="Optional comma-separated stock codes for a partial audit refresh.",
    )
    parser.add_argument(
        "--strategy-profile",
        default="steady_20p_10d",
        choices=["steady_20p_10d", "steady_20p_10d_v2"],
        help="Recommendation profile to audit.",
    )
    parser.add_argument(
        "--disable-probability",
        action="store_true",
        help="Skip probability-model training when only gate diagnostics are needed.",
    )
    return parser.parse_args()


def _resolve_recommendation_date(session, requested_date: date | None) -> date:
    if requested_date is not None:
        return requested_date

    latest_price_date = session.query(func.max(FactDailyPrice.price_date)).scalar()
    if latest_price_date is None:
        raise RuntimeError("Cannot audit recommendations: fact_daily_prices is empty.")
    return latest_price_date


def _parse_stock_codes(raw_value: str) -> list[str] | None:
    stock_codes = [
        stock_code.strip().upper()
        for stock_code in raw_value.split(",")
        if stock_code.strip()
    ]
    return stock_codes or None


def main() -> None:
    args = _parse_args()
    logging.getLogger("sqlalchemy.engine").setLevel(logging.WARNING)

    profile = args.strategy_profile
    stock_codes = _parse_stock_codes(args.stocks)

    db = get_db()
    db.engine.echo = False
    with db.get_session() as session:
        recommendation_date = _resolve_recommendation_date(
            session,
            args.recommendation_date,
        )
        screener = StockScreener(session, strategy_profile=profile)
        if args.disable_probability:
            screener.probability_estimator = NullProbabilityEstimator()

        recommendations = screener.generate_recommendations(
            recommendation_date=recommendation_date,
            stock_codes=stock_codes,
            strategy_profile=profile,
            capture_audit=True,
        )
        portfolio_policy = ProductionPortfolioPolicy()
        open_positions = portfolio_policy.count_open_positions(
            session,
            recommendation_date=recommendation_date,
            profile=profile,
        )
        recommendations = portfolio_policy.apply(
            recommendations,
            existing_open_positions=open_positions,
        )
        screener.apply_portfolio_audit(recommendations)

        rec_repo = RecommendationRepository(session)
        audit_rows = rec_repo.replace_audit_entries(
            recommendation_date=recommendation_date,
            profile=profile,
            audit_entries=screener.last_audit_entries,
            full_snapshot=stock_codes is None,
        )

        stage_counts = Counter(
            entry.stage_reached
            for entry in screener.last_audit_entries
        )
        rejection_counts = Counter(
            entry.rejection_reason
            for entry in screener.last_audit_entries
            if entry.rejection_reason
        )

    print(
        {
            "recommendation_date": recommendation_date.isoformat(),
            "profile": profile,
            "audit_rows": audit_rows,
            "recommendations": len(recommendations),
            "approved_recommendations": sum(
                1 for recommendation in recommendations
                if recommendation.portfolio_approved
            ),
            "stage_counts": dict(stage_counts),
            "rejection_counts": dict(rejection_counts),
            "probability_enabled": not args.disable_probability,
            "stock_codes": stock_codes or "ALL",
        }
    )


if __name__ == "__main__":
    main()
