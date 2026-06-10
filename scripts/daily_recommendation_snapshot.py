#!/usr/bin/env python3
"""Verify daily steady-profile recommendations for dashboard views."""

from __future__ import annotations

import argparse
from datetime import date
import logging

from app.config.database import get_db
from app.models import FactDailyPrice, FactRecommendation
from app.services.advisory.advisor import RecommendationProfile
from sqlalchemy import desc, func


def _parse_iso_date(raw_value: str) -> date:
    try:
        return date.fromisoformat(raw_value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            f"Expected YYYY-MM-DD date, got {raw_value!r}"
        ) from exc


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Capture daily steady-profile recommendations for dashboards."
    )
    parser.add_argument(
        "--snapshot-date",
        "--market-date",
        dest="snapshot_date",
        type=_parse_iso_date,
        help="Market/recommendation date to snapshot, formatted as YYYY-MM-DD.",
    )
    return parser.parse_args()


def _resolve_snapshot_date(session, requested_date: date | None) -> date:
    if requested_date is not None:
        return requested_date

    latest_recommendation_date = session.query(
        func.max(FactRecommendation.recommendation_date)
    ).scalar()
    if latest_recommendation_date is not None:
        return latest_recommendation_date

    latest_price_date = session.query(func.max(FactDailyPrice.price_date)).scalar()
    if latest_price_date is not None:
        return latest_price_date

    return date.today()


def main() -> None:
    args = _parse_args()
    logging.getLogger("sqlalchemy.engine").setLevel(logging.WARNING)
    logging.getLogger("stock_screener").setLevel(logging.WARNING)

    profile = RecommendationProfile.STEADY_20P_10D.value

    db = get_db()
    db.engine.echo = False
    with db.get_session() as session:
        snapshot_date = _resolve_snapshot_date(session, args.snapshot_date)
        recommendations = (
            session.query(FactRecommendation)
            .filter(
                FactRecommendation.recommendation_date == snapshot_date,
                FactRecommendation.profile == profile,
                FactRecommendation.portfolio_approved.is_(True),
            )
            .order_by(
                FactRecommendation.portfolio_rank.asc().nullslast(),
                desc(FactRecommendation.predicted_probability_10d_up),
                desc(FactRecommendation.heuristic_score),
                desc(FactRecommendation.signal_agreement),
            )
            .limit(10)
            .all()
        )

    print(
        {
            "snapshot_date": snapshot_date.isoformat(),
            "profile": profile,
            "recommendations": len(recommendations),
            "source": "fact_recommendations",
        }
    )


if __name__ == "__main__":
    main()
