#!/usr/bin/env python3
"""
Capture daily steady-profile recommendations for dashboards.
"""

from __future__ import annotations

from datetime import date
import logging

from app.config.database import get_db
from app.models import Base, DailyRecommendationSnapshot
from app.services.advisory.advisor import RecommendationProfile, StockScreener


def _as_float(value):
    return float(value) if value is not None else None


def main() -> None:
    logging.getLogger("sqlalchemy.engine").setLevel(logging.WARNING)
    logging.getLogger("stock_screener").setLevel(logging.WARNING)

    snapshot_date = date.today()
    profile = RecommendationProfile.STEADY_20P_10D.value

    db = get_db()
    Base.metadata.create_all(db.engine)
    with db.get_session() as session:
        session.query(DailyRecommendationSnapshot).filter(
            DailyRecommendationSnapshot.snapshot_date == snapshot_date,
            DailyRecommendationSnapshot.profile == profile,
        ).delete(synchronize_session=False)

        screener = StockScreener(session, strategy_profile=profile)
        recommendations = screener.generate_recommendations(
            recommendation_date=snapshot_date,
            strategy_profile=profile,
        )
        recommendations.sort(key=lambda rec: float(rec.score), reverse=True)

        rows = []
        for rec in recommendations[:10]:
            rows.append(
                DailyRecommendationSnapshot(
                    snapshot_date=snapshot_date,
                    profile=profile,
                    stock_code=rec.stock_code,
                    company_name=rec.stock_name,
                    signal_type=rec.signal_type.value,
                    confidence=_as_float(rec.confidence),
                    score=_as_float(rec.score),
                    current_price=_as_float(rec.current_price),
                    target_price=_as_float(rec.target_price),
                    stop_loss=_as_float(rec.stop_loss),
                    reasons=rec.reasons,
                )
            )

        session.add_all(rows)

    print(
        {
            "snapshot_date": snapshot_date.isoformat(),
            "profile": profile,
            "snapshots": len(recommendations[:10]),
        }
    )


if __name__ == "__main__":
    main()
