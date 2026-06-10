#!/usr/bin/env python3
"""Persist a weekly recommendation board from the candidate audit funnel."""

from __future__ import annotations

import argparse
from datetime import date, timedelta
import logging
from typing import Iterable

from sqlalchemy import desc, func

from app.config.database import get_db
from app.models import (
    DimSector,
    DimStock,
    FactDailyPrice,
    FactRecommendationAudit,
    WeeklyRecommendation,
)
from app.repositories.recommendation_repository import RecommendationRepository
from app.services.advisory import ProductionPortfolioPolicy, StockScreener
from app.services.modeling import NullProbabilityEstimator


ACTIONABLE_AUDIT_ACTIONS = {"BUY", "STRONG_BUY"}
WEEKLY_STATUS_PRIORITY = {
    "APPROVED": 1,
    "WATCHLIST": 2,
    "WAIT_FOR_PULLBACK": 3,
    "WAIT_FOR_VOLUME": 4,
    "HIGH_RISK_WATCHLIST": 5,
    "SPECULATIVE_WATCHLIST": 6,
}


def _parse_iso_date(raw_value: str) -> date:
    try:
        return date.fromisoformat(raw_value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            f"Expected YYYY-MM-DD date, got {raw_value!r}"
        ) from exc


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Persist weekly watchlist recommendations from audit rows."
    )
    parser.add_argument(
        "--week-date",
        "--market-date",
        dest="week_date",
        type=_parse_iso_date,
        help="Market date to use as the weekly board anchor. Defaults to latest price date.",
    )
    parser.add_argument(
        "--strategy-profile",
        default="steady_20p_10d",
        choices=["steady_20p_10d", "steady_20p_10d_v2"],
        help="Recommendation profile to use.",
    )
    parser.add_argument(
        "--top-n",
        type=int,
        default=15,
        help="Maximum weekly candidates to persist.",
    )
    parser.add_argument(
        "--min-score",
        type=float,
        default=68.0,
        help="Minimum heuristic score for weekly watchlist candidates.",
    )
    parser.add_argument(
        "--use-existing-audit",
        action="store_true",
        help="Do not refresh the candidate audit before building the weekly board.",
    )
    parser.add_argument(
        "--disable-probability",
        action="store_true",
        help="Skip probability-model training while refreshing audit rows.",
    )
    return parser.parse_args()


def _resolve_week_date(session, requested_date: date | None) -> date:
    if requested_date is not None:
        return requested_date

    latest_price_date = session.query(func.max(FactDailyPrice.price_date)).scalar()
    if latest_price_date is None:
        raise RuntimeError("Cannot build weekly recommendations: fact_daily_prices is empty.")
    return latest_price_date


def _week_start(week_end_date: date) -> date:
    return week_end_date - timedelta(days=week_end_date.weekday())


def _weekly_status(
    *,
    candidate_tier: str,
    rejection_reason: str | None,
) -> str:
    if candidate_tier == "approved":
        return "APPROVED"
    if rejection_reason in {
        "below_min_drawdown_20d",
        "above_max_price_change_20d",
        "outside_rsi_band",
    }:
        return "WAIT_FOR_PULLBACK"
    if rejection_reason == "below_min_volume_ratio":
        return "WAIT_FOR_VOLUME"
    if rejection_reason == "above_max_volatility":
        return "HIGH_RISK_WATCHLIST"
    if rejection_reason == "below_min_price":
        return "SPECULATIVE_WATCHLIST"
    return "WATCHLIST"


def _float_or_none(value) -> float | None:
    return float(value) if value is not None else None


def _rationale(row, weekly_status: str) -> list[str]:
    rationale = [
        f"Weekly status: {weekly_status}",
        f"Action signal: {row.action_type}",
        f"Heuristic score: {float(row.heuristic_score):.2f}",
    ]
    if row.signal_agreement is not None:
        rationale.append(f"Signal agreement: {float(row.signal_agreement):.2f}")
    if row.rejection_reason:
        rationale.append(f"Daily gate reason: {row.rejection_reason}")
    if row.price_change_20d is not None:
        rationale.append(f"20-day price change: {float(row.price_change_20d):.2f}%")
    if row.drawdown_20d_pct is not None:
        rationale.append(f"20-day drawdown: {float(row.drawdown_20d_pct):.2f}%")
    if row.volume_ratio is not None:
        rationale.append(f"Volume ratio: {float(row.volume_ratio):.2f}")
    return rationale


def _refresh_audit(
    session,
    *,
    recommendation_date: date,
    profile: str,
    disable_probability: bool,
) -> int:
    screener = StockScreener(session, strategy_profile=profile)
    if disable_probability:
        screener.probability_estimator = NullProbabilityEstimator()

    recommendations = screener.generate_recommendations(
        recommendation_date=recommendation_date,
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
    return rec_repo.replace_audit_entries(
        recommendation_date=recommendation_date,
        profile=profile,
        audit_entries=screener.last_audit_entries,
        full_snapshot=True,
    )


def _candidate_rows(
    session,
    *,
    recommendation_date: date,
    profile: str,
    min_score: float,
) -> list:
    rows = (
        session.query(
            FactRecommendationAudit,
            DimStock.stock_code,
            DimStock.company_name,
            DimSector.sector_name,
        )
        .join(DimStock, DimStock.stock_id == FactRecommendationAudit.stock_id)
        .outerjoin(DimSector, DimSector.sector_id == DimStock.sector_id)
        .filter(
            FactRecommendationAudit.recommendation_date == recommendation_date,
            FactRecommendationAudit.profile == profile,
            FactRecommendationAudit.candidate_tier.in_(("approved", "watchlist")),
            FactRecommendationAudit.action_type.in_(ACTIONABLE_AUDIT_ACTIONS),
            FactRecommendationAudit.heuristic_score >= min_score,
            FactRecommendationAudit.current_price.isnot(None),
        )
        .order_by(
            desc(FactRecommendationAudit.heuristic_score),
            desc(FactRecommendationAudit.signal_agreement),
        )
        .all()
    )

    return sorted(
        rows,
        key=lambda item: (
            -float(item[0].heuristic_score),
            WEEKLY_STATUS_PRIORITY[
                _weekly_status(
                    candidate_tier=item[0].candidate_tier,
                    rejection_reason=item[0].rejection_reason,
                )
            ],
            -float(item[0].signal_agreement or 0),
            item[1],
        ),
    )


def _replace_weekly_board(
    session,
    *,
    week_end_date: date,
    profile: str,
    rows: Iterable,
    top_n: int,
) -> int:
    week_start_date = _week_start(week_end_date)
    session.query(WeeklyRecommendation).filter(
        WeeklyRecommendation.week_end_date == week_end_date,
        WeeklyRecommendation.profile == profile,
    ).delete(synchronize_session=False)

    persisted = 0
    for rank, item in enumerate(list(rows)[:top_n], start=1):
        audit, stock_code, company_name, sector_name = item
        weekly_status = _weekly_status(
            candidate_tier=audit.candidate_tier,
            rejection_reason=audit.rejection_reason,
        )
        session.add(
            WeeklyRecommendation(
                week_start_date=week_start_date,
                week_end_date=week_end_date,
                recommendation_date=audit.recommendation_date,
                profile=profile,
                stock_id=audit.stock_id,
                stock_code=stock_code,
                company_name=company_name,
                sector_name=sector_name or "Unclassified",
                rank=rank,
                weekly_status=weekly_status,
                candidate_tier=audit.candidate_tier,
                action_type=audit.action_type,
                technical_signal_type=audit.technical_signal_type,
                rejection_reason=audit.rejection_reason,
                signal_agreement=audit.signal_agreement,
                heuristic_score=audit.heuristic_score,
                current_price=audit.current_price,
                rsi_14=audit.rsi_14,
                volatility=audit.volatility,
                volume_ratio=audit.volume_ratio,
                price_change_20d=audit.price_change_20d,
                drawdown_20d_pct=audit.drawdown_20d_pct,
                rationale=_rationale(audit, weekly_status),
            )
        )
        persisted += 1

    session.flush()
    return persisted


def main() -> None:
    args = _parse_args()
    if args.top_n <= 0:
        raise ValueError("--top-n must be positive")
    logging.getLogger("sqlalchemy.engine").setLevel(logging.WARNING)

    db = get_db()
    db.engine.echo = False
    with db.get_session() as session:
        week_date = _resolve_week_date(session, args.week_date)
        audit_rows = None
        if not args.use_existing_audit:
            audit_rows = _refresh_audit(
                session,
                recommendation_date=week_date,
                profile=args.strategy_profile,
                disable_probability=args.disable_probability,
            )

        candidates = _candidate_rows(
            session,
            recommendation_date=week_date,
            profile=args.strategy_profile,
            min_score=args.min_score,
        )
        weekly_rows = _replace_weekly_board(
            session,
            week_end_date=week_date,
            profile=args.strategy_profile,
            rows=candidates,
            top_n=args.top_n,
        )
        session.commit()

    print(
        {
            "week_start_date": _week_start(week_date).isoformat(),
            "week_end_date": week_date.isoformat(),
            "profile": args.strategy_profile,
            "audit_rows": audit_rows,
            "candidate_rows": len(candidates),
            "weekly_recommendations": weekly_rows,
            "min_score": args.min_score,
            "top_n": args.top_n,
            "probability_enabled": not args.disable_probability,
        }
    )


if __name__ == "__main__":
    main()
