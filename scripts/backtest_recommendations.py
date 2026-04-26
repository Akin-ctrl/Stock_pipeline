#!/usr/bin/env python3
"""Run a historical backtest of the recommendation engine."""

from __future__ import annotations

import argparse
import json
import logging
from datetime import date

from app.config.database import get_db
from app.services.backtesting import RecommendationBacktester


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Backtest recommendation engine on historical close prices")
    parser.add_argument("--start-date", required=True, help="Start date in YYYY-MM-DD format")
    parser.add_argument("--end-date", required=True, help="End date in YYYY-MM-DD format")
    parser.add_argument("--horizon-days", type=int, default=5, help="Trading-day holding period")
    parser.add_argument("--stocks", default="", help="Comma-separated stock codes; blank = all active stocks")
    parser.add_argument("--min-score", type=float, default=None, help="Minimum recommendation score")
    parser.add_argument("--min-confidence", type=float, default=None, help="Minimum recommendation confidence")
    parser.add_argument("--strategy-profile", default="steady_20p_10d", choices=["steady_20p_10d"], help="Recommendation profile")
    parser.add_argument("--round-trip-cost-pct", type=float, default=0.20, help="Estimated total transaction cost in percent")
    parser.add_argument("--include-hold", action="store_true", help="Include HOLD signals in the evaluation")
    parser.add_argument("--summary-only", action="store_true", help="Print only aggregate metrics (omit full trade list)")
    return parser.parse_args()


def main() -> None:
    logging.getLogger("sqlalchemy.engine").setLevel(logging.WARNING)
    logging.getLogger("stock_screener").setLevel(logging.WARNING)
    args = parse_args()
    start_date = date.fromisoformat(args.start_date)
    end_date = date.fromisoformat(args.end_date)
    stock_codes = [code.strip().upper() for code in args.stocks.split(",") if code.strip()] or None

    db = get_db()
    with db.get_session() as session:
        backtester = RecommendationBacktester(
            session=session,
            strategy_profile=args.strategy_profile,
            round_trip_cost_pct=args.round_trip_cost_pct,
        )
        result = backtester.run(
            start_date=start_date,
            end_date=end_date,
            horizon_days=args.horizon_days,
            stock_codes=stock_codes,
            min_score=args.min_score,
            min_confidence=args.min_confidence,
            include_hold=args.include_hold,
        )

    payload = result.to_dict()
    if args.summary_only:
        payload.pop("trades", None)
    print(json.dumps(payload, indent=2, default=str))


if __name__ == "__main__":
    main()
