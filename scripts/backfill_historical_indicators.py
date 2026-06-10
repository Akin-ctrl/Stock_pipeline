#!/usr/bin/env python3
"""Backfill technical indicators from trusted fact_daily_prices.

This utility is intentionally idempotent: every computed stock/date indicator
row is upserted, so reruns repair stale values and converge to the same final
state without creating duplicates.
"""

from __future__ import annotations

import argparse
import logging
from datetime import date, datetime, timedelta
from typing import Iterable, Optional

from app.config.database import get_db
from app.models import Base
from app.repositories import IndicatorRepository, PriceRepository, StockRepository
from app.services.indicators import IndicatorCalculator


DEFAULT_WARMUP_CALENDAR_DAYS = 3650


def _parse_date(value: Optional[str]) -> Optional[date]:
    if not value:
        return None
    normalized = value.strip().lower()
    if normalized == "today":
        return date.today()
    if normalized == "yesterday":
        return date.today() - timedelta(days=1)
    return datetime.strptime(value, "%Y-%m-%d").date()


def _parse_stocks(value: str) -> Optional[list[str]]:
    stocks = [code.strip().upper() for code in value.split(",") if code.strip()]
    return stocks or None


def _history_start_for(
    start_date: Optional[date],
    warmup_calendar_days: int,
) -> Optional[date]:
    """Return a calendar warmup start that supports sparsely traded stocks."""
    if start_date is None:
        return None
    if warmup_calendar_days < 0:
        raise ValueError("warmup_calendar_days must be non-negative")
    return start_date - timedelta(days=warmup_calendar_days)


def _price_rows(prices: Iterable[object]) -> list[dict]:
    ascending_prices = sorted(prices, key=lambda price: price.price_date)
    return [
        {
            "price_date": price.price_date,
            "close_price": float(price.close_price),
        }
        for price in ascending_prices
    ]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Idempotently backfill fact_technical_indicators from trusted daily prices."
    )
    parser.add_argument("--start-date", help="First indicator date to persist, YYYY-MM-DD.")
    parser.add_argument("--end-date", help="Last indicator date to persist, YYYY-MM-DD.")
    parser.add_argument("--stocks", default="", help="Comma-separated stock codes. Defaults to all active stocks.")
    parser.add_argument("--min-confidence", type=float, default=60.0, help="Minimum price confidence score.")
    parser.add_argument("--require-complete", action="store_true", help="Require complete daily price rows.")
    parser.add_argument(
        "--warmup-calendar-days",
        type=int,
        default=DEFAULT_WARMUP_CALENDAR_DAYS,
        help=(
            "Calendar days of trusted price history to load before start-date. "
            "Sparse NGX names need a long warmup to reach the 90-price indicator window."
        ),
    )
    parser.add_argument("--dry-run", action="store_true", help="Calculate counts without writing rows.")
    return parser.parse_args()


def main() -> None:
    logging.getLogger("sqlalchemy.engine").setLevel(logging.WARNING)
    args = parse_args()
    start_date = _parse_date(args.start_date)
    end_date = _parse_date(args.end_date) or date.today()
    stock_codes = _parse_stocks(args.stocks)

    calculator = IndicatorCalculator()
    history_start = _history_start_for(start_date, args.warmup_calendar_days)

    db = get_db()
    db.engine.echo = False
    Base.metadata.create_all(db.engine)
    summary = {
        "stocks_seen": 0,
        "stocks_with_prices": 0,
        "computed_rows": 0,
        "rows_to_write": 0,
        "rows_written": 0,
        "dry_run": args.dry_run,
        "start_date": start_date.isoformat() if start_date else None,
        "end_date": end_date.isoformat(),
        "warmup_calendar_days": args.warmup_calendar_days,
    }

    with db.get_session() as session:
        stock_repo = StockRepository(session)
        price_repo = PriceRepository(session)
        indicator_repo = IndicatorRepository(session)

        if stock_codes:
            stocks = [stock_repo.get_by_code(code) for code in stock_codes]
            stocks = [stock for stock in stocks if stock is not None]
        else:
            stocks = stock_repo.get_all_active()

        summary["stocks_seen"] = len(stocks)

        for stock in stocks:
            prices = price_repo.get_trusted_price_history(
                stock.stock_id,
                start_date=history_start,
                end_date=end_date,
                min_confidence=args.min_confidence,
                require_complete=args.require_complete,
            )
            price_rows = _price_rows(prices)
            if len(price_rows) < calculator.minimum_history_required():
                continue

            summary["stocks_with_prices"] += 1
            indicators = calculator.calculate_for_stock(
                stock_id=stock.stock_id,
                stock_code=stock.stock_code,
                price_history=price_rows,
            )
            if start_date:
                indicators = [
                    row for row in indicators
                    if row["calculation_date"] >= start_date
                ]
            indicators = [
                row for row in indicators
                if row["calculation_date"] <= end_date
            ]
            summary["computed_rows"] += len(indicators)

            summary["rows_to_write"] += len(indicators)
            if args.dry_run or not indicators:
                continue

            written = indicator_repo.bulk_save_indicators(indicators)
            summary["rows_written"] += written
            session.commit()

    print(summary)


if __name__ == "__main__":
    main()
