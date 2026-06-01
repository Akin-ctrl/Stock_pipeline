#!/usr/bin/env python3
"""Synchronize stock sector classifications from versioned reference data.

The script is intentionally idempotent. It upserts sector dimension rows and
updates existing stocks to the mapped ``sector_id`` without creating fake stock
records. Re-running the same map should converge to zero changes.
"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path
from typing import Optional

from app.config.database import get_db
from app.models import DimSector, DimStock
from app.services.reference_data import StockSectorMapping, load_stock_sector_map
from app.utils.logger import get_logger

logger = get_logger("sync_stock_sectors")


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""

    parser = argparse.ArgumentParser(
        description="Idempotently sync dim_stocks.sector_id from the reference sector map."
    )
    parser.add_argument(
        "--map-path",
        type=Path,
        help="Optional CSV path. Defaults to the packaged NGX sector map.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate and report changes without writing to the database.",
    )
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Fail if an active stock is missing from the reference map.",
    )
    return parser.parse_args()


def sync_stock_sectors(
    sector_map: dict[str, StockSectorMapping],
    *,
    dry_run: bool = False,
    strict: bool = False,
) -> dict[str, object]:
    """Apply the reference map to ``dim_sectors`` and ``dim_stocks``."""

    db = get_db()
    db.engine.echo = False
    summary: dict[str, object] = {
        "mapped_stocks": len(sector_map),
        "sectors_created": 0,
        "stocks_updated": 0,
        "stocks_unchanged": 0,
        "stocks_missing_in_database": [],
        "active_stocks_missing_mapping": [],
        "dry_run": dry_run,
    }

    with db.get_session() as session:
        sectors_by_name = {
            sector.sector_name: sector
            for sector in session.query(DimSector).all()
        }
        stocks_by_code = {
            stock.stock_code.upper(): stock
            for stock in session.query(DimStock).all()
        }

        for stock_code, mapping in sorted(sector_map.items()):
            sector = sectors_by_name.get(mapping.sector_name)
            if sector is None:
                sector = DimSector(
                    sector_name=mapping.sector_name,
                    description=f"{mapping.sector_name} sector",
                )
                if not dry_run:
                    session.add(sector)
                    session.flush()
                sectors_by_name[mapping.sector_name] = sector
                summary["sectors_created"] = int(summary["sectors_created"]) + 1

            stock = stocks_by_code.get(stock_code)
            if stock is None:
                missing = summary["stocks_missing_in_database"]
                assert isinstance(missing, list)
                missing.append(stock_code)
                continue

            if stock.sector_id == sector.sector_id:
                summary["stocks_unchanged"] = int(summary["stocks_unchanged"]) + 1
                continue

            if not dry_run:
                stock.sector_id = sector.sector_id
            summary["stocks_updated"] = int(summary["stocks_updated"]) + 1

        active_missing = [
            stock_code
            for stock_code, stock in sorted(stocks_by_code.items())
            if stock.is_active and stock_code not in sector_map
        ]
        summary["active_stocks_missing_mapping"] = active_missing

        if strict and active_missing:
            raise RuntimeError(
                "Active stocks missing sector mapping: "
                + ", ".join(active_missing)
            )

        if dry_run:
            session.rollback()

    return summary


def main() -> None:
    """CLI entrypoint."""

    logging.getLogger("sqlalchemy.engine").setLevel(logging.WARNING)
    args = parse_args()
    sector_map = load_stock_sector_map(args.map_path)
    summary = sync_stock_sectors(
        sector_map,
        dry_run=args.dry_run,
        strict=args.strict,
    )
    logger.info("Stock sector sync completed", extra=summary)


if __name__ == "__main__":
    main()
