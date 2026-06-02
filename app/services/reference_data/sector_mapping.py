"""Load authoritative stock-to-sector reference data.

Afrimarket currently supplies prices but not reliable sector metadata. This
module keeps sector classification as an explicit, versioned reference-data
contract so ingestion, maintenance scripts, and dashboard rebuilds all use the
same source of truth.
"""

from __future__ import annotations

import csv
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

UNKNOWN_SECTOR_NAME = "Unknown"
UNKNOWN_SECTOR_ALIASES = {
    "",
    "unknown",
    "unclassified",
    "n/a",
    "na",
    "none",
    "null",
    "nan",
}
REQUIRED_COLUMNS = {"stock_code", "sector_name"}
PACKAGE_MAP_PATH = Path(__file__).with_name("ngx_stock_sector_map.csv")


@dataclass(frozen=True)
class StockSectorMapping:
    """Reference classification for one stock code."""

    stock_code: str
    sector_name: str
    sector_source: Optional[str] = None
    verified_on: Optional[str] = None


def is_unknown_sector(sector_name: Optional[object]) -> bool:
    """Return True when a sector value is empty or a known placeholder."""

    if sector_name is None:
        return True
    return str(sector_name).strip().lower() in UNKNOWN_SECTOR_ALIASES


def choose_sector_name(
    stock_code: str,
    sector_map: dict[str, StockSectorMapping],
    existing_sector_name: Optional[str] = None,
    source_sector_name: Optional[str] = None,
) -> str:
    """Choose the best sector while protecting existing real classifications.

    Existing non-placeholder sectors win during daily ingestion so a source
    outage or missing source metadata cannot degrade curated master data back
    to ``Unknown``. The explicit sync script may still apply the reference map
    as authoritative when we intentionally refresh classifications.
    """

    if not is_unknown_sector(existing_sector_name):
        return str(existing_sector_name).strip()

    mapping = sector_map.get(stock_code.strip().upper())
    if mapping and not is_unknown_sector(mapping.sector_name):
        return mapping.sector_name

    if not is_unknown_sector(source_sector_name):
        return str(source_sector_name).strip()

    return UNKNOWN_SECTOR_NAME


def load_stock_sector_map(path: Optional[Path | str] = None) -> dict[str, StockSectorMapping]:
    """Load the stock-sector mapping CSV keyed by uppercase stock code."""

    csv_path = _resolve_mapping_path(path)
    mappings: dict[str, StockSectorMapping] = {}

    with csv_path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        fieldnames = set(reader.fieldnames or [])
        missing_columns = REQUIRED_COLUMNS - fieldnames
        if missing_columns:
            missing = ", ".join(sorted(missing_columns))
            raise ValueError(f"Sector map is missing required columns: {missing}")

        for line_number, row in enumerate(reader, start=2):
            stock_code = (row.get("stock_code") or "").strip().upper()
            sector_name = (row.get("sector_name") or "").strip()
            if not stock_code:
                raise ValueError(f"Sector map row {line_number} has an empty stock_code")
            if is_unknown_sector(sector_name):
                raise ValueError(f"Sector map row {line_number} has an invalid sector_name")
            if stock_code in mappings:
                raise ValueError(f"Sector map contains duplicate stock_code: {stock_code}")

            mappings[stock_code] = StockSectorMapping(
                stock_code=stock_code,
                sector_name=sector_name,
                sector_source=(row.get("sector_source") or "").strip() or None,
                verified_on=(row.get("verified_on") or "").strip() or None,
            )

    return mappings


def _resolve_mapping_path(path: Optional[Path | str]) -> Path:
    """Resolve mapping path from explicit input, env var, or packaged CSV."""

    candidates: list[Path] = []
    if path:
        candidates.append(Path(path))

    env_path = os.getenv("STOCK_SECTOR_MAP_PATH")
    if env_path:
        candidates.append(Path(env_path))

    candidates.append(PACKAGE_MAP_PATH)

    for candidate in candidates:
        if candidate.exists():
            return candidate

    expected_paths = ", ".join(str(candidate) for candidate in candidates)
    raise FileNotFoundError(f"Stock sector map not found. Checked: {expected_paths}")
