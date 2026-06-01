from pathlib import Path

import pytest

from app.services.reference_data import choose_sector_name, load_stock_sector_map


def test_load_stock_sector_map_rejects_duplicate_codes(tmp_path: Path):
    sector_map = tmp_path / "sector_map.csv"
    sector_map.write_text(
        "stock_code,sector_name\n"
        "GTCO,Financial Services\n"
        "gtco,Financial Services\n",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="duplicate stock_code"):
        load_stock_sector_map(sector_map)


def test_choose_sector_name_preserves_existing_real_sector():
    sector_map = {
        "GTCO": load_stock_sector_map()[
            "GTCO"
        ],
    }

    sector_name = choose_sector_name(
        "GTCO",
        sector_map,
        existing_sector_name="Financial Services",
        source_sector_name="Unknown",
    )

    assert sector_name == "Financial Services"


def test_choose_sector_name_uses_reference_for_unknown_existing_sector():
    sector_map = load_stock_sector_map()

    sector_name = choose_sector_name(
        "DANGCEM",
        sector_map,
        existing_sector_name="Unknown",
        source_sector_name=None,
    )

    assert sector_name == "Industrial Goods"
