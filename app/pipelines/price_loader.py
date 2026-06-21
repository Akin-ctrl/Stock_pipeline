"""
Stock dimension maintenance and fact_daily_prices promotion.

PriceLoader owns:
- dim_stocks upserts (load_stocks)
- fact_daily_prices inserts/updates (load_prices)
- Quality scoring, freshness, and field derivation for each price row

Split from PipelineOrchestrator so that price loading logic can evolve
independently and be unit-tested without standing up the full orchestrator.
"""

from datetime import date, datetime
from typing import Any, Dict, List, Optional

import pandas as pd

from app.repositories import StockRepository, PriceRepository
from app.models import DimSector, DimStock
from app.services.reference_data import (
    UNKNOWN_SECTOR_NAME,
    choose_sector_name,
    is_unknown_sector,
    load_stock_sector_map,
)
from app.utils import get_logger


def _to_none(value: Any) -> Any:
    """Convert pandas NA / float NaN to Python None so psycopg2 can bind them as SQL NULL."""
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    return value


class PriceLoader:
    """
    Loads stock dimension rows and fact price rows into the database.

    Responsibilities:
    - Upsert dim_stocks with sector resolution from curated reference data
    - Derive missing percentage-change fields from batch and DB history
    - Bulk-insert fact_daily_prices with quality metrics

    Errors and warnings are appended to the shared lists supplied at
    construction time so the owning orchestrator can collect them centrally.
    """

    def __init__(
        self,
        db,
        batch_size: int,
        use_staging: bool,
        errors: List[str],
        warnings: List[str],
        stage_times: Dict[str, float],
    ):
        self.db = db
        self.batch_size = batch_size
        self.use_staging = use_staging
        self.logger = get_logger("price_loader")
        self.errors = errors
        self.warnings = warnings
        self.stage_times = stage_times

    # ------------------------------------------------------------------
    # Public API (called by PipelineOrchestrator and Airflow tasks)
    # ------------------------------------------------------------------

    def load_stocks(self, data: pd.DataFrame) -> int:
        """Upsert dim_stocks rows derived from the fetched DataFrame.

        Sector assignment prefers the curated reference map; falling back to
        the source-provided sector name; and finally UNKNOWN if neither is
        available. An existing known sector is never downgraded to UNKNOWN.

        Args:
            data: DataFrame containing stock_code, company_name, exchange
                  and (optionally) sector columns.

        Returns:
            Number of stock rows created or updated.
        """
        stage_start = datetime.now()
        self.logger.info("Stage 4: Loading stocks")
        loaded_count = 0

        try:
            with self.db.get_session() as session:
                stock_repo = StockRepository(session)
                try:
                    sector_map = load_stock_sector_map()
                except FileNotFoundError as exc:
                    sector_map = {}
                    self.logger.warning(
                        "Stock sector reference map unavailable; falling back to source sectors",
                        extra={"error": str(exc)},
                    )

                def get_or_create_sector(sector_name: str) -> DimSector:
                    sector = session.query(DimSector).filter(
                        DimSector.sector_name == sector_name
                    ).first()
                    if sector:
                        return sector
                    sector = DimSector(
                        sector_name=sector_name,
                        description=f"{sector_name} sector",
                    )
                    session.add(sector)
                    session.flush()
                    return sector

                # Note: Afrimarket does not currently provide reliable sector
                # metadata, so curated reference data protects master data from
                # being degraded back to Unknown during daily runs.
                required_cols = ['stock_code', 'company_name', 'exchange']
                if 'sector' not in data.columns:
                    unique_stocks = data[required_cols].drop_duplicates()
                    unique_stocks = unique_stocks.copy()
                    unique_stocks['sector'] = None
                else:
                    unique_stocks = data[required_cols + ['sector']].drop_duplicates()

                for _, row in unique_stocks.iterrows():
                    try:
                        stock = stock_repo.get_by_code(row['stock_code'])

                        if stock:
                            update_values = {}
                            if stock.company_name != row['company_name']:
                                update_values["company_name"] = row['company_name']

                            existing_sector_name = (
                                stock.sector.sector_name if stock.sector else None
                            )
                            sector_name = choose_sector_name(
                                row['stock_code'],
                                sector_map,
                                existing_sector_name=existing_sector_name,
                                source_sector_name=row.get('sector'),
                            )
                            if (
                                is_unknown_sector(existing_sector_name)
                                and not is_unknown_sector(sector_name)
                            ):
                                sector = get_or_create_sector(sector_name)
                                if stock.sector_id != sector.sector_id:
                                    update_values["sector_id"] = sector.sector_id

                            if update_values:
                                stock_repo.update(stock, **update_values)
                                loaded_count += 1
                        else:
                            sector_name = choose_sector_name(
                                row['stock_code'],
                                sector_map,
                                source_sector_name=row.get('sector'),
                            )
                            sector = get_or_create_sector(
                                sector_name or UNKNOWN_SECTOR_NAME
                            )
                            stock_repo.create_stock(
                                stock_code=row['stock_code'],
                                company_name=row['company_name'],
                                sector_id=sector.sector_id,
                                exchange=row['exchange'],
                            )
                            loaded_count += 1

                    except Exception as exc:
                        msg = f"Failed to load stock {row['stock_code']}: {str(exc)}"
                        self.logger.warning(msg)
                        self.warnings.append(msg)

                session.commit()

            self.logger.info(
                f"Loaded {loaded_count} stocks",
                extra={"loaded": loaded_count},
            )
            self.stage_times['load_stocks'] = (datetime.now() - stage_start).total_seconds()
            return loaded_count

        except Exception as exc:
            self.errors.append(f"Stock loading error: {str(exc)}")
            self.logger.error(f"Stock loading failed: {str(exc)}")
            return 0

    def load_prices(self, data: pd.DataFrame) -> int:
        """Promote transformed price rows into fact_daily_prices.

        Derives missing change_1d_pct / change_ytd_pct values, scores each row
        for data quality, then bulk-inserts in batches with a single commit at
        the end so no partial state is left on failure.

        Note: is_official is permanently False until an official NGX end-of-day
        data source is integrated (no such source is available yet).

        Args:
            data: Transformed DataFrame ready for fact promotion.

        Returns:
            Number of price rows inserted or updated.

        Raises:
            RuntimeError: If input is non-empty but zero prices are inserted.
        """
        stage_start = datetime.now()
        self.logger.info("Stage 5: Loading prices")
        loaded_count = 0

        try:
            data = self._calculate_missing_changes(data)

            with self.db.get_session() as session:
                price_repo = PriceRepository(session)
                stock_rows = (
                    session.query(DimStock.stock_code, DimStock.stock_id)
                    .filter(DimStock.stock_code.in_(data['stock_code'].dropna().unique().tolist()))
                    .all()
                )
                stock_map = {
                    str(stock_code).upper(): stock_id
                    for stock_code, stock_id in stock_rows
                }

                for i in range(0, len(data), self.batch_size):
                    batch = data.iloc[i:i + self.batch_size]
                    price_records = []

                    for _, row in batch.iterrows():
                        try:
                            stock_id = stock_map.get(str(row['stock_code']).upper())
                            if not stock_id:
                                self.warnings.append(f"Stock not found: {row['stock_code']}")
                                continue

                            if pd.isna(row.get('close_price')):
                                msg = f"Skipping {row['stock_code']} on {row.get('price_date')}: null close_price"
                                self.logger.warning(msg)
                                self.warnings.append(msg)
                                continue

                            quality_flag = self._derive_quality_flag(row)
                            complete_data = all([
                                pd.notna(row.get('close_price')),
                                pd.notna(row.get('change_1d_pct')),
                                pd.notna(row.get('change_ytd_pct')),
                            ])
                            quality = self._build_quality_metrics(
                                price_date=row.get("price_date"),
                                quality_flag=quality_flag,
                                volume=row.get("volume"),
                                change_1d_pct=row.get("change_1d_pct"),
                                change_ytd_pct=row.get("change_ytd_pct"),
                            )

                            price_records.append({
                                'stock_id': stock_id,
                                'price_date': row['price_date'],
                                'close_price': row['close_price'],
                                'volume': _to_none(row.get('volume')),
                                'change_1d_pct': _to_none(row.get('change_1d_pct')),
                                'change_ytd_pct': _to_none(row.get('change_ytd_pct')),
                                'source': row.get('source', 'unknown'),
                                'source_count': 1,
                                'bar_status': 'RECONCILED' if self.use_staging else 'OBSERVED',
                                'is_official': False,
                                'confidence_score': quality["confidence_score"],
                                'data_quality_flag': quality_flag,
                                'has_complete_data': complete_data,
                            })

                        except Exception as exc:
                            msg = f"Failed to prepare price for {row['stock_code']}: {str(exc)}"
                            self.logger.warning(msg)
                            self.warnings.append(msg)

                    if price_records:
                        batch_count = price_repo.bulk_insert_prices(price_records)
                        loaded_count += batch_count

                    self.logger.info(
                        f"Loaded batch {i // self.batch_size + 1}: {len(price_records)} prices",
                        extra={"batch": i // self.batch_size + 1},
                    )

                # Single commit after all batches succeed — prevents partial commits
                # on failure that would leave the DB in an inconsistent state.
                session.commit()

            self.logger.info(
                f"Loaded {loaded_count} prices",
                extra={"loaded": loaded_count},
            )
            self.stage_times['load_prices'] = (datetime.now() - stage_start).total_seconds()

            if loaded_count == 0 and len(data) > 0:
                raise RuntimeError(
                    f"Price loading failed: 0 prices loaded from {len(data)} input records"
                )

            return loaded_count

        except Exception as exc:
            self.errors.append(f"Price loading error: {str(exc)}")
            self.logger.error("Price loading failed", error=exc)
            raise

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _calculate_missing_changes(self, data: pd.DataFrame) -> pd.DataFrame:
        """Derive change_1d_pct and change_ytd_pct for rows where these are absent.

        For sources that supply only absolute price movement (like Afrimarket
        current quotes), we always recalculate from first principles rather
        than trust the source value.

        Strategy (in order):
        1. Calculate from other rows in the same batch.
        2. Look up the previous/year-start close from fact_daily_prices history.
        3. Default any remaining gaps to 0.0.
        """
        df = data.copy()
        if df.empty:
            return df

        if not pd.api.types.is_datetime64_any_dtype(df['price_date']):
            df['price_date'] = pd.to_datetime(df['price_date'])

        df = df.sort_values(['stock_code', 'price_date']).copy()

        # Recalculate changes for sources that do not provide trustworthy
        # percentage fields. Afrimarket current quotes expose absolute change,
        # not percentage change, so we should always derive the percentages.
        untrusted_pct_sources = df['source'].fillna('').str.lower().isin({'afrimarket'})

        if 'change_1d_pct' not in df.columns:
            df['change_1d_pct'] = pd.NA
        if 'change_ytd_pct' not in df.columns:
            df['change_ytd_pct'] = pd.NA

        batch_prev_close = df.groupby('stock_code')['close_price'].shift(1)
        batch_change_1d = ((df['close_price'] - batch_prev_close) / batch_prev_close) * 100
        recalc_1d_mask = df['change_1d_pct'].isna() | untrusted_pct_sources
        df.loc[recalc_1d_mask, 'change_1d_pct'] = batch_change_1d[recalc_1d_mask].round(4)

        df['calc_year'] = df['price_date'].dt.year
        batch_year_start = df.groupby(['stock_code', 'calc_year'])['close_price'].transform('first')
        batch_change_ytd = ((df['close_price'] - batch_year_start) / batch_year_start) * 100
        recalc_ytd_mask = df['change_ytd_pct'].isna() | untrusted_pct_sources
        df.loc[recalc_ytd_mask, 'change_ytd_pct'] = batch_change_ytd[recalc_ytd_mask].round(4)

        # When the batch only contains the first observed row for a stock-year,
        # a current-only rerun will incorrectly treat "today" as the YTD
        # baseline. Force those rows back through DB-history lookup.
        batch_year_position = df.groupby(['stock_code', 'calc_year']).cumcount()
        needs_ytd_history = recalc_ytd_mask & untrusted_pct_sources & (batch_year_position == 0)
        df.loc[needs_ytd_history, 'change_ytd_pct'] = pd.NA

        rows_needing_history = df[
            (recalc_1d_mask & df['change_1d_pct'].isna()) |
            (recalc_ytd_mask & df['change_ytd_pct'].isna())
        ]
        if not rows_needing_history.empty:
            with self.db.get_session() as session:
                stock_repo = StockRepository(session)
                price_repo = PriceRepository(session)
                stock_cache: Dict[str, Optional[int]] = {}

                for idx, row in rows_needing_history.iterrows():
                    stock_code = row['stock_code']
                    if stock_code not in stock_cache:
                        stock = stock_repo.get_by_code(stock_code)
                        stock_cache[stock_code] = stock.stock_id if stock else None
                    stock_id = stock_cache[stock_code]
                    if stock_id is None:
                        continue

                    price_date = row['price_date'].date()

                    if recalc_1d_mask.loc[idx] and pd.isna(df.loc[idx, 'change_1d_pct']):
                        previous_price = price_repo.get_previous_price(stock_id, price_date)
                        if previous_price and previous_price.close_price:
                            prev_close = float(previous_price.close_price)
                            if prev_close > 0:
                                change_1d = ((float(row['close_price']) - prev_close) / prev_close) * 100
                                df.loc[idx, 'change_1d_pct'] = round(change_1d, 4)

                    if recalc_ytd_mask.loc[idx] and pd.isna(df.loc[idx, 'change_ytd_pct']):
                        year_start_price = price_repo.get_first_price_of_year(
                            stock_id,
                            row['calc_year'],
                            through_date=price_date,
                        )
                        if year_start_price and year_start_price.close_price:
                            baseline = float(year_start_price.close_price)
                            if baseline > 0:
                                change_ytd = ((float(row['close_price']) - baseline) / baseline) * 100
                                df.loc[idx, 'change_ytd_pct'] = round(change_ytd, 4)

        # Leave remaining NaN values as NULL rather than substituting 0.0.
        # A NULL change is correctly flagged as INCOMPLETE quality; a 0.0 would
        # appear as a genuine flat-day and pass downstream eligibility filters.
        df = df.drop(columns=['calc_year'])
        return df

    def _derive_quality_flag(self, row: pd.Series) -> str:
        """Assign GOOD / INCOMPLETE / POOR quality flag to a single price row."""
        if pd.isna(row.get("close_price")):
            return "POOR"
        has_complete = all([
            pd.notna(row.get("close_price")),
            pd.notna(row.get("change_1d_pct")),
            pd.notna(row.get("change_ytd_pct")),
        ])
        return "GOOD" if has_complete else "INCOMPLETE"

    def _build_quality_metrics(
        self,
        price_date: Any,
        quality_flag: str,
        volume: Any,
        change_1d_pct: Any,
        change_ytd_pct: Any,
    ) -> Dict[str, Any]:
        """Build completeness, freshness and confidence metrics for a price row."""
        fields_present = {
            "close_price": True,
            "change_1d_pct": pd.notna(change_1d_pct),
            "change_ytd_pct": pd.notna(change_ytd_pct),
            "volume": pd.notna(volume),
        }
        completeness_score = round(
            (sum(1 for v in fields_present.values() if v) / len(fields_present)) * 100,
            2,
        )

        age_days = 0
        if price_date is not None:
            try:
                normalized_date = pd.to_datetime(price_date).date()
                age_days = max((date.today() - normalized_date).days, 0)
            except Exception:
                age_days = 0

        freshness_score = max(0.0, round(100 - min(age_days, 30) * 3, 2))

        confidence_map = {
            "GOOD": 85.0,
            "INCOMPLETE": 70.0,
            "SUSPICIOUS": 45.0,
            "MISSING": 30.0,
            "STALE": 30.0,
            "POOR": 20.0,
        }
        confidence_score = confidence_map.get(quality_flag, 50.0)
        anomaly_score = 60.0 if quality_flag == "SUSPICIOUS" else 0.0

        return {
            "completeness_score": completeness_score,
            "freshness_score": freshness_score,
            "confidence_score": confidence_score,
            "anomaly_score": anomaly_score,
            "quality_label": quality_flag,
            "field_coverage": fields_present,
            "notes": (
                f"Generated from {quality_flag.lower()} market data "
                f"during fact_daily_prices in-place redesign"
            ),
        }
