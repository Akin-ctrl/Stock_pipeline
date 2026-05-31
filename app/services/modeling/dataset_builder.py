"""Build reproducible stock-date modeling datasets from canonical market facts."""

from __future__ import annotations

from collections import Counter
from dataclasses import asdict, dataclass, field
from datetime import date
from typing import Dict, Iterable, List, Optional, Sequence

from app.models import DimStock, FactDailyPrice, FactTechnicalIndicator
from app.repositories import IndicatorRepository, PriceRepository, StockRepository
from app.services.modeling.feature_engineering import (
    PROBABILITY_FEATURE_NAMES,
    build_historical_feature_snapshot,
    to_float,
)
from app.services.modeling.targets import (
    DEFAULT_DIRECTION_TARGET,
    DirectionTargetDefinition,
    build_forward_return_label,
)
from app.utils import get_logger


@dataclass(frozen=True)
class ModelingDatasetConfig:
    """Configuration for building the first canonical direction dataset."""

    target_definition: DirectionTargetDefinition = DEFAULT_DIRECTION_TARGET
    allowed_bar_statuses: Sequence[str] = field(
        default_factory=lambda: tuple(PriceRepository.TRUSTED_BAR_STATUSES)
    )
    allowed_quality_flags: Sequence[str] = field(
        default_factory=lambda: tuple(PriceRepository.TRUSTED_QUALITY_FLAGS)
    )
    require_complete_data: bool = True
    require_official: bool = False
    min_confidence_score: Optional[float] = None
    max_abs_anchor_return_pct: Optional[float] = 50.0
    max_abs_forward_return_pct: Optional[float] = 50.0
    required_indicator_fields: Sequence[str] = field(
        default_factory=tuple
    )


@dataclass(frozen=True)
class ModelingDatasetRow:
    """One model-ready stock-date row anchored on canonical end-of-day data."""

    stock_id: int
    stock_code: str
    anchor_date: date
    horizon_date: date
    close_price: float
    horizon_close_price: float
    volume: Optional[int]
    change_1d_pct: Optional[float]
    change_ytd_pct: Optional[float]
    price_confidence_score: Optional[float]
    price_quality_flag: str
    bar_status: str
    has_complete_data: bool
    is_official: bool
    trusted_history_days: int
    volume_ratio: Optional[float]
    price_change_pct: Optional[float]
    price_change_3d: Optional[float]
    price_change_5d: Optional[float]
    price_change_10d: Optional[float]
    price_change_20d: Optional[float]
    price_change_30d: Optional[float]
    price_change_60d: Optional[float]
    ma_7: Optional[float]
    ma_30: Optional[float]
    ma_90: Optional[float]
    close_vs_ma_7_pct: float
    close_vs_ma_30_pct: float
    close_vs_ma_90_pct: float
    ma_7_vs_ma_30_pct: float
    ma_30_vs_ma_90_pct: float
    close_vs_20d_high_pct: float
    close_vs_60d_high_pct: float
    close_vs_20d_low_pct: float
    close_vs_60d_low_pct: float
    drawdown_20d_pct: float
    drawdown_60d_pct: float
    rebound_20d_pct: float
    rebound_60d_pct: float
    volatility_10d: float
    volatility_20d: float
    downside_volatility_20d: float
    average_volume_20d: Optional[float]
    volume_trend_ratio: Optional[float]
    rsi_14: Optional[float]
    macd: Optional[float]
    macd_signal: Optional[float]
    macd_histogram: Optional[float]
    volatility_30: Optional[float]
    atr_14: Optional[float]
    bollinger_upper: Optional[float]
    bollinger_middle: Optional[float]
    bollinger_lower: Optional[float]
    ma_crossover_signal: Optional[str]
    trend_strength: Optional[float]
    target_up_10d: int
    forward_return_10d: float

    def to_dict(self) -> Dict[str, object]:
        """Serialize the row to a plain dictionary."""
        return asdict(self)


@dataclass(frozen=True)
class ModelingDatasetSummary:
    """Validation summary for a generated modeling dataset."""

    row_count: int
    stock_count: int
    feature_count: int
    date_start: Optional[date]
    date_end: Optional[date]
    positive_rate: float
    duplicate_row_count: int
    missing_value_counts: Dict[str, int]
    all_null_fields: tuple[str, ...]
    constant_value_fields: Dict[str, object]
    constant_probability_features: Dict[str, float]


class ModelingDatasetBuilder:
    """Build the canonical 10-trading-day direction dataset."""

    def __init__(self, session, config: Optional[ModelingDatasetConfig] = None):
        self.session = session
        self.config = config or ModelingDatasetConfig()
        self.logger = get_logger("modeling_dataset_builder")
        self.stock_repo = StockRepository(session)
        self.price_repo = PriceRepository(session)
        self.indicator_repo = IndicatorRepository(session)

    def build(
        self,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        stock_codes: Optional[Sequence[str]] = None,
    ) -> List[ModelingDatasetRow]:
        """Build model-ready rows for the configured target definition."""
        stocks = self._resolve_stocks(stock_codes)
        rows: List[ModelingDatasetRow] = []

        for stock in stocks:
            rows.extend(
                self._build_rows_for_stock(
                    stock=stock,
                    start_date=start_date,
                    end_date=end_date,
                )
            )

        self.logger.info(
            "Built modeling dataset",
            extra={
                "rows": len(rows),
                "stocks": len(stocks),
                "horizon_trading_days": self.config.target_definition.horizon_trading_days,
            },
        )
        return rows

    def summarize(self, rows: Sequence[ModelingDatasetRow]) -> ModelingDatasetSummary:
        """Produce a structural summary for validation and inspection."""
        if not rows:
            return ModelingDatasetSummary(
                row_count=0,
                stock_count=0,
                feature_count=len(PROBABILITY_FEATURE_NAMES),
                date_start=None,
                date_end=None,
                positive_rate=0.0,
                duplicate_row_count=0,
                missing_value_counts={},
                all_null_fields=(),
                constant_value_fields={},
                constant_probability_features={},
            )

        row_dicts = [row.to_dict() for row in rows]
        missing_counts: Counter[str] = Counter()
        row_keys = Counter((row.stock_id, row.anchor_date) for row in rows)
        constant_value_fields: Dict[str, object] = {}
        constant_probability_features: Dict[str, float] = {}

        for row_dict in row_dicts:
            for key, value in row_dict.items():
                if value is None:
                    missing_counts[key] += 1

        for key in row_dicts[0].keys():
            values = [row_dict[key] for row_dict in row_dicts]
            if all(value == values[0] for value in values):
                constant_value_fields[key] = values[0]
                if key in PROBABILITY_FEATURE_NAMES and values[0] is not None:
                    constant_probability_features[key] = float(values[0])

        anchors = [row.anchor_date for row in rows]
        positive_rate = sum(row.target_up_10d for row in rows) / len(rows)
        all_null_fields = tuple(
            key
            for key, missing_count in sorted(missing_counts.items())
            if missing_count == len(rows)
        )

        return ModelingDatasetSummary(
            row_count=len(rows),
            stock_count=len({row.stock_id for row in rows}),
            feature_count=len(PROBABILITY_FEATURE_NAMES),
            date_start=min(anchors),
            date_end=max(anchors),
            positive_rate=positive_rate,
            duplicate_row_count=sum(count - 1 for count in row_keys.values() if count > 1),
            missing_value_counts=dict(sorted(missing_counts.items())),
            all_null_fields=all_null_fields,
            constant_value_fields=dict(sorted(constant_value_fields.items())),
            constant_probability_features=dict(sorted(constant_probability_features.items())),
        )

    def _resolve_stocks(self, stock_codes: Optional[Sequence[str]]) -> List[DimStock]:
        """Resolve the stock universe for dataset construction."""
        if stock_codes:
            resolved = [self.stock_repo.get_by_code(stock_code) for stock_code in stock_codes]
            return [stock for stock in resolved if stock is not None]
        return self.stock_repo.get_all_active()

    def _build_rows_for_stock(
        self,
        stock: DimStock,
        start_date: Optional[date],
        end_date: Optional[date],
    ) -> List[ModelingDatasetRow]:
        """Build model-ready rows for one stock."""
        prices_desc = self.price_repo.get_price_history(
            stock.stock_id,
            start_date=start_date,
            end_date=end_date,
        )
        eligible_prices = list(reversed([price for price in prices_desc if self._is_price_eligible(price)]))

        if len(eligible_prices) <= self.config.target_definition.horizon_trading_days:
            return []

        indicator_lookup = self._load_indicator_lookup(
            stock.stock_id,
            start_date=min(price.price_date for price in eligible_prices),
            end_date=max(price.price_date for price in eligible_prices),
        )

        rows: List[ModelingDatasetRow] = []
        horizon_offset = self.config.target_definition.horizon_trading_days

        for anchor_index, anchor_price in enumerate(eligible_prices[:-horizon_offset]):
            indicator = indicator_lookup.get(anchor_price.price_date)
            if indicator is None or not self._has_required_indicator_fields(indicator):
                continue
            if self._has_extreme_anchor_return(anchor_price, anchor_index, eligible_prices):
                continue

            horizon_price = eligible_prices[anchor_index + horizon_offset]
            label = build_forward_return_label(
                anchor_date=anchor_price.price_date,
                anchor_close_price=anchor_price.close_price,
                horizon_date=horizon_price.price_date,
                horizon_close_price=horizon_price.close_price,
                target_definition=self.config.target_definition,
            )
            if self._has_extreme_forward_return(label.forward_return_pct):
                continue

            rows.append(
                self._build_dataset_row(
                    stock=stock,
                    anchor_price=anchor_price,
                    horizon_price=horizon_price,
                    anchor_index=anchor_index,
                    price_history=eligible_prices,
                    indicator=indicator,
                    target_up=label.target_up,
                    forward_return_pct=label.forward_return_pct,
                )
            )

        return rows

    def _load_indicator_lookup(
        self,
        stock_id: int,
        start_date: date,
        end_date: date,
    ) -> Dict[date, FactTechnicalIndicator]:
        """Load indicators keyed by calculation date for one stock."""
        indicators = self.indicator_repo.get_indicator_history(
            stock_id=stock_id,
            start_date=start_date,
            end_date=end_date,
        )
        return {indicator.calculation_date: indicator for indicator in indicators}

    def _has_required_indicator_fields(self, indicator: FactTechnicalIndicator) -> bool:
        """Require a minimum indicator snapshot for a usable feature row."""
        for field_name in self.config.required_indicator_fields:
            if getattr(indicator, field_name) is None:
                return False
        return True

    def _is_price_eligible(self, price: FactDailyPrice) -> bool:
        """Apply explicit trust and completeness eligibility rules."""
        if price.bar_status not in self.config.allowed_bar_statuses:
            return False
        if price.data_quality_flag not in self.config.allowed_quality_flags:
            return False
        if self.config.require_complete_data and not price.has_complete_data:
            return False
        if self.config.require_official and not price.is_official:
            return False
        if price.close_price is None:
            return False
        if self.config.min_confidence_score is not None:
            if price.confidence_score is None:
                return False
            if float(price.confidence_score) < self.config.min_confidence_score:
                return False
        return True

    def _has_extreme_anchor_return(
        self,
        anchor_price: FactDailyPrice,
        anchor_index: int,
        price_history: Sequence[FactDailyPrice],
    ) -> bool:
        """Exclude split-like anchor days from model training features."""
        threshold = self.config.max_abs_anchor_return_pct
        if threshold is None:
            return False

        anchor_return = to_float(anchor_price.change_1d_pct)
        if anchor_return is None and anchor_index > 0:
            previous_close = to_float(price_history[anchor_index - 1].close_price)
            current_close = to_float(anchor_price.close_price)
            if previous_close and previous_close > 0 and current_close is not None:
                anchor_return = ((current_close - previous_close) / previous_close) * 100.0

        return anchor_return is not None and abs(anchor_return) > threshold

    def _has_extreme_forward_return(self, forward_return_pct: float) -> bool:
        """Exclude extreme outcomes from model labels and validation metrics."""
        threshold = self.config.max_abs_forward_return_pct
        return threshold is not None and abs(forward_return_pct) > threshold

    def _build_dataset_row(
        self,
        stock: DimStock,
        anchor_price: FactDailyPrice,
        horizon_price: FactDailyPrice,
        anchor_index: int,
        price_history: Sequence[FactDailyPrice],
        indicator: FactTechnicalIndicator,
        target_up: int,
        forward_return_pct: float,
    ) -> ModelingDatasetRow:
        """Build one dataset row without leaking future information into features."""
        history_through_anchor = price_history[: anchor_index + 1]
        ma_7 = to_float(indicator.ma_7)
        ma_30 = to_float(indicator.ma_30)
        ma_90 = to_float(indicator.ma_90)
        engineered_features = build_historical_feature_snapshot(
            history_through_anchor=history_through_anchor,
            current_price=anchor_price.close_price,
            ma_7=ma_7,
            ma_30=ma_30,
            ma_90=ma_90,
        )

        return ModelingDatasetRow(
            stock_id=stock.stock_id,
            stock_code=stock.stock_code,
            anchor_date=anchor_price.price_date,
            horizon_date=horizon_price.price_date,
            close_price=float(anchor_price.close_price),
            horizon_close_price=float(horizon_price.close_price),
            volume=int(anchor_price.volume) if anchor_price.volume is not None else None,
            change_1d_pct=to_float(anchor_price.change_1d_pct),
            change_ytd_pct=to_float(anchor_price.change_ytd_pct),
            price_confidence_score=to_float(anchor_price.confidence_score),
            price_quality_flag=anchor_price.data_quality_flag,
            bar_status=anchor_price.bar_status,
            has_complete_data=bool(anchor_price.has_complete_data),
            is_official=bool(anchor_price.is_official),
            trusted_history_days=len(history_through_anchor),
            volume_ratio=engineered_features.volume_ratio,
            price_change_pct=engineered_features.price_change_pct,
            price_change_3d=engineered_features.price_change_3d,
            price_change_5d=engineered_features.price_change_5d,
            price_change_10d=engineered_features.price_change_10d,
            price_change_20d=engineered_features.price_change_20d,
            price_change_30d=engineered_features.price_change_30d,
            price_change_60d=engineered_features.price_change_60d,
            ma_7=ma_7,
            ma_30=ma_30,
            ma_90=ma_90,
            close_vs_ma_7_pct=engineered_features.close_vs_ma_7_pct,
            close_vs_ma_30_pct=engineered_features.close_vs_ma_30_pct,
            close_vs_ma_90_pct=engineered_features.close_vs_ma_90_pct,
            ma_7_vs_ma_30_pct=engineered_features.ma_7_vs_ma_30_pct,
            ma_30_vs_ma_90_pct=engineered_features.ma_30_vs_ma_90_pct,
            close_vs_20d_high_pct=engineered_features.close_vs_20d_high_pct,
            close_vs_60d_high_pct=engineered_features.close_vs_60d_high_pct,
            close_vs_20d_low_pct=engineered_features.close_vs_20d_low_pct,
            close_vs_60d_low_pct=engineered_features.close_vs_60d_low_pct,
            drawdown_20d_pct=engineered_features.drawdown_20d_pct,
            drawdown_60d_pct=engineered_features.drawdown_60d_pct,
            rebound_20d_pct=engineered_features.rebound_20d_pct,
            rebound_60d_pct=engineered_features.rebound_60d_pct,
            volatility_10d=engineered_features.volatility_10d,
            volatility_20d=engineered_features.volatility_20d,
            downside_volatility_20d=engineered_features.downside_volatility_20d,
            average_volume_20d=engineered_features.average_volume_20d,
            volume_trend_ratio=engineered_features.volume_trend_ratio,
            rsi_14=to_float(indicator.rsi_14),
            macd=to_float(indicator.macd),
            macd_signal=to_float(indicator.macd_signal),
            macd_histogram=to_float(indicator.macd_histogram),
            volatility_30=to_float(indicator.volatility_30),
            atr_14=to_float(indicator.atr_14),
            bollinger_upper=to_float(indicator.bollinger_upper),
            bollinger_middle=to_float(indicator.bollinger_middle),
            bollinger_lower=to_float(indicator.bollinger_lower),
            ma_crossover_signal=indicator.ma_crossover_signal,
            trend_strength=to_float(indicator.trend_strength),
            target_up_10d=target_up,
            forward_return_10d=forward_return_pct,
        )
