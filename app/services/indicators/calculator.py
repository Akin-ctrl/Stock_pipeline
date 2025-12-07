"""
Technical indicators calculator.

Calculates technical analysis indicators from price history:
- Moving Averages (SMA, EMA)
- RSI (Relative Strength Index)
- MACD (Moving Average Convergence Divergence)
- Bollinger Bands
- Volatility
"""

from typing import Dict, List, Optional, Tuple
from datetime import date, datetime
import pandas as pd
import numpy as np

from app.utils import get_logger


class IndicatorCalculator:
    """
    Calculates technical indicators from price data.
    
    All calculations use pandas vectorized operations for efficiency.
    Missing values are handled gracefully with forward/backward fill.
    """
    
    # Default parameters
    MA_SHORT_PERIOD = 20  # 20-day moving average
    MA_LONG_PERIOD = 50   # 50-day moving average
    RSI_PERIOD = 14       # 14-day RSI
    MACD_FAST = 12        # MACD fast EMA
    MACD_SLOW = 26        # MACD slow EMA
    MACD_SIGNAL = 9       # MACD signal line
    BOLLINGER_PERIOD = 20 # Bollinger Bands period
    BOLLINGER_STD = 2     # Bollinger Bands standard deviations
    VOLATILITY_PERIOD = 30 # 30-day volatility
    
    def __init__(
        self,
        ma_short: int = MA_SHORT_PERIOD,
        ma_long: int = MA_LONG_PERIOD,
        rsi_period: int = RSI_PERIOD,
        macd_fast: int = MACD_FAST,
        macd_slow: int = MACD_SLOW,
        macd_signal: int = MACD_SIGNAL,
        bollinger_period: int = BOLLINGER_PERIOD,
        bollinger_std: float = BOLLINGER_STD,
        volatility_period: int = VOLATILITY_PERIOD
    ):
        """
        Initialize calculator with custom parameters.
        
        Args:
            ma_short: Short moving average period (days)
            ma_long: Long moving average period (days)
            rsi_period: RSI calculation period (days)
            macd_fast: MACD fast EMA period
            macd_slow: MACD slow EMA period
            macd_signal: MACD signal line period
            bollinger_period: Bollinger Bands period
            bollinger_std: Bollinger Bands standard deviations
            volatility_period: Volatility calculation period
        """
        self.ma_short = ma_short
        self.ma_long = ma_long
        self.rsi_period = rsi_period
        self.macd_fast = macd_fast
        self.macd_slow = macd_slow
        self.macd_signal = macd_signal
        self.bollinger_period = bollinger_period
        self.bollinger_std = bollinger_std
        self.volatility_period = volatility_period
        self.logger = get_logger("indicator_calculator")
    
    def calculate_all(
        self,
        price_df: pd.DataFrame,
        stock_code: Optional[str] = None
    ) -> pd.DataFrame:
        """
        Calculate all technical indicators for price data.
        
        Args:
            price_df: DataFrame with columns: price_date, close_price, high_price, low_price
                     Must be sorted by price_date ascending
            stock_code: Optional stock code for logging
            
        Returns:
            DataFrame with all indicators added as columns
        """
        if price_df.empty:
            return pd.DataFrame()
        
        # Make a copy to avoid modifying original
        df = price_df.copy()
        
        # Ensure sorted by date
        df = df.sort_values('price_date')
        
        # Calculate each indicator
        df = self._calculate_moving_averages(df)
        df = self._calculate_rsi(df)
        df = self._calculate_macd(df)
        df = self._calculate_bollinger_bands(df)
        df = self._calculate_volatility(df)
        
        # Detect crossovers
        df = self._detect_ma_crossovers(df)
        
        # Add calculation timestamp
        df['calculation_date'] = datetime.now()
        
        if stock_code:
            self.logger.info(
                f"Calculated indicators for {stock_code}",
                extra={"stock_code": stock_code, "records": len(df)}
            )
        
        return df
    
    def _calculate_moving_averages(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calculate Simple Moving Averages (SMA)."""
        if 'close_price' not in df.columns:
            return df
        
        # Short-term MA
        df[f'ma_{self.ma_short}'] = df['close_price'].rolling(
            window=self.ma_short,
            min_periods=1
        ).mean()
        
        # Long-term MA
        df[f'ma_{self.ma_long}'] = df['close_price'].rolling(
            window=self.ma_long,
            min_periods=1
        ).mean()
        
        return df
    
    def _calculate_rsi(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate Relative Strength Index (RSI).
        
        RSI = 100 - (100 / (1 + RS))
        where RS = Average Gain / Average Loss over period
        """
        if 'close_price' not in df.columns:
            return df
        
        # Calculate price changes
        delta = df['close_price'].diff()
        
        # Separate gains and losses
        gain = (delta.where(delta > 0, 0)).rolling(
            window=self.rsi_period,
            min_periods=1
        ).mean()
        
        loss = (-delta.where(delta < 0, 0)).rolling(
            window=self.rsi_period,
            min_periods=1
        ).mean()
        
        # Calculate RS and RSI
        rs = gain / loss.replace(0, np.nan)  # Avoid division by zero
        df['rsi'] = 100 - (100 / (1 + rs))
        
        # Handle edge cases
        df['rsi'] = df['rsi'].fillna(50)  # Neutral RSI for missing values
        
        return df
    
    def _calculate_macd(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate MACD (Moving Average Convergence Divergence).
        
        MACD Line = 12-day EMA - 26-day EMA
        Signal Line = 9-day EMA of MACD Line
        MACD Histogram = MACD Line - Signal Line
        """
        if 'close_price' not in df.columns:
            return df
        
        # Calculate EMAs
        ema_fast = df['close_price'].ewm(
            span=self.macd_fast,
            adjust=False,
            min_periods=1
        ).mean()
        
        ema_slow = df['close_price'].ewm(
            span=self.macd_slow,
            adjust=False,
            min_periods=1
        ).mean()
        
        # MACD line
        df['macd_line'] = ema_fast - ema_slow
        
        # Signal line
        df['macd_signal'] = df['macd_line'].ewm(
            span=self.macd_signal,
            adjust=False,
            min_periods=1
        ).mean()
        
        # Histogram
        df['macd_histogram'] = df['macd_line'] - df['macd_signal']
        
        return df
    
    def _calculate_bollinger_bands(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate Bollinger Bands.
        
        Middle Band = 20-day SMA
        Upper Band = Middle Band + (2 * standard deviation)
        Lower Band = Middle Band - (2 * standard deviation)
        """
        if 'close_price' not in df.columns:
            return df
        
        # Middle band (SMA)
        df['bb_middle'] = df['close_price'].rolling(
            window=self.bollinger_period,
            min_periods=1
        ).mean()
        
        # Standard deviation
        rolling_std = df['close_price'].rolling(
            window=self.bollinger_period,
            min_periods=1
        ).std()
        
        # Upper and lower bands
        df['bb_upper'] = df['bb_middle'] + (self.bollinger_std * rolling_std)
        df['bb_lower'] = df['bb_middle'] - (self.bollinger_std * rolling_std)
        
        return df
    
    def _calculate_volatility(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate historical volatility (standard deviation of returns).
        
        Annualized volatility = std(daily returns) * sqrt(252 trading days)
        """
        if 'close_price' not in df.columns:
            return df
        
        # Calculate daily returns
        returns = df['close_price'].pct_change()
        
        # Rolling volatility (30-day)
        df[f'volatility_{self.volatility_period}'] = returns.rolling(
            window=self.volatility_period,
            min_periods=1
        ).std() * np.sqrt(252)  # Annualized
        
        # Fill missing values
        df[f'volatility_{self.volatility_period}'] = df[f'volatility_{self.volatility_period}'].fillna(0)
        
        return df
    
    def _detect_ma_crossovers(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Detect moving average crossovers.
        
        BULLISH: Short MA crosses above Long MA
        BEARISH: Short MA crosses below Long MA
        """
        ma_short_col = f'ma_{self.ma_short}'
        ma_long_col = f'ma_{self.ma_long}'
        
        if ma_short_col not in df.columns or ma_long_col not in df.columns:
            df['ma_crossover_signal'] = None
            return df
        
        # Calculate crossover
        df['ma_crossover_signal'] = None
        
        # Detect where short MA crosses long MA
        for i in range(1, len(df)):
            prev_short = df[ma_short_col].iloc[i-1]
            prev_long = df[ma_long_col].iloc[i-1]
            curr_short = df[ma_short_col].iloc[i]
            curr_long = df[ma_long_col].iloc[i]
            
            # Bullish crossover (golden cross)
            if prev_short <= prev_long and curr_short > curr_long:
                df.loc[df.index[i], 'ma_crossover_signal'] = 'BULLISH'
            # Bearish crossover (death cross)
            elif prev_short >= prev_long and curr_short < curr_long:
                df.loc[df.index[i], 'ma_crossover_signal'] = 'BEARISH'
        
        return df
    
    def calculate_for_stock(
        self,
        stock_id: int,
        stock_code: str,
        price_history: List[Dict]
    ) -> List[Dict]:
        """
        Calculate indicators for a single stock.
        
        Args:
            stock_id: Stock database ID
            stock_code: Stock ticker code
            price_history: List of price dicts with keys: price_date, close_price, etc.
            
        Returns:
            List of indicator dicts ready for database insertion
        """
        if not price_history:
            return []
        
        # Convert to DataFrame
        df = pd.DataFrame(price_history)
        
        # Calculate all indicators
        df = self.calculate_all(df, stock_code=stock_code)
        
        # Prepare for database insertion
        indicators = []
        for _, row in df.iterrows():
            indicator = {
                'stock_id': stock_id,
                'indicator_date': row['price_date'],
                f'ma_{self.ma_short}': self._to_float(row.get(f'ma_{self.ma_short}')),
                f'ma_{self.ma_long}': self._to_float(row.get(f'ma_{self.ma_long}')),
                'rsi': self._to_float(row.get('rsi')),
                'macd_line': self._to_float(row.get('macd_line')),
                'macd_signal': self._to_float(row.get('macd_signal')),
                'macd_histogram': self._to_float(row.get('macd_histogram')),
                'bb_upper': self._to_float(row.get('bb_upper')),
                'bb_middle': self._to_float(row.get('bb_middle')),
                'bb_lower': self._to_float(row.get('bb_lower')),
                f'volatility_{self.volatility_period}': self._to_float(row.get(f'volatility_{self.volatility_period}')),
                'ma_crossover_signal': row.get('ma_crossover_signal')
            }
            indicators.append(indicator)
        
        return indicators
    
    def _to_float(self, value) -> Optional[float]:
        """Convert value to float, handling NaN and None."""
        if pd.isna(value):
            return None
        try:
            return float(value)
        except (ValueError, TypeError):
            return None
    
    def calculate_batch(
        self,
        stocks_data: Dict[int, Tuple[str, List[Dict]]]
    ) -> Dict[int, List[Dict]]:
        """
        Calculate indicators for multiple stocks in batch.
        
        Args:
            stocks_data: Dict mapping stock_id -> (stock_code, price_history)
            
        Returns:
            Dict mapping stock_id -> indicator_list
        """
        results = {}
        
        for stock_id, (stock_code, price_history) in stocks_data.items():
            try:
                indicators = self.calculate_for_stock(
                    stock_id=stock_id,
                    stock_code=stock_code,
                    price_history=price_history
                )
                results[stock_id] = indicators
            except Exception as e:
                self.logger.error(
                    f"Failed to calculate indicators for {stock_code}",
                    extra={"stock_code": stock_code, "error": str(e)}
                )
                results[stock_id] = []
        
        self.logger.info(
            f"Calculated indicators for {len(results)} stocks",
            extra={"total_stocks": len(stocks_data)}
        )
        
        return results
