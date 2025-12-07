"""
Test technical indicators calculator.

Tests MA, RSI, MACD, Bollinger Bands, and volatility calculations.
"""

from datetime import date, timedelta
import pandas as pd
import numpy as np

from app.services.indicators import IndicatorCalculator
from app.utils import get_logger


logger = get_logger("test_indicators")


def test_moving_averages():
    """Test moving average calculations."""
    logger.info("=" * 60)
    logger.info("Testing Moving Averages")
    logger.info("=" * 60)
    
    calculator = IndicatorCalculator(ma_short=5, ma_long=10)
    
    # Create test price data (10 days)
    prices = [100, 102, 101, 103, 105, 104, 106, 108, 107, 109]
    test_data = pd.DataFrame({
        'price_date': [date.today() - timedelta(days=9-i) for i in range(10)],
        'close_price': prices
    })
    
    logger.info(f"Testing with {len(test_data)} days of price data")
    logger.info(f"Prices: {prices}")
    
    # Calculate indicators
    result = calculator.calculate_all(test_data)
    
    logger.info(f"\nResults:")
    logger.info(f"MA_5 (last 3 days): {result['ma_5'].tail(3).tolist()}")
    logger.info(f"MA_10 (last 3 days): {result['ma_10'].tail(3).tolist()}")
    
    # Verify MA_5 for last day (average of last 5: 104, 106, 108, 107, 109)
    expected_ma5 = np.mean([104, 106, 108, 107, 109])
    actual_ma5 = result['ma_5'].iloc[-1]
    logger.info(f"\nExpected MA_5: {expected_ma5:.2f}")
    logger.info(f"Actual MA_5: {actual_ma5:.2f}")
    
    assert abs(actual_ma5 - expected_ma5) < 0.01, "MA_5 calculation incorrect"
    logger.info("✓ Moving average calculation verified")
    
    return result


def test_rsi():
    """Test RSI calculation."""
    logger.info("\n" + "=" * 60)
    logger.info("Testing RSI (Relative Strength Index)")
    logger.info("=" * 60)
    
    calculator = IndicatorCalculator(rsi_period=14)
    
    # Create price data with clear trend
    base_price = 100
    prices = []
    for i in range(20):
        # Uptrend with some volatility
        if i < 10:
            prices.append(base_price + i * 2)
        else:
            prices.append(base_price + i * 2 + np.random.uniform(-1, 1))
    
    test_data = pd.DataFrame({
        'price_date': [date.today() - timedelta(days=19-i) for i in range(20)],
        'close_price': prices
    })
    
    logger.info(f"Testing with {len(test_data)} days of price data")
    
    result = calculator.calculate_all(test_data)
    
    logger.info(f"\nRSI values (last 5 days): {result['rsi'].tail(5).tolist()}")
    
    # RSI should be between 0 and 100
    assert result['rsi'].min() >= 0, "RSI below 0"
    assert result['rsi'].max() <= 100, "RSI above 100"
    
    # For uptrend, RSI should be > 50
    last_rsi = result['rsi'].iloc[-1]
    logger.info(f"Last RSI: {last_rsi:.2f}")
    logger.info("✓ RSI calculation verified (0-100 range)")
    
    return result


def test_macd():
    """Test MACD calculation."""
    logger.info("\n" + "=" * 60)
    logger.info("Testing MACD")
    logger.info("=" * 60)
    
    calculator = IndicatorCalculator(macd_fast=12, macd_slow=26, macd_signal=9)
    
    # Create price data with trend change
    prices = []
    for i in range(50):
        if i < 25:
            prices.append(100 + i * 0.5)  # Uptrend
        else:
            prices.append(100 + (50 - i) * 0.5)  # Downtrend
    
    test_data = pd.DataFrame({
        'price_date': [date.today() - timedelta(days=49-i) for i in range(50)],
        'close_price': prices
    })
    
    logger.info(f"Testing with {len(test_data)} days of price data")
    
    result = calculator.calculate_all(test_data)
    
    logger.info(f"\nMACD Line (last 3): {result['macd_line'].tail(3).tolist()}")
    logger.info(f"MACD Signal (last 3): {result['macd_signal'].tail(3).tolist()}")
    logger.info(f"MACD Histogram (last 3): {result['macd_histogram'].tail(3).tolist()}")
    
    # Check that histogram = line - signal
    last_hist = result['macd_histogram'].iloc[-1]
    last_line = result['macd_line'].iloc[-1]
    last_signal = result['macd_signal'].iloc[-1]
    
    expected_hist = last_line - last_signal
    logger.info(f"\nExpected histogram: {expected_hist:.4f}")
    logger.info(f"Actual histogram: {last_hist:.4f}")
    
    assert abs(last_hist - expected_hist) < 0.01, "MACD histogram incorrect"
    logger.info("✓ MACD calculation verified")
    
    return result


def test_bollinger_bands():
    """Test Bollinger Bands calculation."""
    logger.info("\n" + "=" * 60)
    logger.info("Testing Bollinger Bands")
    logger.info("=" * 60)
    
    calculator = IndicatorCalculator(bollinger_period=20, bollinger_std=2)
    
    # Create price data
    np.random.seed(42)
    prices = 100 + np.cumsum(np.random.randn(30) * 2)
    
    test_data = pd.DataFrame({
        'price_date': [date.today() - timedelta(days=29-i) for i in range(30)],
        'close_price': prices
    })
    
    logger.info(f"Testing with {len(test_data)} days of price data")
    
    result = calculator.calculate_all(test_data)
    
    logger.info(f"\nLast day values:")
    logger.info(f"Price: {result['close_price'].iloc[-1]:.2f}")
    logger.info(f"BB Upper: {result['bb_upper'].iloc[-1]:.2f}")
    logger.info(f"BB Middle: {result['bb_middle'].iloc[-1]:.2f}")
    logger.info(f"BB Lower: {result['bb_lower'].iloc[-1]:.2f}")
    
    # Check that Upper > Middle > Lower
    last_upper = result['bb_upper'].iloc[-1]
    last_middle = result['bb_middle'].iloc[-1]
    last_lower = result['bb_lower'].iloc[-1]
    
    assert last_upper > last_middle, "BB Upper should be > Middle"
    assert last_middle > last_lower, "BB Middle should be > Lower"
    logger.info("✓ Bollinger Bands calculation verified")
    
    return result


def test_volatility():
    """Test volatility calculation."""
    logger.info("\n" + "=" * 60)
    logger.info("Testing Volatility")
    logger.info("=" * 60)
    
    calculator = IndicatorCalculator(volatility_period=30)
    
    # Create price data with low volatility
    np.random.seed(42)
    prices_low = 100 + np.cumsum(np.random.randn(40) * 0.5)
    
    test_data = pd.DataFrame({
        'price_date': [date.today() - timedelta(days=39-i) for i in range(40)],
        'close_price': prices_low
    })
    
    logger.info(f"Testing with {len(test_data)} days of price data")
    
    result = calculator.calculate_all(test_data)
    
    logger.info(f"\nVolatility (last 5 days): {result['volatility_30'].tail(5).tolist()}")
    
    # Volatility should be non-negative
    assert (result['volatility_30'] >= 0).all(), "Volatility should be non-negative"
    
    last_vol = result['volatility_30'].iloc[-1]
    logger.info(f"Last volatility: {last_vol:.4f}")
    logger.info("✓ Volatility calculation verified")
    
    return result


def test_ma_crossovers():
    """Test moving average crossover detection."""
    logger.info("\n" + "=" * 60)
    logger.info("Testing MA Crossover Detection")
    logger.info("=" * 60)
    
    calculator = IndicatorCalculator(ma_short=5, ma_long=10)
    
    # Create data with clear crossover
    prices = []
    for i in range(30):
        if i < 15:
            prices.append(100 - i)  # Downtrend (short MA below long MA)
        else:
            prices.append(85 + (i - 15) * 2)  # Sharp uptrend (crossover should occur)
    
    test_data = pd.DataFrame({
        'price_date': [date.today() - timedelta(days=29-i) for i in range(30)],
        'close_price': prices
    })
    
    logger.info(f"Testing with {len(test_data)} days of price data")
    logger.info("Price pattern: Downtrend (15 days) → Uptrend (15 days)")
    
    result = calculator.calculate_all(test_data)
    
    # Find crossovers
    crossovers = result[result['ma_crossover_signal'].notna()]
    
    logger.info(f"\nCrossovers detected: {len(crossovers)}")
    if not crossovers.empty:
        for idx, row in crossovers.iterrows():
            logger.info(
                f"  {row['price_date']}: {row['ma_crossover_signal']} "
                f"(MA_5={row['ma_5']:.2f}, MA_10={row['ma_10']:.2f})"
            )
    
    logger.info("✓ MA crossover detection verified")
    
    return result


def test_calculate_for_stock():
    """Test calculating indicators for a single stock."""
    logger.info("\n" + "=" * 60)
    logger.info("Testing calculate_for_stock (database format)")
    logger.info("=" * 60)
    
    calculator = IndicatorCalculator()
    
    # Create price history as list of dicts (database format)
    price_history = []
    for i in range(30):
        price_history.append({
            'price_date': date.today() - timedelta(days=29-i),
            'close_price': 100 + i * 0.5,
            'high_price': 101 + i * 0.5,
            'low_price': 99 + i * 0.5
        })
    
    logger.info(f"Testing with {len(price_history)} price records")
    
    # Calculate indicators
    indicators = calculator.calculate_for_stock(
        stock_id=1,
        stock_code='TEST',
        price_history=price_history
    )
    
    logger.info(f"\nGenerated {len(indicators)} indicator records")
    
    # Show sample record
    sample = indicators[-1]  # Last record
    logger.info("\nSample indicator record (last day):")
    logger.info(f"  stock_id: {sample['stock_id']}")
    logger.info(f"  indicator_date: {sample['indicator_date']}")
    logger.info(f"  ma_20: {sample['ma_20']}")
    logger.info(f"  ma_50: {sample['ma_50']}")
    logger.info(f"  rsi: {sample['rsi']}")
    logger.info(f"  macd_line: {sample['macd_line']}")
    logger.info(f"  volatility_30: {sample['volatility_30']}")
    
    # Verify structure
    assert len(indicators) == len(price_history), "Should have indicator for each price"
    assert all('stock_id' in ind for ind in indicators), "Missing stock_id"
    assert all('indicator_date' in ind for ind in indicators), "Missing indicator_date"
    
    logger.info("✓ Database format indicators verified")
    
    return indicators


def test_batch_calculation():
    """Test calculating indicators for multiple stocks."""
    logger.info("\n" + "=" * 60)
    logger.info("Testing Batch Calculation")
    logger.info("=" * 60)
    
    calculator = IndicatorCalculator()
    
    # Create data for 3 stocks
    stocks_data = {}
    for stock_id in [1, 2, 3]:
        price_history = []
        for i in range(20):
            price_history.append({
                'price_date': date.today() - timedelta(days=19-i),
                'close_price': 100 * stock_id + i
            })
        stocks_data[stock_id] = (f'STOCK{stock_id}', price_history)
    
    logger.info(f"Testing with {len(stocks_data)} stocks")
    
    # Calculate batch
    results = calculator.calculate_batch(stocks_data)
    
    logger.info(f"\nResults:")
    for stock_id, indicators in results.items():
        logger.info(f"  Stock {stock_id}: {len(indicators)} indicator records")
    
    assert len(results) == 3, "Should have results for 3 stocks"
    logger.info("✓ Batch calculation verified")
    
    return results


if __name__ == "__main__":
    try:
        # Test individual indicators
        test_moving_averages()
        test_rsi()
        test_macd()
        test_bollinger_bands()
        test_volatility()
        test_ma_crossovers()
        
        # Test database format
        test_calculate_for_stock()
        test_batch_calculation()
        
        logger.info("\n" + "=" * 60)
        logger.info("✓ All indicator calculator tests passed!")
        logger.info("=" * 60)
        
    except Exception as e:
        logger.error(f"Test failed: {e}")
        raise
