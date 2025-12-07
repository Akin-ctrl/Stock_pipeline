"""
Test script for data sources.

Tests NGX and Yahoo Finance data fetching.
"""

from datetime import date, timedelta
from app.services.data_sources import NGXDataSource, YahooDataSource
from app.utils import get_logger

logger = get_logger(__name__)


def test_yahoo_source():
    """Test Yahoo Finance data source."""
    logger.info("=" * 60)
    logger.info("Testing YahooDataSource...")
    
    try:
        yahoo = YahooDataSource()
        
        # Fetch last 7 days
        end_date = date.today()
        start_date = end_date - timedelta(days=7)
        
        df = yahoo.fetch(start_date=start_date, end_date=end_date)
        
        logger.info(f"✅ Fetched {len(df)} records from Yahoo Finance")
        
        if not df.empty:
            logger.info(f"Columns: {list(df.columns)}")
            logger.info(f"Tickers: {df['stock_code'].unique().tolist()}")
            logger.info(f"Date range: {df['price_date'].min()} to {df['price_date'].max()}")
            logger.info(f"\nSample data (first 3 rows):\n{df.head(3)}")
        
        return df
        
    except Exception as e:
        logger.error(f"❌ Yahoo source test failed: {str(e)}")
        raise


def test_ngx_source():
    """Test NGX data source."""
    logger.info("=" * 60)
    logger.info("Testing NGXDataSource...")
    logger.info("⚠️  Note: NGX source requires live website access and correct HTML parsing")
    
    try:
        ngx = NGXDataSource()
        df = ngx.fetch()
        
        logger.info(f"✅ Fetched {len(df)} records from NGX")
        
        if not df.empty:
            logger.info(f"Columns: {list(df.columns)}")
            logger.info(f"Unique stocks: {df['stock_code'].nunique()}")
            logger.info(f"Sectors: {df['sector'].unique().tolist()}")
            logger.info(f"\nSample data (first 5 rows):\n{df.head(5)}")
        else:
            logger.warning("⚠️  No data returned (website structure may have changed)")
        
        return df
        
    except Exception as e:
        logger.warning(f"⚠️  NGX source test failed: {str(e)}")
        logger.info("This is expected if the website structure has changed")
        logger.info("HTML parsing logic in ngx_source.py needs to be updated")
        return None


if __name__ == "__main__":
    logger.info("Testing Data Sources...")
    
    # Test Yahoo (should work reliably)
    yahoo_df = test_yahoo_source()
    
    # Test NGX (may need HTML parsing adjustments)
    ngx_df = test_ngx_source()
    
    logger.info("=" * 60)
    logger.info("✅ Data source tests complete!")
    logger.info(f"Yahoo records: {len(yahoo_df) if yahoo_df is not None else 0}")
    logger.info(f"NGX records: {len(ngx_df) if ngx_df is not None else 0}")
