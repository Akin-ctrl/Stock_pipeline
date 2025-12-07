"""
Test script to verify repository layer functionality.

Quick sanity checks for all repositories.
"""

from app.config import get_db
from app.repositories import (
    StockRepository,
    PriceRepository,
    IndicatorRepository,
    AlertRepository
)
from app.utils import get_logger

logger = get_logger(__name__)


def test_repositories():
    """Test basic repository operations."""
    
    db = get_db()
    
    try:
        with db.get_session() as session:
            # Test StockRepository
            logger.info("=" * 50)
            logger.info("Testing StockRepository...")
            stock_repo = StockRepository(session)
            
            # Get all active stocks
            active_stocks = stock_repo.get_all_active()
            logger.info(f"Active stocks count: {len(active_stocks)}")
            
            # Get NGX stocks
            ngx_stocks = stock_repo.get_by_exchange('NGX')
            logger.info(f"NGX stocks count: {len(ngx_stocks)}")
            
            # Get LSE stocks
            lse_stocks = stock_repo.get_by_exchange('LSE')
            logger.info(f"LSE stocks count: {len(lse_stocks)}")
            
            # Get stock codes
            all_codes = stock_repo.get_stock_codes()
            logger.info(f"Stock codes: {all_codes[:5] if all_codes else 'None'}")
            
            # Test PriceRepository
            logger.info("=" * 50)
            logger.info("Testing PriceRepository...")
            price_repo = PriceRepository(session)
            
            # Count prices
            price_count = price_repo.count()
            logger.info(f"Total price records: {price_count}")
            
            # Test IndicatorRepository
            logger.info("=" * 50)
            logger.info("Testing IndicatorRepository...")
            indicator_repo = IndicatorRepository(session)
            
            # Count indicators
            indicator_count = indicator_repo.count()
            logger.info(f"Total indicator records: {indicator_count}")
            
            # Test AlertRepository
            logger.info("=" * 50)
            logger.info("Testing AlertRepository...")
            alert_repo = AlertRepository(session)
            
            # Get all rules
            rules = alert_repo.get_all_rules()
            logger.info(f"Alert rules count: {len(rules)}")
            for rule in rules:
                logger.info(f"  - {rule.rule_name} ({rule.rule_type}) [{rule.severity}]")
            
            # Count alerts
            alert_count = alert_repo.count()
            logger.info(f"Total alert records: {alert_count}")
            
            # Get active alerts
            active_alerts = alert_repo.get_active_alerts()
            logger.info(f"Active alerts count: {len(active_alerts)}")
            
            logger.info("=" * 50)
            logger.info("✅ All repository tests passed!")
            
    except Exception as e:
        logger.error(f"Repository test failed: {str(e)}", exc_info=True)
        raise


if __name__ == "__main__":
    test_repositories()
