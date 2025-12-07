"""
Test data processors (validator and transformer).

Tests validation rules and data cleaning transformations.
"""

from datetime import date, timedelta
import pandas as pd

from app.services.processors import DataValidator, DataTransformer
from app.utils import get_logger


logger = get_logger("test_processors")


def test_validator():
    """Test DataValidator with various data quality scenarios."""
    logger.info("=" * 60)
    logger.info("Testing DataValidator")
    logger.info("=" * 60)
    
    # Define valid sectors (from Nigerian stock exchange sectors)
    valid_sectors = [
        'Financials', 'Consumer Goods', 'Oil & Gas', 'Industrials',
        'Technology', 'Basic Materials', 'Consumer Services', 'Health Care',
        'Telecom', 'Utilities', 'ETF'
    ]
    
    validator = DataValidator(valid_sectors=valid_sectors)
    
    # Create test data with various quality issues
    test_data = pd.DataFrame([
        # Good record
        {
            'stock_code': 'TEST1',
            'company_name': 'Test Company 1',
            'sector': 'Financials',
            'exchange': 'NGX',
            'price_date': date.today(),
            'open_price': 10.0,
            'high_price': 11.0,
            'low_price': 9.5,
            'close_price': 10.5,
            'volume': 100000,
            'change_1d_pct': 5.0
        },
        # Missing close_price
        {
            'stock_code': 'TEST2',
            'company_name': 'Test Company 2',
            'sector': 'Consumer Goods',
            'exchange': 'NGX',
            'price_date': date.today(),
            'close_price': None,
            'change_1d_pct': None
        },
        # Extreme price change (suspicious)
        {
            'stock_code': 'TEST3',
            'company_name': 'Test Company 3',
            'sector': 'Oil & Gas',
            'exchange': 'NGX',
            'price_date': date.today(),
            'close_price': 15.0,
            'change_1d_pct': 75.0  # >50% - suspicious
        },
        # Invalid exchange
        {
            'stock_code': 'TEST4',
            'company_name': 'Test Company 4',
            'sector': 'Industrials',
            'exchange': 'NYSE',  # Not in VALID_EXCHANGES
            'price_date': date.today(),
            'close_price': 20.0
        },
        # OHLC inconsistency (suspicious)
        {
            'stock_code': 'TEST5',
            'company_name': 'Test Company 5',
            'sector': 'Technology',
            'exchange': 'LSE',
            'price_date': date.today(),
            'open_price': 30.0,
            'high_price': 25.0,  # High < Low - impossible
            'low_price': 28.0,
            'close_price': 29.0
        },
        # Duplicate (same stock_code + price_date as TEST1)
        {
            'stock_code': 'TEST1',
            'company_name': 'Test Company 1 Duplicate',
            'sector': 'Financials',
            'exchange': 'NGX',
            'price_date': date.today(),
            'close_price': 11.0
        },
    ])
    
    logger.info(f"Testing with {len(test_data)} records (including invalid)")
    
    # Validate
    cleaned_df, result = validator.validate(test_data)
    
    logger.info("\n" + "=" * 60)
    logger.info("Validation Results")
    logger.info("=" * 60)
    logger.info(f"Valid records: {result.valid_count}")
    logger.info(f"Suspicious records: {result.suspicious_count}")
    logger.info(f"Invalid records: {result.invalid_count}")
    logger.info(f"Total processed: {result.total_count}")
    logger.info(f"Is valid: {result.is_valid}")
    
    if result.errors:
        logger.info("\nErrors:")
        for error in result.errors:
            logger.info(f"  - {error}")
    
    if result.warnings:
        logger.info("\nWarnings:")
        for warning in result.warnings[:5]:  # Show first 5
            logger.info(f"  - {warning}")
    
    logger.info(f"\nCleaned DataFrame has {len(cleaned_df)} records")
    logger.info("\nQuality flags distribution:")
    logger.info(cleaned_df['data_quality_flag'].value_counts().to_dict())
    
    return cleaned_df, result


def test_transformer():
    """Test DataTransformer with messy data."""
    logger.info("\n" + "=" * 60)
    logger.info("Testing DataTransformer")
    logger.info("=" * 60)
    
    transformer = DataTransformer()
    
    # Create messy test data
    test_data = pd.DataFrame([
        {
            'stock_code': '  test1  ',  # Whitespace
            'company_name': '  test  company  ONE  ',  # Extra spaces
            'sector': 'Financials',
            'exchange': 'NGX',
            'price_date': date.today(),
            'close_price': 10.5,
            'open_price': 10.0,
            'high_price': 11.0,
            'low_price': 9.5,
            'volume': 100000
        },
        {
            'stock_code': 'test-2',  # Special character
            'company_name': 'TEST COMPANY TWO',
            'sector': None,  # Missing sector
            'exchange': 'LSE',
            'price_date': date.today(),
            'close_price': 20.0,
            'open_price': None,  # Missing OHLC
            'high_price': None,
            'low_price': None,
            'volume': None
        },
    ])
    
    logger.info(f"Testing with {len(test_data)} messy records")
    logger.info("\nBefore transformation:")
    logger.info(f"stock_code: {test_data['stock_code'].tolist()}")
    logger.info(f"company_name: {test_data['company_name'].tolist()}")
    logger.info(f"sector: {test_data['sector'].tolist()}")
    
    # Transform
    cleaned_df = transformer.transform(test_data, source='test')
    
    logger.info("\nAfter transformation:")
    logger.info(f"stock_code: {cleaned_df['stock_code'].tolist()}")
    logger.info(f"company_name: {cleaned_df['company_name'].tolist()}")
    logger.info(f"sector: {cleaned_df['sector'].tolist()}")
    logger.info(f"source: {cleaned_df['source'].unique()}")
    logger.info(f"has_complete_data: {cleaned_df['has_complete_data'].tolist()}")
    
    # Test deduplication
    logger.info("\n" + "-" * 60)
    logger.info("Testing deduplication")
    logger.info("-" * 60)
    
    dup_data = pd.DataFrame([
        {'stock_code': 'TEST1', 'price_date': date.today(), 'close_price': 10.0},
        {'stock_code': 'TEST1', 'price_date': date.today(), 'close_price': 10.5},  # Duplicate
        {'stock_code': 'TEST2', 'price_date': date.today(), 'close_price': 20.0},
    ])
    
    logger.info(f"Before deduplication: {len(dup_data)} records")
    dedup_df = transformer.deduplicate(dup_data, keep='last')
    logger.info(f"After deduplication: {len(dedup_df)} records")
    
    return cleaned_df


def test_integration():
    """Test validator and transformer together."""
    logger.info("\n" + "=" * 60)
    logger.info("Testing Validator + Transformer Integration")
    logger.info("=" * 60)
    
    # Define valid sectors
    valid_sectors = [
        'Financials', 'Consumer Goods', 'Oil & Gas', 'Industrials',
        'Technology', 'Basic Materials', 'Consumer Services', 'Health Care',
        'Telecom', 'Utilities', 'ETF'
    ]
    
    validator = DataValidator(valid_sectors=valid_sectors)
    transformer = DataTransformer()
    
    # Create realistic test data
    test_data = pd.DataFrame([
        {
            'stock_code': '  dangcem  ',
            'company_name': '  DANGOTE CEMENT  PLC  ',
            'sector': 'Basic Materials',
            'exchange': 'NGX',
            'price_date': date.today(),
            'close_price': 450.0,
            'change_1d_pct': 2.5
        },
        {
            'stock_code': 'gtco',
            'company_name': 'Guaranty Trust Bank',
            'sector': 'Financials',
            'exchange': 'NGX',
            'price_date': date.today(),
            'close_price': 35.0,
            'change_1d_pct': None  # Missing
        },
    ])
    
    logger.info(f"Starting with {len(test_data)} records")
    
    # Step 1: Transform
    logger.info("\nStep 1: Transform data")
    transformed_df = transformer.transform(test_data, source='test')
    logger.info(f"After transformation: {len(transformed_df)} records")
    logger.info(f"Sample stock_code: {transformed_df['stock_code'].iloc[0]}")
    logger.info(f"Sample company_name: {transformed_df['company_name'].iloc[0]}")
    
    # Step 2: Validate
    logger.info("\nStep 2: Validate data")
    validated_df, result = validator.validate(transformed_df)
    logger.info(f"After validation: {len(validated_df)} records")
    logger.info(f"Valid: {result.valid_count}, Suspicious: {result.suspicious_count}, Invalid: {result.invalid_count}")
    
    # Final result
    logger.info("\n" + "=" * 60)
    logger.info("Final Result")
    logger.info("=" * 60)
    logger.info(f"Records ready for database: {len(validated_df)}")
    logger.info(f"Columns: {validated_df.columns.tolist()}")
    
    return validated_df


if __name__ == "__main__":
    try:
        # Test validator
        cleaned_df, validation_result = test_validator()
        
        # Test transformer
        transformed_df = test_transformer()
        
        # Test integration
        final_df = test_integration()
        
        logger.info("\n" + "=" * 60)
        logger.info("✓ All processor tests completed successfully!")
        logger.info("=" * 60)
        
    except Exception as e:
        logger.error(f"Test failed: {e}")
        raise
