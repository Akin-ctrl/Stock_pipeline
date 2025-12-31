# Integration Testing Documentation

## 📋 Overview

Complete integration testing suite for the Nigerian Stock Pipeline project. Tests verify end-to-end functionality with real database connections and data flow.

## 🏗️ Test Structure

```
tests/
├── conftest.py                          # Shared fixtures and test database setup
├── pytest.ini                           # Pytest configuration
└── integration/
    ├── test_repositories.py             # Database CRUD operations
    ├── test_data_pipeline.py            # Data fetching, validation, transformation
    ├── test_indicators.py               # Technical indicator calculations
    ├── test_alerts.py                   # Alert evaluation and notifications
    └── test_pipeline_orchestrator.py    # Full pipeline execution
```

## 🚀 Running Tests

### Run All Integration Tests
```bash
pytest tests/integration/ -v
```

### Run Specific Test File
```bash
pytest tests/integration/test_repositories.py -v
```

### Run Tests by Marker
```bash
# Database tests only
pytest -m database -v

# Slow tests (with external APIs)
pytest -m slow -v

# Exclude external API tests
pytest -m "not external" -v
```

### Run with Coverage
```bash
pytest tests/integration/ --cov=app --cov-report=html
```

## 🎯 Test Categories

### 1. Repository Tests (`test_repositories.py`)
- ✅ Stock CRUD operations
- ✅ Price data management
- ✅ Indicator storage and retrieval
- ✅ Bulk operations
- ✅ Relationship integrity

**Key Tests:**
- `test_create_and_get_stock` - Stock creation and retrieval
- `test_bulk_create_prices` - Bulk price data loading
- `test_get_latest_indicator` - Latest indicator retrieval
- `test_repository_relationships` - ORM relationship validation

### 2. Data Pipeline Tests (`test_data_pipeline.py`)
- ✅ NGX data fetching
- ✅ Yahoo Finance integration
- ✅ Data validation
- ✅ Data transformation
- ✅ Error handling

**Key Tests:**
- `test_fetch_validate_transform_load` - Full NGX pipeline
- `test_validate_missing_values` - Missing data handling
- `test_validate_negative_prices` - Invalid price detection
- `test_transform_split_data` - Stock/price data separation

### 3. Indicator Tests (`test_indicators.py`)
- ✅ Moving averages (SMA, EMA)
- ✅ RSI calculation
- ✅ MACD calculation
- ✅ Bollinger Bands
- ✅ Performance testing

**Key Tests:**
- `test_calculate_moving_averages` - SMA/EMA accuracy
- `test_calculate_rsi` - RSI range validation (0-100)
- `test_calculate_macd` - MACD components
- `test_save_indicators_to_database` - Database persistence

### 4. Alert Tests (`test_alerts.py`)
- ✅ RSI oversold/overbought alerts
- ✅ Price cross SMA alerts
- ✅ Alert history tracking
- ✅ Alert status management
- ✅ Notification generation

**Key Tests:**
- `test_evaluate_rsi_oversold_alert` - RSI < 30 detection
- `test_evaluate_rsi_overbought_alert` - RSI > 70 detection
- `test_evaluate_multiple_alerts` - Bulk alert evaluation
- `test_get_unacknowledged_alerts` - Alert filtering

### 5. Pipeline Orchestrator Tests (`test_pipeline_orchestrator.py`)
- ✅ Individual stage execution
- ✅ Full pipeline run
- ✅ Error handling
- ✅ Configuration customization
- ✅ Transaction management

**Key Tests:**
- `test_full_pipeline_execution` - Complete E2E test
- `test_pipeline_handles_invalid_data` - Error resilience
- `test_selective_stage_execution` - Stage control
- `test_pipeline_rollback_on_critical_error` - Transaction safety

## 🔧 Test Database Setup

### Automatic Test Database Creation
The test suite automatically creates `stock_pipeline_test` database:

```python
# In conftest.py
TEST_DATABASE_URL = "postgresql://user:pass@localhost:5432/stock_pipeline_test"
```

### Database Isolation
- Each test function gets a fresh transaction
- Automatic rollback after each test
- Data cleanup between tests

### Fixtures Available
```python
# Database session
def test_example(db_session: Session):
    # Use db_session for database operations
    
# Pre-populated data
def test_with_data(sample_stocks, sample_prices, sample_indicators):
    # Test with realistic data
```

## 📊 Test Markers

| Marker | Description | Usage |
|--------|-------------|-------|
| `@pytest.mark.integration` | Integration test | All integration tests |
| `@pytest.mark.database` | Requires database | Database-dependent tests |
| `@pytest.mark.external` | Calls external APIs | NGX, Yahoo Finance |
| `@pytest.mark.slow` | Takes >5 seconds | Performance, E2E tests |

## 🎨 Sample Test Output

```
🚀 Running FULL pipeline execution...
============================================================
📊 Pipeline Execution Results:
   Status: success
   Start Time: 2025-12-22 10:30:15
   End Time: 2025-12-22 10:30:45
   Duration: 30.24 seconds
   Stages Completed: 7

   Statistics:
     stocks_loaded: 150
     prices_loaded: 4500
     indicators_calculated: 3000
     alerts_triggered: 12

   📈 Total stocks in database: 150
   📊 Sample stock (DANGCEM):
      Prices: 30
      Indicators: 30
      Alerts: 2

============================================================
✅ Full pipeline test completed!
```

## 🔍 Debugging Failed Tests

### View Detailed Output
```bash
pytest tests/integration/test_repositories.py::TestStockRepository::test_create_and_get_stock -vv -s
```

### Check Test Database
```bash
psql -U stock_user -d stock_pipeline_test -c "SELECT * FROM dim_stocks LIMIT 5;"
```

### Enable Debug Logging
```python
# In test file
import logging
logging.basicConfig(level=logging.DEBUG)
```

## 📈 Coverage Goals

| Component | Target | Current |
|-----------|--------|---------|
| Repositories | 90%+ | ✅ |
| Data Sources | 85%+ | ✅ |
| Indicators | 95%+ | ✅ |
| Alerts | 90%+ | ✅ |
| Orchestrator | 85%+ | ✅ |

## 🐳 Running Tests in Docker

```bash
# Start test database
docker-compose up -d postgres

# Run tests in container
docker-compose run --rm app pytest tests/integration/ -v

# With coverage
docker-compose run --rm app pytest tests/integration/ --cov=app --cov-report=term-missing
```

## 🔐 Test Environment Variables

Create `.env.test` file:
```bash
POSTGRES_USER=stock_user
POSTGRES_PASSWORD=stock_password
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=stock_pipeline_test

# Optional: Disable external API calls
ENABLE_NGX_FETCH=false
ENABLE_YAHOO_FETCH=false
```

## ✅ Best Practices

1. **Isolation**: Each test should be independent
2. **Cleanup**: Use fixtures for setup/teardown
3. **Naming**: Descriptive test names (`test_validate_negative_prices`)
4. **Assertions**: Multiple assertions are OK for integration tests
5. **Documentation**: Add docstrings to complex tests
6. **Performance**: Mark slow tests with `@pytest.mark.slow`

## 🚨 Common Issues

### Database Connection Failed
```bash
# Check PostgreSQL is running
sudo systemctl status postgresql

# Verify connection
psql -U stock_user -d postgres -c "SELECT 1"
```

### Fixture Not Found
```bash
# Ensure conftest.py is in correct location
ls tests/conftest.py
```

### Import Errors
```bash
# Install dependencies
pip install -r app/requirements.txt

# Add project to PYTHONPATH
export PYTHONPATH="${PYTHONPATH}:/home/Stock_pipeline"
```

## 📚 Next Steps

1. Add unit tests in `tests/unit/`
2. Add API endpoint tests if building web service
3. Add performance benchmarking tests
4. Set up CI/CD pipeline for automated testing
5. Generate coverage reports in CI

---

**Last Updated**: December 22, 2025  
**Test Count**: 50+ integration tests  
**Coverage**: 85%+ overall
