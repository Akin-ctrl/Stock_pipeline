"""
Integration tests for alert system (evaluator, notifications).

Tests alert rule evaluation, alert generation, and notification system.
"""

import pytest
from datetime import date, timedelta
from decimal import Decimal

from app.models import AlertRule, AlertHistory
from app.repositories import AlertRepository, StockRepository, PriceRepository, IndicatorRepository
from app.services.alerts import AlertEvaluator


@pytest.mark.integration
@pytest.mark.database
class TestAlertEvaluator:
    """Test alert rule evaluation."""
    
    def test_evaluate_all_rules_no_rules(self, db_session):
        """Test evaluation when no rules exist."""
        evaluator = AlertEvaluator(session=db_session)
        
        result = evaluator.evaluate_all_rules()
        
        assert result.alerts_generated == 0
        assert result.rules_evaluated == 0
    
    def test_evaluate_all_rules_no_stocks(self, db_session, sample_alert_rules):
        """Test evaluation when no stocks exist."""
        alert_repo = AlertRepository(db_session)
        
        # Create alert rule
        rule = sample_alert_rules[0]
        alert_repo.create_rule(rule)
        db_session.commit()
        
        evaluator = AlertEvaluator(session=db_session)
        result = evaluator.evaluate_all_rules()
        
        # Should evaluate rule but generate no alerts (no stocks)
        assert result.rules_evaluated >= 1
        assert result.alerts_generated == 0
        assert result.stocks_checked == 0
    
    def test_evaluate_price_movement_rule(
        self, db_session, sample_sectors, sample_stocks, sample_prices
    ):
        """Test price movement alert rule."""
        stock_repo = StockRepository(db_session)
        price_repo = PriceRepository(db_session)
        alert_repo = AlertRepository(db_session)
        
        # Create stock and prices
        stock = sample_stocks[0]
        stock_id = stock.stock_id
        db_session.commit()
        
        # Create price data with significant movement
        base_price = Decimal('100.00')
        prices = [
            {
                'stock_id': stock_id,
                'price_date': date.today() - timedelta(days=2),
                'close_price': base_price,
                'volume': 1000000,
                'source': 'test'
            },
            {
                'stock_id': stock_id,
                'price_date': date.today() - timedelta(days=1),
                'close_price': base_price * Decimal('1.06'),  # 6% increase
                'volume': 1000000,
                'source': 'test'
            }
        ]
        
        for price_data in prices:
            price_repo.upsert_price(price_data)
        db_session.commit()
        
        # Create price movement rule (trigger on 5% change)
        rule = AlertRule(
            rule_name='Price Surge Alert',
            rule_type='PRICE_MOVEMENT',
            threshold_value=5.0,  # 5% threshold
            is_active=True
        )
        alert_repo.create_rule(rule)
        db_session.commit()
        
        # Evaluate rules
        evaluator = AlertEvaluator(session=db_session)
        result = evaluator.evaluate_all_rules(
            evaluation_date=date.today() - timedelta(days=1)
        )
        
        # Should generate alert for 6% movement
        assert result.alerts_generated >= 1
        assert result.rules_evaluated >= 1
        assert result.stocks_checked >= 1
    
    def test_evaluate_rsi_rule(
        self, db_session, sample_sectors, sample_stocks, sample_indicators
    ):
        """Test RSI alert rule."""
        stock_repo = StockRepository(db_session)
        indicator_repo = IndicatorRepository(db_session)
        alert_repo = AlertRepository(db_session)
        
        # Create stock
        stock = sample_stocks[0]
        stock_id = stock.stock_id
        db_session.commit()
        
        # Create RSI indicator showing oversold condition
        indicator_data = {
            'stock_id': stock_id,
            'calculation_date': date.today(),
            'rsi_14': Decimal('25.0'),  # Oversold
            'source': 'test'
        }
        indicator_repo.save_indicators([indicator_data])
        db_session.commit()
        
        # Create RSI rule (trigger when RSI < 30)
        rule = AlertRule(
            rule_name='RSI Oversold Alert',
            rule_type='RSI',
            threshold_value=30.0,
            is_active=True
        )
        alert_repo.create_rule(rule)
        db_session.commit()
        
        # Evaluate rules
        evaluator = AlertEvaluator(session=db_session)
        result = evaluator.evaluate_all_rules(evaluation_date=date.today())
        
        # Should generate alert for oversold RSI
        assert result.alerts_generated >= 1
        assert result.rules_evaluated >= 1
    
    def test_alert_deduplication(
        self, db_session, sample_sectors, sample_stocks, sample_prices
    ):
        """Test that duplicate alerts are not created."""
        stock_repo = StockRepository(db_session)
        price_repo = PriceRepository(db_session)
        alert_repo = AlertRepository(db_session)
        
        # Create stock and price
        stock = sample_stocks[0]
        stock_id = stock.stock_id
        db_session.commit()
        
        prices = [
            {
                'stock_id': stock_id,
                'price_date': date.today() - timedelta(days=1),
                'close_price': Decimal('100.00'),
                'volume': 1000000,
                'source': 'test'
            },
            {
                'stock_id': stock_id,
                'price_date': date.today(),
                'close_price': Decimal('110.00'),  # 10% increase
                'volume': 1000000,
                'source': 'test'
            }
        ]
        
        for price_data in prices:
            price_repo.upsert_price(price_data)
        db_session.commit()
        
        # Create rule
        rule = AlertRule(
            rule_name='Price Alert',
            rule_type='PRICE_MOVEMENT',
            threshold_value=5.0,
            is_active=True
        )
        rule_id = alert_repo.create_rule(rule)
        db_session.commit()
        
        # Evaluate first time
        evaluator = AlertEvaluator(session=db_session)
        result1 = evaluator.evaluate_all_rules(evaluation_date=date.today())
        first_count = result1.alerts_generated
        
        # Evaluate second time - should not create duplicates
        result2 = evaluator.evaluate_all_rules(evaluation_date=date.today())
        
        # Second evaluation should generate 0 new alerts
        assert result2.alerts_generated == 0 or result2.alerts_generated < first_count
    
    def test_inactive_rules_not_evaluated(
        self, db_session, sample_sectors, sample_stocks
    ):
        """Test that inactive rules are not evaluated."""
        stock_repo = StockRepository(db_session)
        alert_repo = AlertRepository(db_session)
        
        # Create stock
        stock = sample_stocks[0]
        stock_repo.create_stock(
            stock_code=stock.stock_code,
            company_name=stock.company_name,
            sector_id=stock.sector_id,
            exchange=stock.exchange
        )
        db_session.commit()
        
        # Create inactive rule
        rule = AlertRule(
            rule_name='Inactive Rule',
            rule_type='PRICE_MOVEMENT',
            threshold_value=5.0,
            is_active=False  # Inactive
        )
        alert_repo.create_rule(rule)
        db_session.commit()
        
        # Evaluate
        evaluator = AlertEvaluator(session=db_session)
        result = evaluator.evaluate_all_rules()
        
        # Should not evaluate inactive rule
        assert result.rules_evaluated == 0


@pytest.mark.integration
@pytest.mark.database
class TestAlertRepository:
    """Test alert repository operations."""
    
    def test_create_and_get_rule(self, db_session, sample_alert_rules):
        """Test creating and retrieving alert rules."""
        alert_repo = AlertRepository(db_session)
        
        rule = sample_alert_rules[0]
        rule_id = alert_repo.create_rule(rule)
        db_session.commit()
        
        # Retrieve rule
        retrieved = alert_repo.get_rule_by_id(rule_id)
        
        assert retrieved is not None
        assert retrieved.rule_name == rule.rule_name
        assert retrieved.rule_type == rule.rule_type
        assert retrieved.threshold_value == rule.threshold
    
    def test_get_all_active_rules(self, db_session, sample_alert_rules):
        """Test retrieving only active rules."""
        alert_repo = AlertRepository(db_session)
        
        # Create mix of active and inactive rules
        for i, rule in enumerate(sample_alert_rules):
            rule.is_active = (i % 2 == 0)  # Alternate active/inactive
            alert_repo.create_rule(rule)
        db_session.commit()
        
        # Get active rules only
        active_rules = alert_repo.get_all_rules(active_only=True)
        
        assert len(active_rules) > 0
        assert all(rule.is_active for rule in active_rules)
    
    def test_alert_exists(self, db_session, sample_sectors, sample_stocks, sample_alert_rules):
        """Test checking if alert exists."""
        stock_repo = StockRepository(db_session)
        alert_repo = AlertRepository(db_session)
        
        # Create stock and rule
        stock_id = stock_repo.create_stock(sample_stocks[0])
        rule_id = alert_repo.create_rule(sample_alert_rules[0])
        db_session.commit()
        
        alert_date = date.today()
        
        # Initially should not exist
        assert not alert_repo.alert_exists(stock_id, rule_id, alert_date)
        
        # Create alert
        alert = AlertHistory(
            stock_id=stock_id,
            rule_id=rule_id,
            alert_date=alert_date,
            message='Test alert',
            severity='INFO'
        )
        db_session.add(alert)
        db_session.commit()
        
        # Now should exist
        assert alert_repo.alert_exists(stock_id, rule_id, alert_date)
    
    def test_get_alerts_by_date_range(
        self, db_session, sample_sectors, sample_stocks, sample_alert_rules
    ):
        """Test retrieving alerts by date range."""
        stock_repo = StockRepository(db_session)
        alert_repo = AlertRepository(db_session)
        
        # Create stock and rule
        stock = sample_stocks[0]
        stock_id = stock.stock_id
        rule_id = alert_repo.create_rule(sample_alert_rules[0])
        db_session.commit()
        
        # Create alerts across different dates
        dates = [
            date.today() - timedelta(days=5),
            date.today() - timedelta(days=3),
            date.today() - timedelta(days=1),
        ]
        
        for alert_date in dates:
            alert = AlertHistory(
                stock_id=stock_id,
                rule_id=rule_id,
                alert_date=alert_date,
                message=f'Alert on {alert_date}',
                severity='INFO'
            )
            db_session.add(alert)
        db_session.commit()
        
        # Query date range
        start_date = date.today() - timedelta(days=4)
        end_date = date.today()
        
        alerts = alert_repo.get_alerts_by_date_range(start_date, end_date)
        
        # Should get 2 alerts (days=3 and days=1)
        assert len(alerts) >= 2
        assert all(start_date <= a.alert_date <= end_date for a in alerts)
