"""
Test alert evaluator.

Tests rule evaluation logic with existing database data.
"""

from datetime import date

from app.services.alerts import AlertEvaluator
from app.repositories import AlertRepository
from app.config.database import get_session
from app.utils import get_logger


logger = get_logger("test_alerts")


def test_rule_types():
    """Test checking what rule types exist."""
    logger.info("=" * 60)
    logger.info("Testing Rule Types")
    logger.info("=" * 60)
    
    session = get_session()
    alert_repo = AlertRepository(session)
    
    # Check each rule type
    rule_types = ['PRICE_MOVEMENT', 'MA_CROSSOVER', 'RSI', 'VOLATILITY', 'VOLUME_SPIKE']
    
    logger.info("Active rules by type:")
    for rule_type in rule_types:
        rules = alert_repo.get_rules_by_type(rule_type)
        active_rules = [r for r in rules if r.is_active]
        logger.info(f"  {rule_type}: {len(active_rules)} active rules")
        for rule in active_rules:
            logger.info(f"    - {rule.rule_name} ({rule.severity})")
    
    session.close()
    logger.info("✓ Rule types check completed")


def test_evaluate_all_rules():
    """Test evaluating all active alert rules."""
    logger.info("\n" + "=" * 60)
    logger.info("Testing Alert Rule Evaluation")
    logger.info("=" * 60)
    
    evaluator = AlertEvaluator()
    
    # Get active rules
    session = get_session()
    alert_repo = AlertRepository(session)
    rules = alert_repo.get_all_rules(active_only=True)
    
    logger.info(f"Found {len(rules)} active alert rules")
    
    # Evaluate all rules for today
    logger.info(f"\nEvaluating rules for {date.today()}")
    result = evaluator.evaluate_all_rules(evaluation_date=date.today())
    
    logger.info(f"\nEvaluation Results:")
    logger.info(f"  Rules evaluated: {result.rules_evaluated}")
    logger.info(f"  Stocks checked: {result.stocks_checked}")
    logger.info(f"  Alerts generated: {result.alerts_generated}")
    
    if result.alerts:
        logger.info(f"\nGenerated Alerts (showing first 3):")
        for alert in result.alerts[:3]:
            logger.info(f"  - {alert['message']}")
    else:
        logger.info("\nNo alerts triggered (conditions not met)")
    
    evaluator.close()
    session.close()
    
    logger.info("✓ Alert evaluation completed successfully")


def test_check_active_alerts():
    """Test checking existing active alerts."""
    logger.info("\n" + "=" * 60)
    logger.info("Testing Active Alerts Query")
    logger.info("=" * 60)
    
    session = get_session()
    alert_repo = AlertRepository(session)
    
    # Get active alerts (limit via slicing)
    active_alerts = alert_repo.get_active_alerts()[:5]
    
    logger.info(f"Found {len(active_alerts)} active alerts")
    
    if active_alerts:
        for alert in active_alerts:
            logger.info(f"  [{alert.severity}] {alert.message[:60]}...")
    else:
        logger.info("No active alerts in database")
    
    session.close()
    logger.info("✓ Active alerts query completed")


def test_alert_summary():
    """Test getting alert summary statistics."""
    logger.info("\n" + "=" * 60)
    logger.info("Testing Alert Summary")
    logger.info("=" * 60)
    
    session = get_session()
    alert_repo = AlertRepository(session)
    
    # Get summary (returns nested dict with 'severity', 'resolved', 'period_days')
    summary = alert_repo.get_alert_summary()
    
    logger.info(f"Period: Last {summary['period_days']} days")
    
    # Show severity breakdown
    severity_total = sum(summary['severity'].values()) if summary['severity'] else 0
    logger.info(f"Total alerts by severity: {severity_total}")
    for severity, count in summary['severity'].items():
        logger.info(f"  {severity}: {count}")
    
    # Show resolution status
    resolved_count = summary['resolved'].get(True, 0)
    unresolved_count = summary['resolved'].get(False, 0)
    logger.info(f"Resolution status:")
    logger.info(f"  Resolved: {resolved_count}")
    logger.info(f"  Unresolved: {unresolved_count}")
    
    session.close()
    logger.info("✓ Alert summary completed")


if __name__ == "__main__":
    try:
        test_rule_types()
        test_evaluate_all_rules()
        test_check_active_alerts()
        test_alert_summary()
        
        logger.info("\n" + "=" * 60)
        logger.info("✓ All alert evaluator tests completed!")
        logger.info("=" * 60)
        
    except Exception as e:
        logger.error(f"Test failed: {e}")
        import traceback
        traceback.print_exc()
        raise
