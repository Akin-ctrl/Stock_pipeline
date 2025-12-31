"""
Test script for alert notification system.

Tests email and Slack notifications with sample alerts.
"""

import sys
from pathlib import Path
from datetime import date, datetime

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from config.settings import Settings
from config.database import get_session
from services.alerts import AlertNotifier
from repositories import AlertRepository
from models import AlertHistory, AlertRule
from utils import get_logger


def test_notifications():
    """Test alert notification system."""
    logger = get_logger("test_notifications")
    
    # Load settings
    settings = Settings.load()
    logger.info("=" * 80)
    logger.info("NOTIFICATION SYSTEM TEST")
    logger.info("=" * 80)
    
    # Check notification configuration
    logger.info("\nNotification Configuration:")
    logger.info(f"  Email Enabled: {settings.notifications.email_enabled}")
    if settings.notifications.email_enabled:
        logger.info(f"  SMTP Host: {settings.notifications.smtp_host}")
        logger.info(f"  SMTP Port: {settings.notifications.smtp_port}")
        logger.info(f"  From Email: {settings.notifications.from_email}")
        logger.info(f"  To Emails: {', '.join(settings.notifications.to_emails)}")
    
    logger.info(f"  Slack Enabled: {settings.notifications.slack_enabled}")
    if settings.notifications.slack_enabled:
        logger.info(f"  Webhook URL: {settings.notifications.slack_webhook_url[:50]}...")
    
    if not settings.notifications.email_enabled and not settings.notifications.slack_enabled:
        logger.error("\n❌ No notification channels enabled!")
        logger.info("\nTo enable notifications, set these in .env:")
        logger.info("  NOTIFICATION_EMAIL_ENABLED=true")
        logger.info("  SMTP_USER=your_email@gmail.com")
        logger.info("  SMTP_PASSWORD=your_app_password")
        logger.info("  NOTIFICATION_EMAILS=recipient@example.com")
        logger.info("\nFor Slack:")
        logger.info("  NOTIFICATION_SLACK_ENABLED=true")
        logger.info("  SLACK_WEBHOOK_URL=https://hooks.slack.com/services/...")
        return False
    
    # Initialize database and notifier
    db = get_session()
    alert_repo = AlertRepository(db)
    notifier = AlertNotifier()
    
    logger.info("\n" + "=" * 80)
    logger.info("FETCHING RECENT ALERTS")
    logger.info("=" * 80)
    
    # Get recent alerts
    today = date.today()
    alerts = alert_repo.get_alerts_by_date_range(
        start_date=today,
        end_date=today
    )
    
    if not alerts:
        logger.warning(f"\n⚠️  No alerts found for {today}")
        logger.info("Creating test alert...")
        
        # Create a test alert
        test_alert = create_test_alert(alert_repo, today)
        if test_alert:
            alerts = [test_alert]
        else:
            logger.error("Failed to create test alert")
            return False
    
    logger.info(f"\nFound {len(alerts)} alert(s) for {today}")
    
    # Display alerts
    for i, alert in enumerate(alerts, 1):
        logger.info(f"\n  Alert #{i}:")
        logger.info(f"    Rule: {alert.alert_rule.rule_name}")
        logger.info(f"    Stock: {alert.stock.symbol}")
        logger.info(f"    Severity: {alert.severity}")
        logger.info(f"    Message: {alert.message}")
        logger.info(f"    Notification Sent: {alert.notification_sent}")
    
    # Test notifications
    logger.info("\n" + "=" * 80)
    logger.info("SENDING TEST NOTIFICATIONS")
    logger.info("=" * 80)
    
    # Determine channels
    channels = []
    if settings.notifications.email_enabled:
        channels.append('email')
    if settings.notifications.slack_enabled:
        channels.append('slack')
    
    logger.info(f"\nChannels: {', '.join(channels)}")
    
    # Send individual alert
    if alerts:
        logger.info("\n📧 Sending individual alert notification...")
        result = notifier.send_alert(alerts[0], channels=channels)
        
        if result.success:
            logger.info(f"✅ Alert notification sent successfully")
            for channel, status in result.channels_sent.items():
                logger.info(f"  {channel}: {'✅' if status else '❌'}")
        else:
            logger.error(f"❌ Alert notification failed: {result.error}")
    
    # Send daily digest (email only)
    if settings.notifications.email_enabled:
        logger.info("\n📧 Sending daily digest...")
        result = notifier.send_daily_digest(alerts, today)
        
        if result.success:
            logger.info(f"✅ Daily digest sent successfully")
            for channel, status in result.channels_sent.items():
                logger.info(f"  {channel}: {'✅' if status else '❌'}")
        else:
            logger.error(f"❌ Daily digest failed: {result.error}")
    
    logger.info("\n" + "=" * 80)
    logger.info("TEST COMPLETE")
    logger.info("=" * 80)
    
    return True


def create_test_alert(alert_repo: AlertRepository, alert_date: date) -> AlertHistory:
    """Create a test alert for demonstration."""
    logger = get_logger("test_notifications")
    
    try:
        # Get or create a test alert rule
        db = alert_repo.db
        
        # Check if test rule exists
        test_rule = db.query(DimAlert).filter(
            DimAlert.rule_name == 'TEST_PRICE_SPIKE'
        ).first()
        
        if not test_rule:
            logger.info("Creating test alert rule...")
            test_rule = DimAlert(
                rule_name='TEST_PRICE_SPIKE',
                rule_type='PRICE_CHANGE',
                threshold=5.0,
                condition='GREATER_THAN',
                severity='WARNING',
                message_template='Test: {symbol} price changed by {change}%'
            )
            db.add(test_rule)
            db.commit()
            db.refresh(test_rule)
        
        # Get first stock
        from app.models import DimStock
        stock = db.query(DimStock).filter(DimStock.is_active == True).first()
        
        if not stock:
            logger.error("No active stocks found")
            return None
        
        # Create test alert
        test_alert = AlertHistory(
            stock_key=stock.stock_key,
            alert_key=test_rule.alert_key,
            triggered_date=alert_date,
            severity='WARNING',
            message=f'Test: {stock.symbol} price changed by 5.5%',
            metric_value=5.5,
            threshold_value=5.0,
            notification_sent=False,
            created_at=datetime.now()
        )
        
        db.add(test_alert)
        db.commit()
        db.refresh(test_alert)
        
        logger.info(f"✅ Created test alert for {stock.symbol}")
        return test_alert
        
    except Exception as e:
        logger.error(f"Failed to create test alert: {str(e)}")
        return None


if __name__ == "__main__":
    try:
        success = test_notifications()
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        print("\n\n⚠️  Test interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger = get_logger("test_notifications")
        logger.error(f"Test failed: {str(e)}")
        sys.exit(1)
