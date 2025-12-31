# Alert Notification System

The notification system sends real-time alerts when market conditions trigger alert rules.

## 📋 Overview

The notification system consists of:

1. **AlertNotifier** - Core notification service
2. **NotificationConfig** - Configuration for email/Slack
3. **Integration** - Automatic notifications in the pipeline

## 🚀 Features

### Notification Channels

- **Email** (SMTP)
  - HTML formatted alerts with color-coded severity
  - Daily digest summaries
  - Support for multiple recipients
  - Tested with Gmail, Outlook, Office365

- **Slack** (Webhooks)
  - Real-time alert messages
  - Color-coded based on severity
  - Rich formatting with stock details

- **SMS** (Future - Twilio)
  - Placeholder for SMS integration

### Alert Types

1. **Individual Alerts**
   - Sent immediately for CRITICAL alerts
   - Include stock details, price, and metrics
   - Direct notification to all configured channels

2. **Daily Digest**
   - Email-only summary of all alerts for the day
   - Grouped by severity (Critical, Warning)
   - Sent at end of pipeline execution

## ⚙️ Configuration

### Environment Variables

Add these to your `.env` file:

```bash
# Email Notifications
NOTIFICATION_EMAIL_ENABLED=true
SMTP_HOST=smtp.gmail.com
SMTP_PORT=587
SMTP_USER=your_email@gmail.com
SMTP_PASSWORD=your_app_password
NOTIFICATION_FROM_EMAIL=alerts@stockpipeline.com
NOTIFICATION_EMAILS=recipient1@example.com,recipient2@example.com

# Slack Notifications
NOTIFICATION_SLACK_ENABLED=true
SLACK_WEBHOOK_URL=https://hooks.slack.com/services/YOUR/WEBHOOK/URL
```

### Gmail Setup

For Gmail SMTP:

1. Enable 2-Factor Authentication on your Google Account
2. Generate an App Password:
   - Go to: https://myaccount.google.com/security
   - Click "2-Step Verification" → "App passwords"
   - Select "Mail" and generate password
3. Use the generated 16-character password as `SMTP_PASSWORD`

### Slack Setup

For Slack webhooks:

1. Create a Slack App at https://api.slack.com/apps
2. Enable "Incoming Webhooks"
3. Create a webhook for your channel
4. Copy the webhook URL to `SLACK_WEBHOOK_URL`

## 📧 Email Templates

### Individual Alert Email

```
Subject: [CRITICAL] Stock Alert: MTNN - Price Spike

MTNN Alert Triggered

Stock: MTNN (MTN Nigeria Communications)
Sector: Telecommunications
Alert: Price Spike
Severity: CRITICAL

Message: MTNN price increased by 8.5% to ₦125.00

Details:
- Current Price: ₦125.00
- Metric Value: 8.5
- Threshold: 8.0
- Date: 2025-07-23

--
Stock Pipeline Alert System
```

### Daily Digest Email

```
Subject: Daily Stock Alerts Summary - 2025-07-23

Daily Alerts Summary
2025-07-23

Total Alerts: 5
Critical: 2
Warning: 3

═══════════════════════════════════════
CRITICAL ALERTS
═══════════════════════════════════════

MTNN - Price Spike
Severity: CRITICAL
Message: MTNN price increased by 8.5%
Time: 14:35

DANGCEM - Volume Spike
Severity: CRITICAL
Message: DANGCEM volume 3x average
Time: 15:20

═══════════════════════════════════════
WARNING ALERTS
═══════════════════════════════════════

[List of warning alerts...]
```

## 🔧 Usage

### Automatic (Pipeline)

Notifications are sent automatically during pipeline execution:

```python
from app.pipelines import PipelineOrchestrator

orchestrator = PipelineOrchestrator()
result = orchestrator.run()  # Sends notifications if alerts generated
```

### Manual (Code)

Send notifications programmatically:

```python
from app.services.alerts import AlertNotifier
from app.repositories import AlertRepository
from app.config.database import get_db
from datetime import date

# Initialize
db = get_db()
alert_repo = AlertRepository(db)
notifier = AlertNotifier()

# Get alerts
alerts = alert_repo.get_alerts_by_date_range(
    start_date=date.today(),
    end_date=date.today()
)

# Send individual alert
if alerts:
    result = notifier.send_alert(alerts[0], channels=['email', 'slack'])
    print(f"Success: {result.success}")

# Send daily digest
result = notifier.send_daily_digest(alerts, date.today())
print(f"Digest sent: {result.success}")
```

### Testing

Test the notification system:

```bash
# Run test script
python app/test_notifications.py
```

The test script will:
1. Check notification configuration
2. Fetch recent alerts (or create a test alert)
3. Send individual notification
4. Send daily digest
5. Report success/failure for each channel

## 📊 NotificationResult

The `NotificationResult` dataclass tracks delivery:

```python
@dataclass
class NotificationResult:
    success: bool                    # Overall success
    channels_sent: Dict[str, bool]   # Per-channel status
    error: Optional[str]             # Error message if failed
    timestamp: datetime              # Delivery timestamp
```

Example:

```python
result = notifier.send_alert(alert, channels=['email', 'slack'])

if result.success:
    print("✅ Notification sent")
    print(f"Email: {'✅' if result.channels_sent['email'] else '❌'}")
    print(f"Slack: {'✅' if result.channels_sent['slack'] else '❌'}")
else:
    print(f"❌ Failed: {result.error}")
```

## 🎨 Severity Colors

Alerts are color-coded by severity:

| Severity | Email Color | Slack Color |
|----------|-------------|-------------|
| CRITICAL | #dc3545 (Red) | danger |
| WARNING  | #ffc107 (Yellow) | warning |
| INFO     | #17a2b8 (Blue) | good |

## 🔐 Security

- Store SMTP password in `.env` (never commit)
- Use app-specific passwords for Gmail
- Restrict Slack webhook URL access
- Validate email addresses before sending
- Use TLS/SSL for SMTP connections

## 📝 Logging

All notification activity is logged:

```
INFO: Sending CRITICAL alert notification for MTNN
INFO: Email sent successfully to 2 recipients
INFO: Slack notification sent successfully
INFO: Sent daily digest with 5 alerts
ERROR: Failed to send email: SMTP authentication failed
```

## 🚨 Error Handling

The notifier handles errors gracefully:

- SMTP connection failures → Logged, returns error result
- Invalid email addresses → Skipped, logged
- Slack webhook errors → Logged, returns error result
- Partial delivery → Continues with other channels

## 🔄 Integration Points

The notification system integrates with:

1. **PipelineOrchestrator** - Auto-sends after alert evaluation
2. **AlertEvaluator** - Can trigger notifications on rule match
3. **Airflow DAGs** - Scheduled digest emails
4. **CLI** - Manual notification commands (future)

## 📦 Dependencies

- `smtplib` - Email sending (Python stdlib)
- `email.mime` - Email formatting (Python stdlib)
- `requests` - Slack webhook API
- `app.repositories.AlertRepository` - Fetch alerts
- `app.config.Settings` - Configuration

## 🔮 Future Enhancements

- [ ] SMS notifications via Twilio
- [ ] Webhook notifications for custom integrations
- [ ] Push notifications (mobile)
- [ ] Configurable digest schedules
- [ ] Alert escalation rules
- [ ] Notification preferences per user
- [ ] Rate limiting and throttling
- [ ] Notification templates in database
- [ ] A/B testing for message content
- [ ] Delivery analytics and tracking

## 🐛 Troubleshooting

### Email not sending

1. Check SMTP credentials in `.env`
2. Verify Gmail app password is correct
3. Check firewall allows port 587
4. Review logs for SMTP errors

### Slack not working

1. Verify webhook URL is correct
2. Test webhook with curl:
   ```bash
   curl -X POST -H 'Content-type: application/json' \
     --data '{"text":"Test"}' \
     YOUR_WEBHOOK_URL
   ```
3. Check Slack app permissions

### No alerts generated

1. Run pipeline to generate alerts
2. Check alert thresholds in settings
3. Verify market data is being ingested
4. Review alert rules in database

## 📚 Related Documentation

- [Alert System Documentation](alerts/README.md)
- [Pipeline Orchestrator](../pipelines/README.md)
- [Configuration Guide](../config/README.md)
