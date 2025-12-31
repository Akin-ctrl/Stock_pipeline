# 📧 Quick Start: Alert Notifications

## 🚀 5-Minute Setup

### Step 1: Configure Environment

```bash
# Copy template
cp .env.example .env

# Edit .env and add:
NOTIFICATION_EMAIL_ENABLED=true
SMTP_USER=your_email@gmail.com
SMTP_PASSWORD=your_16_char_app_password
NOTIFICATION_EMAILS=recipient@example.com
```

### Step 2: Get Gmail App Password

1. Visit: https://myaccount.google.com/security
2. Enable 2-Factor Authentication (if not already)
3. Click "2-Step Verification" → "App passwords"
4. Select "Mail" and generate
5. Copy 16-character password to `.env` as `SMTP_PASSWORD`

### Step 3: Test

```bash
python app/test_notifications.py
```

Expected output:
```
✅ Notification configuration loaded
✅ Email sent successfully
  email: ✅
✅ Daily digest sent successfully
```

### Step 4: Run Pipeline

```bash
# Notifications will be sent automatically when alerts trigger
python -m app.pipelines.orchestrator
```

## 📋 Configuration Options

### Email Only
```bash
NOTIFICATION_EMAIL_ENABLED=true
NOTIFICATION_SLACK_ENABLED=false
```

### Email + Slack
```bash
NOTIFICATION_EMAIL_ENABLED=true
NOTIFICATION_SLACK_ENABLED=true
SLACK_WEBHOOK_URL=https://hooks.slack.com/services/YOUR/WEBHOOK/URL
```

### Multiple Recipients
```bash
NOTIFICATION_EMAILS=analyst1@company.com,analyst2@company.com,trader@company.com
```

## 🔍 Verification

### Check if notifications are enabled:
```python
from app.config.settings import Settings

settings = Settings.load()
print(f"Email: {settings.notifications.email_enabled}")
print(f"Slack: {settings.notifications.slack_enabled}")
print(f"Recipients: {settings.notifications.to_emails}")
```

### Check sent notifications:
```python
from app.config.database import get_db
from app.repositories import AlertRepository

db = get_db()
repo = AlertRepository(db)
alerts = repo.get_alerts_by_date_range(date.today(), date.today())

for alert in alerts:
    print(f"{alert.stock.symbol}: Sent={alert.notification_sent}")
```

## 🎯 Common Use Cases

### Send alert manually:
```python
from app.services.alerts import AlertNotifier
from app.repositories import AlertRepository
from app.config.database import get_db
from datetime import date

db = get_db()
repo = AlertRepository(db)
notifier = AlertNotifier()

# Get today's alerts
alerts = repo.get_alerts_by_date_range(date.today(), date.today())

# Send first alert
if alerts:
    result = notifier.send_alert(alerts[0], channels=['email'])
    print(f"Sent: {result.success}")
```

### Send daily digest:
```python
from app.services.alerts import AlertNotifier
from datetime import date

notifier = AlertNotifier()
result = notifier.send_daily_digest(alerts, date.today())
print(f"Digest sent: {result.success}")
```

## ⚠️ Troubleshooting

### Email not sending?

**Check 1: Credentials**
```bash
# Verify SMTP_USER and SMTP_PASSWORD are set
grep SMTP .env
```

**Check 2: Test SMTP connection**
```python
import smtplib

try:
    server = smtplib.SMTP('smtp.gmail.com', 587)
    server.starttls()
    server.login('your_email@gmail.com', 'your_app_password')
    print("✅ SMTP connection successful")
    server.quit()
except Exception as e:
    print(f"❌ SMTP error: {e}")
```

**Check 3: Check logs**
```bash
tail -f app/logs/pipeline_orchestrator.log | grep -i notification
```

### Slack not working?

**Test webhook manually:**
```bash
curl -X POST -H 'Content-type: application/json' \
  --data '{"text":"Test notification"}' \
  YOUR_WEBHOOK_URL
```

If this works, the webhook URL is valid.

## 📊 What Gets Sent?

### CRITICAL Alerts → Immediate Notification
- Price spikes > 8%
- Volume spikes > 2.5x average
- Sent immediately to email + Slack

### WARNING Alerts → Daily Digest
- Price changes 4-8%
- RSI oversold/overbought
- Included in end-of-day email summary

## 🔐 Security Checklist

- ✅ `.env` file in `.gitignore` (never commit)
- ✅ Use Gmail app-specific password (not account password)
- ✅ Restrict Slack webhook to specific channel
- ✅ Use TLS for SMTP connections
- ✅ Review recipient list regularly

## 📚 Next Steps

After notifications are working:

1. **Customize alert thresholds** in `.env`
2. **Add more recipients** to `NOTIFICATION_EMAILS`
3. **Set up Slack** for real-time team alerts
4. **Review logs** to monitor delivery
5. **Proceed to Advisory Service** (next on TODO)

## 🆘 Need Help?

See full documentation: [NOTIFICATION_README.md](app/services/alerts/NOTIFICATION_README.md)

Implementation details: [NOTIFICATION_SYSTEM_SUMMARY.md](NOTIFICATION_SYSTEM_SUMMARY.md)
