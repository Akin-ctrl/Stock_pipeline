"""
Alert notification service.

Sends alerts via multiple channels: Email, Slack, SMS.
Handles formatting, batching, and delivery tracking.
"""

from typing import List, Dict, Optional, Set
from datetime import datetime, date
from dataclasses import dataclass
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import requests
from jinja2 import Template

from app.models import AlertHistory
from app.repositories import AlertRepository
from app.config import get_settings
from app.utils import get_logger
from app.utils.exceptions import NotificationError


@dataclass
class NotificationResult:
    """
    Result of notification delivery attempt.
    
    Attributes:
        success: Whether notification was sent successfully
        channels_sent: List of channels that succeeded
        channels_failed: List of channels that failed
        errors: Error messages if any
    """
    success: bool
    channels_sent: List[str]
    channels_failed: List[str]
    errors: List[str]


class AlertNotifier:
    """
    Alert notification service supporting multiple channels.
    
    Channels:
    - Email: SMTP-based email notifications
    - Slack: Webhook-based Slack messages
    - SMS: Twilio integration (future)
    
    Features:
    - Template-based formatting
    - Batch notifications (daily digest)
    - Severity-based routing
    - Delivery tracking
    """
    
    def __init__(self, alert_repo: Optional[AlertRepository] = None):
        """
        Initialize notifier with configuration.
        
        Args:
            alert_repo: AlertRepository for tracking delivery status
        """
        self.logger = get_logger("alert_notifier")
        self.settings = get_settings()
        self.alert_repo = alert_repo
        
        # Email configuration
        self.email_enabled = getattr(self.settings.notifications, 'email_enabled', False)
        self.smtp_host = getattr(self.settings.notifications, 'smtp_host', 'smtp.gmail.com')
        self.smtp_port = getattr(self.settings.notifications, 'smtp_port', 587)
        self.smtp_user = getattr(self.settings.notifications, 'smtp_user', None)
        self.smtp_password = getattr(self.settings.notifications, 'smtp_password', None)
        self.from_email = getattr(self.settings.notifications, 'from_email', 'alerts@stockpipeline.com')
        self.to_emails = getattr(self.settings.notifications, 'to_emails', [])
        
        # Slack configuration
        self.slack_enabled = getattr(self.settings.notifications, 'slack_enabled', False)
        self.slack_webhook = getattr(self.settings.notifications, 'slack_webhook_url', None)
        
        self.logger.info(
            f"Initialized notifier - Email: {self.email_enabled}, Slack: {self.slack_enabled}"
        )
    
    def send_alert(
        self,
        alert: AlertHistory,
        channels: Optional[List[str]] = None
    ) -> NotificationResult:
        """
        Send a single alert notification.
        
        Args:
            alert: Alert to send
            channels: Specific channels to use (default: all enabled)
            
        Returns:
            NotificationResult with delivery status
        """
        if channels is None:
            channels = self._get_enabled_channels()
        
        if not channels:
            self.logger.warning("No notification channels enabled")
            return NotificationResult(False, [], [], ["No channels enabled"])
        
        self.logger.info(
            f"Sending alert {alert.alert_id} via channels: {channels}",
            extra={"alert_id": alert.alert_id, "channels": channels}
        )
        
        channels_sent = []
        channels_failed = []
        errors = []
        
        # Format alert message
        message = self._format_alert(alert)
        
        # Send via each channel
        if 'email' in channels and self.email_enabled:
            try:
                self._send_email([alert], message)
                channels_sent.append('email')
            except Exception as e:
                self.logger.error(f"Email send failed: {str(e)}", exc_info=True)
                channels_failed.append('email')
                errors.append(f"Email: {str(e)}")
        
        if 'slack' in channels and self.slack_enabled:
            try:
                self._send_slack(message, alert.severity)
                channels_sent.append('slack')
            except Exception as e:
                self.logger.error(f"Slack send failed: {str(e)}", exc_info=True)
                channels_failed.append('slack')
                errors.append(f"Slack: {str(e)}")
        
        # Update delivery status in database
        if self.alert_repo and channels_sent:
            try:
                self.alert_repo.mark_notification_sent(
                    alert.alert_id,
                    ','.join(channels_sent)
                )
            except Exception as e:
                self.logger.warning(f"Failed to update notification status: {str(e)}")
        
        success = len(channels_sent) > 0
        
        return NotificationResult(
            success=success,
            channels_sent=channels_sent,
            channels_failed=channels_failed,
            errors=errors
        )
    
    def send_daily_digest(
        self,
        alerts: List[AlertHistory],
        digest_date: Optional[date] = None
    ) -> NotificationResult:
        """
        Send daily digest of all alerts.
        
        Args:
            alerts: List of alerts to include
            digest_date: Date for the digest (default: today)
            
        Returns:
            NotificationResult with delivery status
        """
        if digest_date is None:
            digest_date = date.today()
        
        if not alerts:
            self.logger.info("No alerts to send in daily digest")
            return NotificationResult(True, [], [], [])
        
        self.logger.info(
            f"Sending daily digest with {len(alerts)} alerts for {digest_date}",
            extra={"alert_count": len(alerts), "date": str(digest_date)}
        )
        
        channels = self._get_enabled_channels()
        channels_sent = []
        channels_failed = []
        errors = []
        
        # Format digest
        message = self._format_digest(alerts, digest_date)
        
        # Send via email
        if 'email' in channels and self.email_enabled:
            try:
                self._send_email_digest(alerts, message, digest_date)
                channels_sent.append('email')
            except Exception as e:
                self.logger.error(f"Email digest failed: {str(e)}", exc_info=True)
                channels_failed.append('email')
                errors.append(f"Email: {str(e)}")
        
        # Send via Slack
        if 'slack' in channels and self.slack_enabled:
            try:
                self._send_slack(message, 'INFO')
                channels_sent.append('slack')
            except Exception as e:
                self.logger.error(f"Slack digest failed: {str(e)}", exc_info=True)
                channels_failed.append('slack')
                errors.append(f"Slack: {str(e)}")
        
        success = len(channels_sent) > 0
        
        return NotificationResult(
            success=success,
            channels_sent=channels_sent,
            channels_failed=channels_failed,
            errors=errors
        )
    
    def _get_enabled_channels(self) -> List[str]:
        """Get list of enabled notification channels."""
        channels = []
        if self.email_enabled:
            channels.append('email')
        if self.slack_enabled:
            channels.append('slack')
        return channels
    
    def _format_alert(self, alert: AlertHistory) -> str:
        """
        Format single alert as text message.
        
        Args:
            alert: Alert to format
            
        Returns:
            Formatted message string
        """
        severity_emoji = {
            'CRITICAL': '🚨',
            'WARNING': '⚠️',
            'INFO': 'ℹ️'
        }
        
        emoji = severity_emoji.get(alert.severity, '📊')
        
        return f"""
{emoji} **{alert.severity} ALERT**

**Stock**: {alert.stock.stock_code} - {alert.stock.company_name}
**Rule**: {alert.rule.rule_name}
**Date**: {alert.alert_date}
**Message**: {alert.message}
**Trigger Value**: {alert.trigger_value}

---
Alert ID: {alert.alert_id}
Timestamp: {alert.alert_timestamp}
"""
    
    def _format_digest(self, alerts: List[AlertHistory], digest_date: date) -> str:
        """
        Format daily digest message.
        
        Args:
            alerts: List of alerts
            digest_date: Date of digest
            
        Returns:
            Formatted digest string
        """
        # Group by severity
        critical = [a for a in alerts if a.severity == 'CRITICAL']
        warnings = [a for a in alerts if a.severity == 'WARNING']
        info = [a for a in alerts if a.severity == 'INFO']
        
        digest = f"""
📈 **Nigerian Stock Alert Digest - {digest_date.strftime('%B %d, %Y')}**

**Summary**: {len(alerts)} total alerts
- 🚨 Critical: {len(critical)}
- ⚠️  Warnings: {len(warnings)}
- ℹ️  Info: {len(info)}

"""
        
        if critical:
            digest += "\n🚨 **CRITICAL ALERTS**\n"
            for alert in critical[:5]:  # Top 5
                digest += f"• {alert.stock.stock_code}: {alert.message}\n"
        
        if warnings:
            digest += "\n⚠️  **WARNINGS**\n"
            for alert in warnings[:5]:  # Top 5
                digest += f"• {alert.stock.stock_code}: {alert.message}\n"
        
        if info:
            digest += "\n📊 **INFO**\n"
            for alert in info[:5]:  # Top 5
                digest += f"• {alert.stock.stock_code}: {alert.message}\n"
        
        digest += f"\n---\n💡 View all alerts in your dashboard or database"
        
        return digest
    
    def _send_email(self, alerts: List[AlertHistory], message: str) -> None:
        """
        Send alert via email.
        
        Args:
            alerts: Alerts to send
            message: Formatted message
            
        Raises:
            NotificationError: If email send fails
        """
        if not self.smtp_user or not self.smtp_password:
            raise NotificationError("SMTP credentials not configured")
        
        if not self.to_emails:
            raise NotificationError("No recipient emails configured")
        
        try:
            # Create message
            msg = MIMEMultipart('alternative')
            msg['Subject'] = f"Stock Alert: {alerts[0].severity} - {alerts[0].stock.stock_code}"
            msg['From'] = self.from_email
            msg['To'] = ', '.join(self.to_emails)
            
            # Plain text and HTML versions
            text_part = MIMEText(message, 'plain')
            html_part = MIMEText(self._message_to_html(message), 'html')
            
            msg.attach(text_part)
            msg.attach(html_part)
            
            # Send via SMTP with timeout to prevent hanging
            try:
                with smtplib.SMTP(self.smtp_host, self.smtp_port, timeout=30) as server:
                    server.starttls()
                    server.login(self.smtp_user, self.smtp_password)
                    server.send_message(msg)
                
                self.logger.info(f"Email sent to {len(self.to_emails)} recipients")
            except (OSError, smtplib.SMTPException, ConnectionError) as e:
                # Network-related errors - provide helpful message
                error_msg = f"Email send failed (network/SMTP error): {str(e)}"
                self.logger.warning(error_msg)
                raise NotificationError(error_msg) from e
            
        except NotificationError:
            # Re-raise NotificationError as-is
            raise
        except Exception as e:
            raise NotificationError(f"Email send failed: {str(e)}") from e
    
    def _send_email_digest(
        self,
        alerts: List[AlertHistory],
        message: str,
        digest_date: date
    ) -> None:
        """
        Send daily digest email.
        
        Args:
            alerts: Alerts to include
            message: Formatted digest
            digest_date: Date of digest
            
        Raises:
            NotificationError: If email send fails
        """
        if not self.smtp_user or not self.smtp_password:
            raise NotificationError("SMTP credentials not configured")
        
        if not self.to_emails:
            raise NotificationError("No recipient emails configured")
        
        try:
            msg = MIMEMultipart('alternative')
            msg['Subject'] = f"Daily Stock Alert Digest - {digest_date.strftime('%B %d, %Y')}"
            msg['From'] = self.from_email
            msg['To'] = ', '.join(self.to_emails)
            
            text_part = MIMEText(message, 'plain')
            html_part = MIMEText(self._digest_to_html(alerts, digest_date), 'html')
            
            msg.attach(text_part)
            msg.attach(html_part)
            
            # Send via SMTP with timeout to prevent hanging
            try:
                with smtplib.SMTP(self.smtp_host, self.smtp_port, timeout=30) as server:
                    server.starttls()
                    server.login(self.smtp_user, self.smtp_password)
                    server.send_message(msg)
                
                self.logger.info(f"Digest email sent to {len(self.to_emails)} recipients")
            except (OSError, smtplib.SMTPException, ConnectionError) as e:
                # Network-related errors - provide helpful message
                error_msg = f"Digest email failed (network/SMTP error): {str(e)}"
                self.logger.warning(error_msg)
                raise NotificationError(error_msg) from e
            
        except NotificationError:
            # Re-raise NotificationError as-is
            raise
        except Exception as e:
            raise NotificationError(f"Digest email failed: {str(e)}") from e
    
    def _send_slack(self, message: str, severity: str) -> None:
        """
        Send notification to Slack.
        
        Args:
            message: Message to send
            severity: Alert severity for color coding
            
        Raises:
            NotificationError: If Slack send fails
        """
        if not self.slack_webhook:
            raise NotificationError("Slack webhook URL not configured")
        
        # Color coding by severity
        color_map = {
            'CRITICAL': '#FF0000',  # Red
            'WARNING': '#FFA500',   # Orange
            'INFO': '#00FF00'       # Green
        }
        
        color = color_map.get(severity, '#808080')
        
        payload = {
            "attachments": [{
                "color": color,
                "text": message,
                "mrkdwn_in": ["text"]
            }]
        }
        
        try:
            response = requests.post(
                self.slack_webhook,
                json=payload,
                timeout=10
            )
            response.raise_for_status()
            
            self.logger.info("Slack notification sent successfully")
            
        except Exception as e:
            raise NotificationError(f"Slack send failed: {str(e)}") from e
    
    def _message_to_html(self, message: str) -> str:
        """Convert plain text message to HTML."""
        # Simple conversion - replace newlines with <br>, bold **text**
        html = message.replace('\n', '<br>')
        html = html.replace('**', '<strong>', 1)
        html = html.replace('**', '</strong>', 1)
        
        return f"""
<html>
<body style="font-family: Arial, sans-serif; line-height: 1.6;">
    <div style="max-width: 600px; margin: 0 auto; padding: 20px;">
        {html}
    </div>
</body>
</html>
"""
    
    def _digest_to_html(self, alerts: List[AlertHistory], digest_date: date) -> str:
        """
        Generate HTML version of digest.
        
        Args:
            alerts: Alerts to include
            digest_date: Date of digest
            
        Returns:
            HTML string
        """
        critical = [a for a in alerts if a.severity == 'CRITICAL']
        warnings = [a for a in alerts if a.severity == 'WARNING']
        info = [a for a in alerts if a.severity == 'INFO']
        
        html = f"""
<html>
<head>
    <style>
        body {{ font-family: Arial, sans-serif; line-height: 1.6; color: #333; }}
        .container {{ max-width: 800px; margin: 0 auto; padding: 20px; }}
        .header {{ background: #2c3e50; color: white; padding: 20px; text-align: center; }}
        .summary {{ background: #ecf0f1; padding: 15px; margin: 20px 0; border-radius: 5px; }}
        .alert-section {{ margin: 20px 0; }}
        .alert-item {{ padding: 10px; margin: 5px 0; border-left: 4px solid #ccc; }}
        .critical {{ border-left-color: #e74c3c; }}
        .warning {{ border-left-color: #f39c12; }}
        .info {{ border-left-color: #3498db; }}
        .footer {{ text-align: center; margin-top: 30px; color: #7f8c8d; font-size: 12px; }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>📈 Nigerian Stock Alert Digest</h1>
            <p>{digest_date.strftime('%B %d, %Y')}</p>
        </div>
        
        <div class="summary">
            <h3>Summary</h3>
            <p><strong>{len(alerts)}</strong> total alerts</p>
            <ul>
                <li>🚨 Critical: {len(critical)}</li>
                <li>⚠️  Warnings: {len(warnings)}</li>
                <li>ℹ️  Info: {len(info)}</li>
            </ul>
        </div>
"""
        
        if critical:
            html += '<div class="alert-section"><h3>🚨 CRITICAL ALERTS</h3>'
            for alert in critical[:10]:
                html += f"""
                <div class="alert-item critical">
                    <strong>{alert.stock.stock_code}</strong> - {alert.stock.company_name}<br>
                    {alert.message}
                </div>
"""
            html += '</div>'
        
        if warnings:
            html += '<div class="alert-section"><h3>⚠️  WARNINGS</h3>'
            for alert in warnings[:10]:
                html += f"""
                <div class="alert-item warning">
                    <strong>{alert.stock.stock_code}</strong> - {alert.stock.company_name}<br>
                    {alert.message}
                </div>
"""
            html += '</div>'
        
        if info:
            html += '<div class="alert-section"><h3>📊 INFO</h3>'
            for alert in info[:10]:
                html += f"""
                <div class="alert-item info">
                    <strong>{alert.stock.stock_code}</strong> - {alert.stock.company_name}<br>
                    {alert.message}
                </div>
"""
            html += '</div>'
        
        html += """
        <div class="footer">
            <p>💡 Nigerian Stock Pipeline - Automated Investment Alerts</p>
            <p>View full details in your Tableau dashboard or database</p>
        </div>
    </div>
</body>
</html>
"""
        
        return html
