"""
Simple email notification test script.
"""
import sys
from pathlib import Path
from datetime import datetime

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from config.settings import Settings
from services.alerts import AlertNotifier
from utils import get_logger


def test_email():
    """Test email notification with a sample message."""
    logger = get_logger("test_email")
    
    # Load settings
    settings = Settings.load()
    
    logger.info("=" * 80)
    logger.info("EMAIL NOTIFICATION TEST")
    logger.info("=" * 80)
    logger.info(f"\nConfiguration:")
    logger.info(f"  SMTP Host: {settings.notifications.smtp_host}")
    logger.info(f"  SMTP Port: {settings.notifications.smtp_port}")
    logger.info(f"  From Email: {settings.notifications.from_email}")
    logger.info(f"  To Emails: {', '.join(settings.notifications.to_emails)}")
    logger.info(f"  Email Enabled: {settings.notifications.email_enabled}")
    
    if not settings.notifications.email_enabled:
        logger.warning("\n⚠️  Email notifications are DISABLED in .env")
        logger.info("Set NOTIFICATION_EMAIL_ENABLED=true to enable")
        return False
    
    if 'your_app_password_here' in settings.notifications.smtp_password:
        logger.error("\n❌ SMTP password not configured!")
        logger.info("Update SMTP_PASSWORD in .env with your Gmail App Password")
        return False
    
    if 'example.com' in ','.join(settings.notifications.to_emails):
        logger.warning("\n⚠️  Using example email addresses")
        logger.info("Update NOTIFICATION_EMAILS in .env with real email addresses")
    
    # Initialize notifier
    logger.info("\n" + "=" * 80)
    logger.info("SENDING TEST EMAIL")
    logger.info("=" * 80)
    
    notifier = AlertNotifier(settings.notifications)
    
    # Create test email content
    subject = "📧 Nigerian Stock Pipeline - Email Test"
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    smtp_host = settings.notifications.smtp_host
    smtp_port = settings.notifications.smtp_port
    from_email = settings.notifications.from_email
    
    html_body = f"""
    <html>
    <head>
        <style>
            body {{ font-family: Arial, sans-serif; }}
            .header {{ background-color: #1e3a8a; color: white; padding: 20px; }}
            .content {{ padding: 20px; }}
            .success {{ color: #10b981; font-weight: bold; }}
            .info {{ background-color: #f3f4f6; padding: 15px; border-left: 4px solid #3b82f6; }}
        </style>
    </head>
    <body>
        <div class="header">
            <h1>✅ Email Notification Test</h1>
        </div>
        <div class="content">
            <p class="success">Congratulations! Your email notification system is working correctly.</p>
            
            <div class="info">
                <h3>📊 System Information</h3>
                <ul>
                    <li><strong>Test Time:</strong> {timestamp}</li>
                    <li><strong>SMTP Server:</strong> {smtp_host}:{smtp_port}</li>
                    <li><strong>From:</strong> {from_email}</li>
                </ul>
            </div>
            
            <p>This confirms that your Nigerian Stock Pipeline can successfully send email alerts for:</p>
            <ul>
                <li>Price spikes and drops</li>
                <li>Volume anomalies</li>
                <li>RSI overbought/oversold conditions</li>
                <li>Daily digest summaries</li>
                <li>Investment recommendations</li>
            </ul>
            
            <p style="color: #6b7280; margin-top: 30px;">
                <em>This is an automated test message from Nigerian Stock Pipeline</em>
            </p>
        </div>
    </body>
    </html>
    """
    
    try:
        # Send test email directly via SMTP
        import smtplib
        from email.mime.text import MIMEText
        from email.mime.multipart import MIMEMultipart
        
        logger.info("\nAttempting to send email...")
        
        msg = MIMEMultipart('alternative')
        msg['Subject'] = subject
        msg['From'] = settings.notifications.from_email
        msg['To'] = ', '.join(settings.notifications.to_emails)
        
        html_part = MIMEText(html_body, 'html')
        msg.attach(html_part)
        
        # Send via SMTP
        with smtplib.SMTP(settings.notifications.smtp_host, settings.notifications.smtp_port) as server:
            server.starttls()
            server.login(settings.notifications.smtp_user, settings.notifications.smtp_password)
            server.send_message(msg)
        
        logger.info("\n" + "=" * 80)
        logger.info("✅ SUCCESS! Email sent successfully")
        logger.info("=" * 80)
        logger.info(f"\nCheck your inbox: {', '.join(settings.notifications.to_emails)}")
        logger.info("(Check spam folder if you don't see it)")
        return True
        
    except Exception as e:
        logger.error("\n" + "=" * 80)
        logger.error("❌ EMAIL FAILED")
        logger.error("=" * 80)
        logger.error(f"\nError: {str(e)}")
        
        if "Authentication" in str(e) or "Username and Password" in str(e):
            logger.error("\n📝 Authentication failed - Check these settings:")
            logger.error("  1. SMTP_USER should be your full Gmail address")
            logger.error("  2. SMTP_PASSWORD should be your 16-character App Password (not regular password)")
            logger.error("  3. Generate App Password at: https://myaccount.google.com/apppasswords")
        elif "Connection refused" in str(e):
            logger.error("\n📝 Connection refused - Check these settings:")
            logger.error("  1. SMTP_HOST should be smtp.gmail.com")
            logger.error("  2. SMTP_PORT should be 587")
            logger.error("  3. Check firewall/network settings")
        else:
            logger.error("\n📝 Unknown error - Check your .env configuration")
        
        return False


if __name__ == "__main__":
    try:
        success = test_email()
        sys.exit(0 if success else 1)
    except Exception as e:
        logger = get_logger("test_email")
        logger.error(f"Test failed: {str(e)}")
        sys.exit(1)
