"""
Configuration management for the Stock Pipeline system.
Features:
- Type safety with dataclasses
- Environment variable support
- Validation
- Clear defaults
"""

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Optional
from dotenv import load_dotenv

from app.utils.exceptions import MissingConfigError

# Load environment variables
load_dotenv()


@dataclass(frozen=True)
class DatabaseConfig:
    """
    Database connection configuration.
    
    Attributes:
        host: Database host
        port: Database port
        database: Database name
        user: Database user
        password: Database password
        pool_size: Connection pool size
        max_overflow: Max connections beyond pool_size
    """
    host: str
    port: int
    database: str
    user: str
    password: str
    pool_size: int = 5
    max_overflow: int = 10
    
    @property
    def connection_string(self) -> str:
        """Generate SQLAlchemy connection string."""
        return (
            f"postgresql://{self.user}:{self.password}@"
            f"{self.host}:{self.port}/{self.database}"
        )
    
    def __repr__(self) -> str:
        """String representation (hides password)."""
        return (
            f"DatabaseConfig(host={self.host}, port={self.port}, "
            f"database={self.database}, user={self.user})"
        )


@dataclass(frozen=True)
class DataSourceConfig:
    """
    Data source configuration.
    
    Attributes:
        ngx_url: URL for NGX stock data
        request_timeout: HTTP request timeout (seconds)
        max_retries: Maximum retry attempts
        user_agent: HTTP User-Agent header
    """
    ngx_url: str
    request_timeout: int = 30
    max_retries: int = 5
    user_agent: str = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )


@dataclass(frozen=True)
class AlertConfig:
    """
    Alert configuration thresholds.
    
    Attributes:
        price_change_warning: Price change % for WARNING alert
        price_change_critical: Price change % for CRITICAL alert
        volatility_multiplier: Volatility spike threshold
        volume_multiplier: Volume spike threshold
        rsi_oversold: RSI oversold threshold
        rsi_overbought: RSI overbought threshold
    """
    price_change_warning: float = 4.0
    price_change_critical: float = 8.0
    volatility_multiplier: float = 2.0
    volume_multiplier: float = 2.5
    rsi_oversold: float = 30.0
    rsi_overbought: float = 70.0


@dataclass(frozen=True)
class NotificationConfig:
    """
    Notification channel configuration.
    
    Attributes:
        email_enabled: Enable email notifications
        smtp_host: SMTP server host
        smtp_port: SMTP server port
        smtp_user: SMTP username
        smtp_password: SMTP password
        from_email: Sender email address
        to_emails: List of recipient emails
        slack_enabled: Enable Slack notifications
        slack_webhook_url: Slack incoming webhook URL
    """
    email_enabled: bool = False
    smtp_host: str = "smtp.gmail.com"
    smtp_port: int = 587
    smtp_user: Optional[str] = None
    smtp_password: Optional[str] = None
    from_email: str = "alerts@stockpipeline.com"
    to_emails: List[str] = field(default_factory=list)
    slack_enabled: bool = False
    slack_webhook_url: Optional[str] = None


@dataclass(frozen=True)
class PathConfig:
    """
    File system paths configuration.
    
    Attributes:
        project_root: Root directory of the project
        data_raw: Raw data storage path
        data_processed: Processed data storage path
        logs: Log files path
        reports: Report output path
    """
    project_root: Path
    data_raw: Path
    data_processed: Path
    logs: Path
    reports: Path
    
    @classmethod
    def from_root(cls, root: Path) -> "PathConfig":
        """Create PathConfig from project root."""
        return cls(
            project_root=root,
            data_raw=root / "data" / "raw",
            data_processed=root / "data" / "processed",
            logs=root / "logs",
            reports=root / "reports"
        )
    
    def create_directories(self) -> None:
        """Create all configured directories if they don't exist."""
        for path in [self.data_raw, self.data_processed, self.logs, self.reports]:
            try:
                path.mkdir(parents=True, exist_ok=True)
            except (PermissionError, OSError):
                # Skip directory creation if permissions are insufficient
                # This allows the app to run in restricted environments (e.g., Airflow)
                pass


@dataclass
class Settings:
    """
    Main application settings.
    
    Aggregates all configuration components with environment-based defaults.
    Supports dev, staging, and production environments.
    
    Example:
        >>> settings = Settings.load()
        >>> db = settings.database
        >>> print(db.connection_string)
    """
    database: DatabaseConfig
    data_sources: DataSourceConfig
    alerts: AlertConfig
    notifications: NotificationConfig
    paths: PathConfig
    environment: str = "development"
    debug: bool = False
    
    @classmethod
    def load(cls, env: Optional[str] = None) -> "Settings":
        """
        Load settings from environment variables.
        
        Args:
            env: Environment name (development, staging, production)
        
        Returns:
            Configured Settings instance
        
        Raises:
            MissingConfigError: If required environment variables are missing
        """
        environment = env or os.getenv("ENVIRONMENT", "development")
        
        # Database configuration
        db_host = os.getenv("POSTGRES_HOST")
        db_port = os.getenv("POSTGRES_PORT", "5432")
        db_name = os.getenv("POSTGRES_DB")
        db_user = os.getenv("POSTGRES_USER")
        db_password = os.getenv("POSTGRES_PASSWORD")
        
        if not all([db_host, db_name, db_user, db_password]):
            raise MissingConfigError(
                "Missing required database configuration. "
                "Set POSTGRES_HOST, POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD"
            )
        
        database = DatabaseConfig(
            host=db_host,
            port=int(db_port),
            database=db_name,
            user=db_user,
            password=db_password,
            pool_size=int(os.getenv("DB_POOL_SIZE", "5")),
            max_overflow=int(os.getenv("DB_MAX_OVERFLOW", "10"))
        )
        
        # Data source configuration
        ngx_url = os.getenv(
            "NGX_URL",
            "https://www.african-markets.com/en/stock-markets/ngse/listed-companies"
        )
        
        data_sources = DataSourceConfig(
            ngx_url=ngx_url,
            request_timeout=int(os.getenv("REQUEST_TIMEOUT", "30")),
            max_retries=int(os.getenv("MAX_RETRIES", "5"))
        )
        
        # Alert configuration
        alerts = AlertConfig(
            price_change_warning=float(os.getenv("ALERT_PRICE_WARNING", "4.0")),
            price_change_critical=float(os.getenv("ALERT_PRICE_CRITICAL", "8.0")),
            volatility_multiplier=float(os.getenv("ALERT_VOLATILITY_MULT", "2.0")),
            volume_multiplier=float(os.getenv("ALERT_VOLUME_MULT", "2.5")),
            rsi_oversold=float(os.getenv("ALERT_RSI_OVERSOLD", "30.0")),
            rsi_overbought=float(os.getenv("ALERT_RSI_OVERBOUGHT", "70.0"))
        )
        
        # Notification configuration
        to_emails_str = os.getenv("NOTIFICATION_EMAILS", "")
        to_emails = [e.strip() for e in to_emails_str.split(",") if e.strip()]
        
        notifications = NotificationConfig(
            email_enabled=os.getenv("NOTIFICATION_EMAIL_ENABLED", "false").lower() == "true",
            smtp_host=os.getenv("SMTP_HOST", "smtp.gmail.com"),
            smtp_port=int(os.getenv("SMTP_PORT", "587")),
            smtp_user=os.getenv("SMTP_USER"),
            smtp_password=os.getenv("SMTP_PASSWORD"),
            from_email=os.getenv("NOTIFICATION_FROM_EMAIL", "alerts@stockpipeline.com"),
            to_emails=to_emails,
            slack_enabled=os.getenv("NOTIFICATION_SLACK_ENABLED", "false").lower() == "true",
            slack_webhook_url=os.getenv("SLACK_WEBHOOK_URL")
        )
        
        # Path configuration
        project_root = Path(os.getenv("PROJECT_ROOT", "/home/Stock_pipeline"))
        paths = PathConfig.from_root(project_root)
        paths.create_directories()
        
        # Debug mode
        debug = os.getenv("DEBUG", "false").lower() == "true"
        
        return cls(
            database=database,
            data_sources=data_sources,
            alerts=alerts,
            notifications=notifications,
            paths=paths,
            environment=environment,
            debug=debug
        )
    
    def __repr__(self) -> str:
        """String representation."""
        return (
            f"Settings(environment={self.environment}, "
            f"database={self.database}, "
            f"debug={self.debug})"
        )


# Global settings instance (lazy-loaded)
_settings: Optional[Settings] = None


def get_settings() -> Settings:
    """
    Get global settings instance (singleton pattern).
    
    Returns:
        Settings instance
    
    Example:
        >>> from app.config.settings import get_settings
        >>> settings = get_settings()
        >>> db_url = settings.database.connection_string
    """
    global _settings
    if _settings is None:
        _settings = Settings.load()
    return _settings
