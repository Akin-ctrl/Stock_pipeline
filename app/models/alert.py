"""
Alert configuration and history models.
Features:
- Type hints
- Clear semantics
- Business logic
"""

from datetime import datetime
from typing import Optional
from decimal import Decimal
from sqlalchemy import (
    Column, BigInteger, Integer, String, Date, 
    Boolean, Text, TIMESTAMP, CheckConstraint, 
    ForeignKey, Index, Numeric
)
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func

from app.models.base import Base, TimestampMixin


class AlertRule(Base, TimestampMixin):
    """
    Alert rule configuration table.
    
    Defines investment alert rules that trigger notifications.
    
    Attributes:
        rule_id: Primary key
        rule_name: Unique rule name
        rule_type: Type of rule (PRICE_MOVEMENT, MA_CROSSOVER, etc.)
        condition_sql: SQL expression for evaluation
        threshold_value: Numeric threshold
        severity: Alert severity (INFO, WARNING, CRITICAL)
        is_active: Whether rule is enabled
        description: Human-readable description
        created_at: Creation timestamp
        updated_at: Last update timestamp
    
    Example:
        >>> rule = AlertRule(
        >>>     rule_name='Daily_Change_Significant',
        >>>     rule_type='PRICE_MOVEMENT',
        >>>     threshold_value=Decimal('4.0'),
        >>>     severity='WARNING'
        >>> )
    """
    __tablename__ = 'alert_rules'
    
    rule_id = Column(Integer, primary_key=True, autoincrement=True)
    rule_name = Column(String(100), unique=True, nullable=False)
    rule_type = Column(String(50), nullable=False)
    condition_sql = Column(Text)
    threshold_value = Column(Numeric(10, 4))
    severity = Column(String(20), default='INFO')
    is_active = Column(Boolean, default=True, nullable=False)
    description = Column(Text)
    created_at = Column(TIMESTAMP, server_default=func.now())
    updated_at = Column(TIMESTAMP, server_default=func.now(), onupdate=func.now())
    
    # Constraints
    __table_args__ = (
        CheckConstraint(
            "severity IN ('INFO', 'WARNING', 'CRITICAL')",
            name='chk_severity'
        ),
        CheckConstraint(
            "rule_type IN ('PRICE_MOVEMENT', 'MA_CROSSOVER', 'VOLATILITY', "
            "'VOLUME_SPIKE', 'RSI', 'MACD', 'CUSTOM')",
            name='chk_rule_type'
        ),
    )
    
    # Relationships
    alerts = relationship("AlertHistory", back_populates="rule")
    
    def __repr__(self) -> str:
        return (
            f"AlertRule(id={self.rule_id}, name={self.rule_name!r}, "
            f"type={self.rule_type}, severity={self.severity})"
        )
    
    @property
    def is_critical(self) -> bool:
        """Check if rule generates critical alerts."""
        return self.severity == 'CRITICAL'
    
    def activate(self) -> None:
        """Enable the alert rule."""
        self.is_active = True
    
    def deactivate(self) -> None:
        """Disable the alert rule."""
        self.is_active = False


class AlertHistory(Base):
    """
    Alert history table (triggered alerts).
    
    Stores all investment alerts that have been triggered.
    
    Attributes:
        alert_id: Primary key
        stock_id: Foreign key to dim_stocks
        rule_id: Foreign key to alert_rules
        alert_date: Date alert was triggered
        alert_timestamp: Precise timestamp
        alert_type: Type of alert
        severity: Alert severity
        trigger_value: Value that triggered the alert
        message: Alert message
        is_resolved: Whether alert has been addressed
        resolved_at: When alert was resolved
        resolution_notes: Notes on resolution
        notification_sent: Whether notification was sent
        notification_channels: Channels used (email, slack, etc.)
    
    Example:
        >>> alert = AlertHistory(
        >>>     stock_id=1,
        >>>     rule_id=1,
        >>>     alert_date=date(2025, 12, 6),
        >>>     alert_type='PRICE_MOVEMENT',
        >>>     severity='WARNING',
        >>>     trigger_value=Decimal('5.2'),
        >>>     message='DANGCEM: +5.2% daily move'
        >>> )
    """
    __tablename__ = 'alert_history'
    
    alert_id = Column(BigInteger, primary_key=True, autoincrement=True)
    stock_id = Column(Integer, ForeignKey('dim_stocks.stock_id', ondelete='CASCADE'), nullable=False)
    rule_id = Column(Integer, ForeignKey('alert_rules.rule_id', ondelete='CASCADE'), nullable=False)
    alert_date = Column(Date, nullable=False, index=True)
    alert_timestamp = Column(TIMESTAMP, server_default=func.now())
    
    alert_type = Column(String(50), nullable=False)
    severity = Column(String(20), nullable=False)
    trigger_value = Column(Numeric(18, 4))
    message = Column(Text, nullable=False)
    
    is_resolved = Column(Boolean, default=False, nullable=False)
    resolved_at = Column(TIMESTAMP)
    resolution_notes = Column(Text)
    
    notification_sent = Column(Boolean, default=False)
    notification_channels = Column(String(100))
    
    # Constraints
    __table_args__ = (
        CheckConstraint(
            "severity IN ('INFO', 'WARNING', 'CRITICAL')",
            name='chk_alert_severity'
        ),
        Index('idx_stock_alerts', 'stock_id', 'alert_date'),
        Index('idx_unresolved', 'is_resolved', 'severity'),
    )
    
    # Relationships
    stock = relationship("DimStock", back_populates="alerts")
    rule = relationship("AlertRule", back_populates="alerts")
    
    def __repr__(self) -> str:
        return (
            f"AlertHistory(id={self.alert_id}, stock_id={self.stock_id}, "
            f"date={self.alert_date}, severity={self.severity}, "
            f"resolved={self.is_resolved})"
        )
    
    @property
    def is_critical(self) -> bool:
        """Check if alert is critical severity."""
        return self.severity == 'CRITICAL'
    
    @property
    def is_pending(self) -> bool:
        """Check if alert is unresolved."""
        return not self.is_resolved
    
    @property
    def age_days(self) -> Optional[int]:
        """Get alert age in days."""
        if self.alert_date:
            delta = datetime.now().date() - self.alert_date
            return delta.days
        return None
    
    def resolve(self, notes: Optional[str] = None) -> None:
        """
        Mark alert as resolved.
        
        Args:
            notes: Optional resolution notes
        """
        self.is_resolved = True
        self.resolved_at = datetime.now()
        if notes:
            self.resolution_notes = notes
    
    def mark_notification_sent(self, channels: str) -> None:
        """
        Mark that notification has been sent.
        
        Args:
            channels: Comma-separated list of channels (e.g., 'email,slack')
        """
        self.notification_sent = True
        self.notification_channels = channels
    
    def should_notify(self) -> bool:
        """
        Check if alert should trigger notification.
        
        Returns:
            True if notification should be sent
        """
        return (
            not self.notification_sent and
            not self.is_resolved and
            self.severity in ('WARNING', 'CRITICAL')
        )
