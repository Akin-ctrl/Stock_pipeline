"""
Repository for alert operations (alert_rules and alert_history).

Handles all database operations related to investment alerts and rules.
"""

from typing import Optional, List, Dict, Any
from datetime import date, datetime, timedelta
from sqlalchemy.orm import Session, joinedload
from sqlalchemy import and_, desc, func

from app.repositories.base import BaseRepository
from app.models import AlertRule, AlertHistory, DimStock
from app.utils.exceptions import RecordNotFoundError


class AlertRepository(BaseRepository[AlertHistory]):
    """
    Repository for alert operations.
    
    Provides methods for creating alerts, managing alert rules,
    and querying alert history with filtering.
    """
    
    def __init__(self, session: Session):
        """
        Initialize alert repository.
        
        Args:
            session: Active database session
        """
        super().__init__(AlertHistory, session)
    
    # ========================================================================
    # Alert Rules Methods
    # ========================================================================
    
    def get_all_rules(self, active_only: bool = True) -> List[AlertRule]:
        """
        Get all alert rules.
        
        Args:
            active_only: If True, return only active rules
            
        Returns:
            List of alert rules
        """
        query = self.session.query(AlertRule)
        
        if active_only:
            query = query.filter(AlertRule.is_active == True)
        
        return query.order_by(AlertRule.rule_id).all()
    
    def get_rule_by_name(self, rule_name: str) -> Optional[AlertRule]:
        """
        Get alert rule by name.
        
        Args:
            rule_name: Rule name
            
        Returns:
            AlertRule or None
        """
        return (
            self.session.query(AlertRule)
            .filter(AlertRule.rule_name == rule_name)
            .first()
        )
    
    def get_rules_by_type(self, rule_type: str) -> List[AlertRule]:
        """
        Get all rules of a specific type.
        
        Args:
            rule_type: Rule type (PRICE_MOVEMENT, MA_CROSSOVER, etc.)
            
        Returns:
            List of matching rules
        """
        return (
            self.session.query(AlertRule)
            .filter(
                and_(
                    AlertRule.rule_type == rule_type,
                    AlertRule.is_active == True
                )
            )
            .all()
        )
    
    def get_rule_by_id(self, rule_id: int) -> Optional[AlertRule]:
        """
        Get alert rule by ID.
        
        Args:
            rule_id: Rule identifier
            
        Returns:
            AlertRule or None
        """
        return self.session.query(AlertRule).filter(AlertRule.rule_id == rule_id).first()
    
    def create_rule(self, rule: AlertRule) -> int:
        """
        Create a new alert rule.
        
        Args:
            rule: AlertRule instance
            
        Returns:
            Created rule ID
        """
        self.session.add(rule)
        self.session.flush()
        return rule.rule_id
    
    def activate_rule(self, rule_name: str) -> AlertRule:
        """
        Activate an alert rule.
        
        Args:
            rule_name: Rule name
            
        Returns:
            Updated rule
            
        Raises:
            RecordNotFoundError: If rule not found
        """
        rule = self.get_rule_by_name(rule_name)
        if not rule:
            raise RecordNotFoundError(f"Rule not found: {rule_name}")
        
        rule.is_active = True
        self.session.flush()
        return rule
    
    def deactivate_rule(self, rule_name: str) -> AlertRule:
        """
        Deactivate an alert rule.
        
        Args:
            rule_name: Rule name
            
        Returns:
            Updated rule
        """
        rule = self.get_rule_by_name(rule_name)
        if not rule:
            raise RecordNotFoundError(f"Rule not found: {rule_name}")
        
        rule.is_active = False
        self.session.flush()
        return rule
    
    # ========================================================================
    # Alert History Methods
    # ========================================================================
    
    def create_alert(
        self,
        stock_id: int,
        rule_id: int,
        alert_date: date,
        alert_type: str,
        severity: str,
        message: str,
        trigger_value: Optional[float] = None
    ) -> AlertHistory:
        """
        Create a new alert record.
        
        Args:
            stock_id: Stock identifier
            rule_id: Alert rule identifier
            alert_date: Date alert triggered
            alert_type: Type of alert
            severity: 'INFO', 'WARNING', or 'CRITICAL'
            message: Alert message
            trigger_value: Value that triggered alert
            
        Returns:
            Created alert
        """
        return self.create(
            stock_id=stock_id,
            rule_id=rule_id,
            alert_date=alert_date,
            alert_type=alert_type,
            severity=severity,
            message=message,
            trigger_value=trigger_value,
            is_resolved=False
        )
    
    def get_active_alerts(
        self,
        stock_id: Optional[int] = None,
        severity: Optional[str] = None
    ) -> List[AlertHistory]:
        """
        Get unresolved alerts.
        
        Args:
            stock_id: Filter by stock (None for all)
            severity: Filter by severity (None for all)
            
        Returns:
            List of active alerts with stock and rule info
        """
        query = (
            self.session.query(AlertHistory)
            .options(
                joinedload(AlertHistory.stock),
                joinedload(AlertHistory.rule)
            )
            .filter(AlertHistory.is_resolved == False)
        )
        
        if stock_id:
            query = query.filter(AlertHistory.stock_id == stock_id)
        if severity:
            query = query.filter(AlertHistory.severity == severity)
        
        return query.order_by(desc(AlertHistory.alert_timestamp)).all()
    
    def get_alerts_by_stock_code(
        self,
        stock_code: str,
        days: int = 30,
        unresolved_only: bool = False
    ) -> List[AlertHistory]:
        """
        Get alerts for a specific stock.
        
        Args:
            stock_code: Stock ticker
            days: Look back N days
            unresolved_only: If True, return only unresolved alerts
            
        Returns:
            List of alerts for the stock
        """
        cutoff_date = date.today() - timedelta(days=days)
        
        query = (
            self.session.query(AlertHistory)
            .join(DimStock)
            .options(joinedload(AlertHistory.rule))
            .filter(
                and_(
                    DimStock.stock_code == stock_code.upper(),
                    AlertHistory.alert_date >= cutoff_date
                )
            )
        )
        
        if unresolved_only:
            query = query.filter(AlertHistory.is_resolved == False)
        
        return query.order_by(desc(AlertHistory.alert_timestamp)).all()
    
    def get_recent_alerts(
        self,
        days: int = 7,
        severity: Optional[str] = None
    ) -> List[AlertHistory]:
        """
        Get recent alerts across all stocks.
        
        Args:
            days: Look back N days
            severity: Filter by severity
            
        Returns:
            List of recent alerts
        """
        cutoff_date = date.today() - timedelta(days=days)
        
        query = (
            self.session.query(AlertHistory)
            .options(
                joinedload(AlertHistory.stock),
                joinedload(AlertHistory.rule)
            )
            .filter(AlertHistory.alert_date >= cutoff_date)
        )
        
        if severity:
            query = query.filter(AlertHistory.severity == severity)
        
        return query.order_by(desc(AlertHistory.alert_timestamp)).all()
    
    def get_alerts_by_date_range(
        self,
        start_date: date,
        end_date: date,
        stock_id: Optional[int] = None
    ) -> List[AlertHistory]:
        """
        Get alerts within a date range.
        
        Args:
            start_date: Start date (inclusive)
            end_date: End date (inclusive)
            stock_id: Optional stock filter
            
        Returns:
            List of alerts in date range
        """
        query = (
            self.session.query(AlertHistory)
            .options(
                joinedload(AlertHistory.stock),
                joinedload(AlertHistory.rule)
            )
            .filter(
                and_(
                    AlertHistory.alert_date >= start_date,
                    AlertHistory.alert_date <= end_date
                )
            )
        )
        
        if stock_id:
            query = query.filter(AlertHistory.stock_id == stock_id)
        
        return query.order_by(desc(AlertHistory.alert_timestamp)).all()
    
    def resolve_alert(
        self,
        alert_id: int,
        resolution_notes: Optional[str] = None
    ) -> AlertHistory:
        """
        Mark an alert as resolved.
        
        Args:
            alert_id: Alert identifier
            resolution_notes: Optional notes about resolution
            
        Returns:
            Updated alert
            
        Raises:
            RecordNotFoundError: If alert not found
        """
        alert = self.get_by_id(alert_id)
        if not alert:
            raise RecordNotFoundError(f"Alert not found: {alert_id}")
        
        return self.update(
            alert,
            is_resolved=True,
            resolved_at=datetime.now(),
            resolution_notes=resolution_notes
        )
    
    def bulk_resolve_alerts(
        self,
        stock_id: int,
        alert_date: date
    ) -> int:
        """
        Resolve all alerts for a stock on a specific date.
        
        Args:
            stock_id: Stock identifier
            alert_date: Date of alerts
            
        Returns:
            Number of alerts resolved
        """
        updated = (
            self.session.query(AlertHistory)
            .filter(
                and_(
                    AlertHistory.stock_id == stock_id,
                    AlertHistory.alert_date == alert_date,
                    AlertHistory.is_resolved == False
                )
            )
            .update({
                'is_resolved': True,
                'resolved_at': datetime.now()
            })
        )
        
        self.session.flush()
        return updated
    
    def alert_exists(
        self,
        stock_id: int,
        rule_id: int,
        alert_date: date
    ) -> bool:
        """
        Check if alert already exists (for deduplication).
        
        Args:
            stock_id: Stock identifier
            rule_id: Rule identifier
            alert_date: Date of alert
            
        Returns:
            True if alert exists
        """
        return self.exists(
            stock_id=stock_id,
            rule_id=rule_id,
            alert_date=alert_date
        )
    
    def get_alert_summary(self, days: int = 7) -> Dict[str, Any]:
        """
        Get summary statistics for recent alerts.
        
        Args:
            days: Look back N days
            
        Returns:
            Dict with alert counts by severity and resolution status
        """
        cutoff_date = date.today() - timedelta(days=days)
        
        # Count by severity
        severity_counts = (
            self.session.query(
                AlertHistory.severity,
                func.count(AlertHistory.alert_id)
            )
            .filter(AlertHistory.alert_date >= cutoff_date)
            .group_by(AlertHistory.severity)
            .all()
        )
        
        # Count resolved vs unresolved
        resolution_counts = (
            self.session.query(
                AlertHistory.is_resolved,
                func.count(AlertHistory.alert_id)
            )
            .filter(AlertHistory.alert_date >= cutoff_date)
            .group_by(AlertHistory.is_resolved)
            .all()
        )
        
        return {
            'severity': {sev: count for sev, count in severity_counts},
            'resolved': dict(resolution_counts),
            'period_days': days
        }
    
    def mark_notification_sent(self, alert_id: int, channels: str) -> AlertHistory:
        """
        Mark alert as notification sent.
        
        Args:
            alert_id: Alert identifier
            channels: Notification channels used (e.g., 'email,slack')
            
        Returns:
            Updated alert
        """
        alert = self.get_by_id(alert_id)
        if not alert:
            raise RecordNotFoundError(f"Alert not found: {alert_id}")
        
        return self.update(
            alert,
            notification_sent=True,
            notification_channels=channels
        )
