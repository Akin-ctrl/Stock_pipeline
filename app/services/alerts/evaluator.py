"""
Alert rule evaluator.

Evaluates alert rules against stock data and generates alerts.
Supports various rule types: price movements, MA crossovers, RSI extremes, volatility, volume.
"""

from typing import Dict, List, Optional, Tuple
from datetime import date, datetime, timedelta
from dataclasses import dataclass
import pandas as pd
from sqlalchemy.orm import Session

from app.models import AlertRule, AlertHistory
from app.repositories import AlertRepository, StockRepository, PriceRepository, IndicatorRepository
from app.utils import get_logger
from app.config.database import get_session


@dataclass
class AlertEvaluationResult:
    """
    Result of evaluating alert rules.
    
    Attributes:
        alerts_generated: Number of new alerts generated
        rules_evaluated: Number of rules evaluated
        stocks_checked: Number of stocks checked
        alerts: List of generated alert dictionaries
    """
    alerts_generated: int
    rules_evaluated: int
    stocks_checked: int
    alerts: List[Dict]


class AlertEvaluator:
    """
    Evaluates alert rules against stock data.
    
    Supports rule types:
    - PRICE_MOVEMENT: Daily price changes exceeding thresholds
    - MA_CROSSOVER: Moving average crossovers (bullish/bearish)
    - RSI: RSI oversold/overbought conditions
    - VOLATILITY: High volatility periods
    - VOLUME_SPIKE: Unusual volume activity
    """
    
    def __init__(self, session: Optional[Session] = None):
        """
        Initialize evaluator with repositories.
        
        Args:
            session: Optional database session (for testing). If not provided, creates new session.
        """
        self.logger = get_logger("alert_evaluator")
        self.session = session if session is not None else get_session()
        self.alert_repo = AlertRepository(self.session)
        self.stock_repo = StockRepository(self.session)
        self.price_repo = PriceRepository(self.session)
        self.indicator_repo = IndicatorRepository(self.session)
    
    def evaluate_all_rules(
        self,
        evaluation_date: Optional[date] = None
    ) -> AlertEvaluationResult:
        """
        Evaluate all active alert rules.
        
        Args:
            evaluation_date: Date to evaluate rules for (default: today)
            
        Returns:
            AlertEvaluationResult with statistics and generated alerts
        """
        if evaluation_date is None:
            evaluation_date = date.today()
        
        self.logger.info(
            f"Starting alert evaluation for {evaluation_date}",
            extra={"evaluation_date": str(evaluation_date)}
        )
        
        # Get all active rules
        rules = self.alert_repo.get_all_rules(active_only=True)
        
        if not rules:
            self.logger.warning("No active alert rules found")
            return AlertEvaluationResult(0, 0, 0, [])
        
        # Get all active stocks
        stocks = self.stock_repo.get_all_active()
        
        if not stocks:
            self.logger.warning("No active stocks found")
            return AlertEvaluationResult(0, len(rules), 0, [])
        
        self.logger.info(
            f"Evaluating {len(rules)} rules for {len(stocks)} stocks",
            extra={"rules": len(rules), "stocks": len(stocks)}
        )
        
        # Evaluate each rule
        all_alerts = []
        for rule in rules:
            alerts = self._evaluate_rule(rule, stocks, evaluation_date)
            all_alerts.extend(alerts)
        
        result = AlertEvaluationResult(
            alerts_generated=len(all_alerts),
            rules_evaluated=len(rules),
            stocks_checked=len(stocks),
            alerts=all_alerts
        )
        
        self.logger.info(
            f"Alert evaluation complete: {result.alerts_generated} alerts generated",
            extra={
                "alerts": result.alerts_generated,
                "rules": result.rules_evaluated,
                "stocks": result.stocks_checked
            }
        )
        
        return result
    
    def _evaluate_rule(
        self,
        rule: AlertRule,
        stocks: List,
        evaluation_date: date
    ) -> List[Dict]:
        """
        Evaluate a single rule against all stocks.
        
        Args:
            rule: AlertRule to evaluate
            stocks: List of stock objects
            evaluation_date: Date to evaluate for
            
        Returns:
            List of alert dictionaries
        """
        alerts = []
        
        for stock in stocks:
            try:
                alert = self._check_rule_for_stock(rule, stock, evaluation_date)
                if alert:
                    alerts.append(alert)
            except Exception as e:
                self.logger.error(
                    f"Error evaluating rule {rule.rule_name} for {stock.stock_code}",
                    extra={
                        "rule": rule.rule_name,
                        "stock": stock.stock_code,
                        "error": str(e)
                    }
                )
        
        if alerts:
            self.logger.info(
                f"Rule {rule.rule_name} generated {len(alerts)} alerts",
                extra={"rule": rule.rule_name, "alerts": len(alerts)}
            )
        
        return alerts
    
    def _check_rule_for_stock(
        self,
        rule: AlertRule,
        stock,
        evaluation_date: date
    ) -> Optional[Dict]:
        """
        Check if a rule is triggered for a specific stock.
        
        Args:
            rule: AlertRule to check
            stock: Stock object
            evaluation_date: Date to check
            
        Returns:
            Alert dict if triggered, None otherwise
        """
        # Check for existing alert (avoid duplicates)
        if self.alert_repo.alert_exists(stock.stock_id, rule.rule_id, evaluation_date):
            return None
        
        # Route to appropriate handler based on rule type
        if rule.rule_type == 'PRICE_MOVEMENT':
            return self._check_price_movement(rule, stock, evaluation_date)
        elif rule.rule_type == 'MA_CROSSOVER':
            return self._check_ma_crossover(rule, stock, evaluation_date)
        elif rule.rule_type == 'RSI':
            return self._check_rsi(rule, stock, evaluation_date)
        elif rule.rule_type == 'VOLATILITY':
            return self._check_volatility(rule, stock, evaluation_date)
        elif rule.rule_type == 'VOLUME_SPIKE':
            return self._check_volume_spike(rule, stock, evaluation_date)
        else:
            self.logger.warning(
                f"Unknown rule type: {rule.rule_type}",
                extra={"rule": rule.rule_name, "type": rule.rule_type}
            )
            return None
    
    def _check_price_movement(
        self,
        rule: AlertRule,
        stock,
        evaluation_date: date
    ) -> Optional[Dict]:
        """Check for significant price movements."""
        # Get latest price
        price = self.price_repo.get_latest_by_code(stock.stock_code, evaluation_date)
        
        if not price or price.change_1d_pct is None:
            return None
        
        # Parse threshold from rule parameters
        threshold = float(rule.parameters.get('threshold', 5.0))
        
        # Check if change exceeds threshold
        if abs(price.change_1d_pct) >= threshold:
            direction = "increased" if price.change_1d_pct > 0 else "decreased"
            message = (
                f"{stock.stock_code} {direction} by {abs(price.change_1d_pct):.2f}% "
                f"(threshold: {threshold}%)"
            )
            
            return {
                'stock_id': stock.stock_id,
                'rule_id': rule.rule_id,
                'alert_date': evaluation_date,
                'severity': rule.severity,
                'message': message,
                'metadata': {
                    'change_pct': price.change_1d_pct,
                    'close_price': float(price.close_price),
                    'threshold': threshold
                }
            }
        
        return None
    
    def _check_ma_crossover(
        self,
        rule: AlertRule,
        stock,
        evaluation_date: date
    ) -> Optional[Dict]:
        """Check for moving average crossovers."""
        # Get latest indicator
        indicator = self.indicator_repo.get_latest_by_code(stock.stock_code, evaluation_date)
        
        if not indicator or not indicator.ma_crossover_signal:
            return None
        
        # Check if crossover matches rule criteria
        signal_type = rule.rule_name.upper()
        
        if 'BULLISH' in signal_type and indicator.ma_crossover_signal == 'BULLISH':
            message = f"{stock.stock_code}: Bullish crossover detected (Golden Cross)"
            return {
                'stock_id': stock.stock_id,
                'rule_id': rule.rule_id,
                'alert_date': evaluation_date,
                'severity': rule.severity,
                'message': message,
                'metadata': {
                    'signal': 'BULLISH',
                    'ma_short': float(indicator.ma_20) if indicator.ma_20 else None,
                    'ma_long': float(indicator.ma_50) if indicator.ma_50 else None
                }
            }
        elif 'BEARISH' in signal_type and indicator.ma_crossover_signal == 'BEARISH':
            message = f"{stock.stock_code}: Bearish crossover detected (Death Cross)"
            return {
                'stock_id': stock.stock_id,
                'rule_id': rule.rule_id,
                'alert_date': evaluation_date,
                'severity': rule.severity,
                'message': message,
                'metadata': {
                    'signal': 'BEARISH',
                    'ma_short': float(indicator.ma_20) if indicator.ma_20 else None,
                    'ma_long': float(indicator.ma_50) if indicator.ma_50 else None
                }
            }
        
        return None
    
    def _check_rsi(
        self,
        rule: AlertRule,
        stock,
        evaluation_date: date
    ) -> Optional[Dict]:
        """Check for RSI oversold/overbought conditions."""
        # Get latest indicator
        indicator = self.indicator_repo.get_latest_by_code(stock.stock_code, evaluation_date)
        
        if not indicator or indicator.rsi is None:
            return None
        
        rsi_value = float(indicator.rsi)
        
        # Check for oversold
        if 'OVERSOLD' in rule.rule_name.upper():
            threshold = float(rule.parameters.get('oversold', 30))
            if rsi_value <= threshold:
                message = f"{stock.stock_code}: RSI oversold at {rsi_value:.2f} (threshold: {threshold})"
                return {
                    'stock_id': stock.stock_id,
                    'rule_id': rule.rule_id,
                    'alert_date': evaluation_date,
                    'severity': rule.severity,
                    'message': message,
                    'metadata': {
                        'rsi': rsi_value,
                        'threshold': threshold,
                        'condition': 'OVERSOLD'
                    }
                }
        
        # Check for overbought
        elif 'OVERBOUGHT' in rule.rule_name.upper():
            threshold = float(rule.parameters.get('overbought', 70))
            if rsi_value >= threshold:
                message = f"{stock.stock_code}: RSI overbought at {rsi_value:.2f} (threshold: {threshold})"
                return {
                    'stock_id': stock.stock_id,
                    'rule_id': rule.rule_id,
                    'alert_date': evaluation_date,
                    'severity': rule.severity,
                    'message': message,
                    'metadata': {
                        'rsi': rsi_value,
                        'threshold': threshold,
                        'condition': 'OVERBOUGHT'
                    }
                }
        
        return None
    
    def _check_volatility(
        self,
        rule: AlertRule,
        stock,
        evaluation_date: date
    ) -> Optional[Dict]:
        """Check for high volatility periods."""
        # Get latest indicator
        indicator = self.indicator_repo.get_latest_by_code(stock.stock_code, evaluation_date)
        
        if not indicator or indicator.volatility_30 is None:
            return None
        
        volatility = float(indicator.volatility_30)
        threshold = float(rule.parameters.get('threshold', 0.3))
        
        if volatility >= threshold:
            message = (
                f"{stock.stock_code}: High volatility detected at {volatility:.2%} "
                f"(threshold: {threshold:.2%})"
            )
            return {
                'stock_id': stock.stock_id,
                'rule_id': rule.rule_id,
                'alert_date': evaluation_date,
                'severity': rule.severity,
                'message': message,
                'metadata': {
                    'volatility': volatility,
                    'threshold': threshold
                }
            }
        
        return None
    
    def _check_volume_spike(
        self,
        rule: AlertRule,
        stock,
        evaluation_date: date
    ) -> Optional[Dict]:
        """Check for unusual volume spikes."""
        # Get recent prices for volume comparison
        end_date = evaluation_date
        start_date = evaluation_date - timedelta(days=30)
        
        prices = self.price_repo.get_price_history(
            stock.stock_id,
            start_date=start_date,
            end_date=end_date
        )
        
        if len(prices) < 5:  # Need enough data
            return None
        
        # Get latest price
        latest = prices[-1]
        if latest.volume is None:
            return None
        
        # Calculate average volume (excluding today)
        historical_volumes = [p.volume for p in prices[:-1] if p.volume is not None]
        if not historical_volumes:
            return None
        
        avg_volume = sum(historical_volumes) / len(historical_volumes)
        
        # Check for spike
        multiplier = float(rule.parameters.get('multiplier', 2.0))
        if latest.volume >= avg_volume * multiplier:
            message = (
                f"{stock.stock_code}: Volume spike detected - {latest.volume:,} "
                f"({latest.volume/avg_volume:.1f}x average)"
            )
            return {
                'stock_id': stock.stock_id,
                'rule_id': rule.rule_id,
                'alert_date': evaluation_date,
                'severity': rule.severity,
                'message': message,
                'metadata': {
                    'volume': int(latest.volume),
                    'avg_volume': int(avg_volume),
                    'multiplier': latest.volume / avg_volume
                }
            }
        
        return None
    
    def save_alerts(self, alerts: List[Dict]) -> int:
        """
        Save generated alerts to database.
        
        Args:
            alerts: List of alert dictionaries
            
        Returns:
            Number of alerts saved
        """
        if not alerts:
            return 0
        
        saved_count = 0
        for alert_data in alerts:
            try:
                alert = self.alert_repo.create_alert(**alert_data)
                saved_count += 1
            except Exception as e:
                self.logger.error(
                    f"Failed to save alert",
                    extra={"alert": alert_data, "error": str(e)}
                )
        
        self.alert_repo.commit()
        
        self.logger.info(
            f"Saved {saved_count} alerts to database",
            extra={"saved": saved_count, "total": len(alerts)}
        )
        
        return saved_count
    
    def close(self):
        """Close database session."""
        self.session.close()
