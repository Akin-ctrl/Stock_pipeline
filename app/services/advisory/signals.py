"""
Signal generation for investment decisions.

Generates buy/sell/hold signals based on technical indicators.
"""

from dataclasses import dataclass
from typing import Optional, Dict, List
from enum import Enum


class SignalType(Enum):
    """Signal types for investment decisions."""
    STRONG_BUY = "STRONG_BUY"
    BUY = "BUY"
    HOLD = "HOLD"
    SELL = "SELL"
    STRONG_SELL = "STRONG_SELL"


@dataclass
class TechnicalSignal:
    """
    Technical analysis signal.
    
    Attributes:
        signal_type: Type of signal (STRONG_BUY to STRONG_SELL)
        confidence: Confidence score (0.0 to 1.0)
        reasons: List of reasons for the signal
        indicators: Dict of indicator values used
    """
    signal_type: SignalType
    confidence: float
    reasons: List[str]
    indicators: Dict[str, float]


class SignalGenerator:
    """
    Generates investment signals from technical indicators.
    
    Analyzes multiple technical indicators to produce actionable
    buy/sell/hold signals with confidence scores.
    """
    
    def __init__(
        self,
        rsi_oversold: float = 30.0,
        rsi_overbought: float = 70.0,
        rsi_strong_oversold: float = 20.0,
        rsi_strong_overbought: float = 80.0
    ):
        """
        Initialize signal generator.
        
        Args:
            rsi_oversold: RSI threshold for oversold (buy signal)
            rsi_overbought: RSI threshold for overbought (sell signal)
            rsi_strong_oversold: RSI threshold for strong buy
            rsi_strong_overbought: RSI threshold for strong sell
        """
        self.rsi_oversold = rsi_oversold
        self.rsi_overbought = rsi_overbought
        self.rsi_strong_oversold = rsi_strong_oversold
        self.rsi_strong_overbought = rsi_strong_overbought
    
    def generate_signal(self, indicators: Dict[str, float]) -> TechnicalSignal:
        """
        Generate trading signal from indicators.
        
        Args:
            indicators: Dict of technical indicators
                - rsi_14: 14-day RSI
                - macd: MACD value
                - macd_signal: MACD signal line
                - sma_50: 50-day simple moving average
                - sma_200: 200-day simple moving average
                - current_price: Current stock price
                - volume_ratio: Current volume / average volume
                
        Returns:
            TechnicalSignal with recommendation
        """
        signals = []
        reasons = []
        
        # RSI Signal
        rsi_signal = self._analyze_rsi(indicators.get('rsi_14'))
        if rsi_signal:
            signals.append(rsi_signal)
            if rsi_signal['signal'] == SignalType.STRONG_BUY:
                reasons.append(f"RSI {indicators.get('rsi_14'):.1f} - Strongly oversold")
            elif rsi_signal['signal'] == SignalType.BUY:
                reasons.append(f"RSI {indicators.get('rsi_14'):.1f} - Oversold")
            elif rsi_signal['signal'] == SignalType.STRONG_SELL:
                reasons.append(f"RSI {indicators.get('rsi_14'):.1f} - Strongly overbought")
            elif rsi_signal['signal'] == SignalType.SELL:
                reasons.append(f"RSI {indicators.get('rsi_14'):.1f} - Overbought")
        
        # MACD Signal
        macd_signal = self._analyze_macd(
            indicators.get('macd'),
            indicators.get('macd_signal')
        )
        if macd_signal:
            signals.append(macd_signal)
            macd_val = indicators.get('macd', 0)
            if macd_signal['signal'] in [SignalType.BUY, SignalType.STRONG_BUY]:
                reasons.append(f"MACD bullish crossover ({macd_val:.2f})")
            elif macd_signal['signal'] in [SignalType.SELL, SignalType.STRONG_SELL]:
                reasons.append(f"MACD bearish crossover ({macd_val:.2f})")
        
        # Moving Average Signal
        ma_signal = self._analyze_moving_averages(
            indicators.get('current_price'),
            indicators.get('sma_50'),
            indicators.get('sma_200')
        )
        if ma_signal:
            signals.append(ma_signal)
            price = indicators.get('current_price', 0)
            sma50 = indicators.get('sma_50', 0)
            sma200 = indicators.get('sma_200', 0)
            
            if ma_signal['signal'] == SignalType.STRONG_BUY:
                reasons.append(f"Golden Cross - SMA50 ({sma50:.2f}) > SMA200 ({sma200:.2f})")
            elif ma_signal['signal'] == SignalType.BUY:
                reasons.append(f"Price ({price:.2f}) > SMA50 ({sma50:.2f})")
            elif ma_signal['signal'] == SignalType.STRONG_SELL:
                reasons.append(f"Death Cross - SMA50 ({sma50:.2f}) < SMA200 ({sma200:.2f})")
            elif ma_signal['signal'] == SignalType.SELL:
                reasons.append(f"Price ({price:.2f}) < SMA50 ({sma50:.2f})")
        
        # Volume Signal
        volume_signal = self._analyze_volume(indicators.get('volume_ratio'))
        if volume_signal:
            signals.append(volume_signal)
            vol_ratio = indicators.get('volume_ratio', 1.0)
            if volume_signal['signal'] in [SignalType.BUY, SignalType.STRONG_BUY]:
                reasons.append(f"High volume support ({vol_ratio:.1f}x average)")
        
        # Aggregate signals
        if not signals:
            return TechnicalSignal(
                signal_type=SignalType.HOLD,
                confidence=0.5,
                reasons=["Insufficient indicator data"],
                indicators=indicators
            )
        
        final_signal, confidence = self._aggregate_signals(signals)
        
        return TechnicalSignal(
            signal_type=final_signal,
            confidence=confidence,
            reasons=reasons if reasons else ["Mixed signals"],
            indicators=indicators
        )
    
    def _analyze_rsi(self, rsi: Optional[float]) -> Optional[Dict]:
        """Analyze RSI indicator."""
        if rsi is None:
            return None
        
        if rsi <= self.rsi_strong_oversold:
            return {'signal': SignalType.STRONG_BUY, 'weight': 1.5}
        elif rsi <= self.rsi_oversold:
            return {'signal': SignalType.BUY, 'weight': 1.0}
        elif rsi >= self.rsi_strong_overbought:
            return {'signal': SignalType.STRONG_SELL, 'weight': 1.5}
        elif rsi >= self.rsi_overbought:
            return {'signal': SignalType.SELL, 'weight': 1.0}
        else:
            return {'signal': SignalType.HOLD, 'weight': 0.5}
    
    def _analyze_macd(
        self,
        macd: Optional[float],
        macd_signal: Optional[float]
    ) -> Optional[Dict]:
        """Analyze MACD indicator."""
        if macd is None or macd_signal is None:
            return None
        
        diff = macd - macd_signal
        
        # Bullish crossover
        if diff > 0 and macd > 0:
            return {'signal': SignalType.STRONG_BUY, 'weight': 1.2}
        elif diff > 0:
            return {'signal': SignalType.BUY, 'weight': 0.8}
        # Bearish crossover
        elif diff < 0 and macd < 0:
            return {'signal': SignalType.STRONG_SELL, 'weight': 1.2}
        elif diff < 0:
            return {'signal': SignalType.SELL, 'weight': 0.8}
        else:
            return {'signal': SignalType.HOLD, 'weight': 0.3}
    
    def _analyze_moving_averages(
        self,
        price: Optional[float],
        sma_50: Optional[float],
        sma_200: Optional[float]
    ) -> Optional[Dict]:
        """Analyze moving average crossovers."""
        if price is None:
            return None
        
        signals = []
        
        # Golden Cross / Death Cross
        if sma_50 is not None and sma_200 is not None:
            if sma_50 > sma_200:
                signals.append({'signal': SignalType.STRONG_BUY, 'weight': 1.3})
            elif sma_50 < sma_200:
                signals.append({'signal': SignalType.STRONG_SELL, 'weight': 1.3})
        
        # Price vs SMA50
        if sma_50 is not None:
            price_diff_pct = ((price - sma_50) / sma_50) * 100
            if price_diff_pct > 5:
                signals.append({'signal': SignalType.BUY, 'weight': 0.7})
            elif price_diff_pct < -5:
                signals.append({'signal': SignalType.SELL, 'weight': 0.7})
        
        if not signals:
            return {'signal': SignalType.HOLD, 'weight': 0.5}
        
        # Return strongest signal
        return max(signals, key=lambda x: x['weight'])
    
    def _analyze_volume(self, volume_ratio: Optional[float]) -> Optional[Dict]:
        """Analyze volume trends."""
        if volume_ratio is None:
            return None
        
        # High volume can confirm trends
        if volume_ratio > 2.0:
            return {'signal': SignalType.BUY, 'weight': 0.6}
        elif volume_ratio > 1.5:
            return {'signal': SignalType.BUY, 'weight': 0.4}
        elif volume_ratio < 0.5:
            return {'signal': SignalType.HOLD, 'weight': 0.2}
        else:
            return None
    
    def _aggregate_signals(
        self,
        signals: List[Dict]
    ) -> tuple[SignalType, float]:
        """
        Aggregate multiple signals into final recommendation.
        
        Args:
            signals: List of signal dicts with 'signal' and 'weight'
            
        Returns:
            Tuple of (final_signal, confidence)
        """
        if not signals:
            return SignalType.HOLD, 0.5
        
        # Score each signal type
        signal_scores = {
            SignalType.STRONG_BUY: 0.0,
            SignalType.BUY: 0.0,
            SignalType.HOLD: 0.0,
            SignalType.SELL: 0.0,
            SignalType.STRONG_SELL: 0.0
        }
        
        total_weight = 0.0
        for sig in signals:
            signal_scores[sig['signal']] += sig['weight']
            total_weight += sig['weight']
        
        # Normalize scores
        if total_weight > 0:
            for signal_type in signal_scores:
                signal_scores[signal_type] /= total_weight
        
        # Find dominant signal
        max_signal = max(signal_scores.items(), key=lambda x: x[1])
        final_signal = max_signal[0]
        confidence = max_signal[1]
        
        # Adjust confidence based on agreement
        buy_score = signal_scores[SignalType.STRONG_BUY] + signal_scores[SignalType.BUY]
        sell_score = signal_scores[SignalType.STRONG_SELL] + signal_scores[SignalType.SELL]
        hold_score = signal_scores[SignalType.HOLD]
        
        # If signals are conflicting, reduce confidence
        if buy_score > 0.3 and sell_score > 0.3:
            confidence *= 0.6  # Conflicting signals
        elif hold_score > 0.5:
            final_signal = SignalType.HOLD
            confidence = hold_score
        
        # Cap confidence
        confidence = min(confidence, 0.95)
        confidence = max(confidence, 0.1)
        
        return final_signal, confidence
    
    def get_signal_summary(self, signal: TechnicalSignal) -> str:
        """
        Get human-readable signal summary.
        
        Args:
            signal: TechnicalSignal object
            
        Returns:
            Summary string
        """
        emoji_map = {
            SignalType.STRONG_BUY: "🚀",
            SignalType.BUY: "📈",
            SignalType.HOLD: "⏸️",
            SignalType.SELL: "📉",
            SignalType.STRONG_SELL: "⚠️"
        }
        
        emoji = emoji_map.get(signal.signal_type, "")
        confidence_pct = signal.confidence * 100
        
        summary = f"{emoji} {signal.signal_type.value} (Confidence: {confidence_pct:.0f}%)\n"
        summary += "Reasons:\n"
        for reason in signal.reasons:
            summary += f"  • {reason}\n"
        
        return summary
