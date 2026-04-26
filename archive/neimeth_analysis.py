#!/usr/bin/env python3
"""NEIMETH Historical Analysis"""

import afrimarket as afm
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

print("="*80)
print("📊 NEIMETH PHARMACEUTICALS - COMPREHENSIVE HISTORICAL ANALYSIS")
print("="*80)

# Fetch data
stock = afm.Stock(ticker='neimeth', market='ngx')
df = stock.get_price()
df['Date'] = pd.to_datetime(df['Date'])
df = df.sort_values('Date')
df['Returns'] = df['Price'].pct_change()

print(f"\n📅 DATA COVERAGE:")
print(f"Total trading days: {len(df)}")
print(f"Date range: {df['Date'].min().strftime('%Y-%m-%d')} to {df['Date'].max().strftime('%Y-%m-%d')}")
print(f"Years of data: {(df['Date'].max() - df['Date'].min()).days / 365:.1f} years")

# Current metrics
current_price = df.iloc[-1]['Price']
print(f"\n💰 CURRENT STATUS:")
print(f"Current Price: ₦{current_price:.2f}")
print(f"52-week High: ₦{df.tail(252)['Price'].max():.2f}")
print(f"52-week Low: ₦{df.tail(252)['Price'].min():.2f}")
print(f"All-time High: ₦{df['Price'].max():.2f} ({df[df['Price'] == df['Price'].max()]['Date'].values[0]})")
print(f"All-time Low: ₦{df['Price'].min():.2f} ({df[df['Price'] == df['Price'].min()]['Date'].values[0]})")

# Moving averages
df['MA_7'] = df['Price'].rolling(7).mean()
df['MA_30'] = df['Price'].rolling(30).mean()
df['MA_90'] = df['Price'].rolling(90).mean()
df['MA_200'] = df['Price'].rolling(200).mean()

print(f"\n📈 MOVING AVERAGES:")
print(f"7-day MA:   ₦{df.iloc[-1]['MA_7']:.2f}")
print(f"30-day MA:  ₦{df.iloc[-1]['MA_30']:.2f}")
print(f"90-day MA:  ₦{df.iloc[-1]['MA_90']:.2f}")
print(f"200-day MA: ₦{df.iloc[-1]['MA_200']:.2f}")

# Trend analysis
print(f"\n🎯 TREND SIGNALS:")
if current_price > df.iloc[-1]['MA_7']:
    print("✅ Price > 7-day MA (Short-term bullish)")
else:
    print("❌ Price < 7-day MA (Short-term bearish)")
    
if df.iloc[-1]['MA_7'] > df.iloc[-1]['MA_30']:
    print("✅ 7-day MA > 30-day MA (Momentum positive)")
else:
    print("❌ 7-day MA < 30-day MA (Momentum negative)")
    
if current_price > df.iloc[-1]['MA_200']:
    print("✅ Price > 200-day MA (Long-term uptrend)")
else:
    print("❌ Price < 200-day MA (Long-term downtrend)")

# Performance metrics
periods = {
    '1 Week': 5,
    '1 Month': 20,
    '3 Months': 60,
    '6 Months': 126,
    '1 Year': 252,
    'YTD': len(df[df['Date'].dt.year == 2026])
}

print(f"\n💹 RETURNS ANALYSIS:")
for period_name, days in periods.items():
    if len(df) >= days:
        old_price = df.iloc[-days]['Price']
        returns = ((current_price / old_price) - 1) * 100
        print(f"{period_name:12}: {returns:+7.2f}% (₦{old_price:.2f} → ₦{current_price:.2f})")

# Volatility analysis
print(f"\n📊 VOLATILITY & RISK METRICS:")
vol_30d = df.tail(30)['Returns'].std() * np.sqrt(252) * 100
vol_90d = df.tail(90)['Returns'].std() * np.sqrt(252) * 100
vol_1yr = df.tail(252)['Returns'].std() * np.sqrt(252) * 100

print(f"30-day Annualized Volatility: {vol_30d:.2f}%")
print(f"90-day Annualized Volatility: {vol_90d:.2f}%")
print(f"1-year Annualized Volatility: {vol_1yr:.2f}%")

# Max drawdown
rolling_max = df['Price'].expanding().max()
drawdown = (df['Price'] - rolling_max) / rolling_max
max_drawdown = drawdown.min() * 100
print(f"Maximum Drawdown: {max_drawdown:.2f}%")

# Best and worst days
print(f"\n🎢 BEST & WORST DAYS:")
best_days = df.nlargest(5, 'Returns')[['Date', 'Price', 'Returns']]
worst_days = df.nsmallest(5, 'Returns')[['Date', 'Price', 'Returns']]

print(f"\n🚀 Top 5 Gaining Days:")
for idx, row in best_days.iterrows():
    print(f"  {row['Date'].strftime('%Y-%m-%d')}: +{row['Returns']*100:.2f}% (₦{row['Price']:.2f})")

print(f"\n💔 Top 5 Losing Days:")
for idx, row in worst_days.iterrows():
    print(f"  {row['Date'].strftime('%Y-%m-%d')}: {row['Returns']*100:.2f}% (₦{row['Price']:.2f})")

# Recent price action (last 30 days)
print(f"\n📅 LAST 30 TRADING DAYS:")
recent = df.tail(30)[['Date', 'Price', 'Returns']]
print(recent.to_string(index=False))

# Support and resistance levels
print(f"\n🎯 KEY PRICE LEVELS (Last 90 days):")
recent_90 = df.tail(90)
resistance_1 = recent_90['Price'].quantile(0.95)
resistance_2 = recent_90['Price'].quantile(0.85)
support_1 = recent_90['Price'].quantile(0.15)
support_2 = recent_90['Price'].quantile(0.05)

print(f"Strong Resistance: ₦{resistance_1:.2f}")
print(f"Weak Resistance:   ₦{resistance_2:.2f}")
print(f"Current Price:     ₦{current_price:.2f}")
print(f"Weak Support:      ₦{support_1:.2f}")
print(f"Strong Support:    ₦{support_2:.2f}")

# Statistical summary
print(f"\n📈 STATISTICAL SUMMARY (All Time):")
print(f"Mean Price:       ₦{df['Price'].mean():.2f}")
print(f"Median Price:     ₦{df['Price'].median():.2f}")
print(f"Std Deviation:    ₦{df['Price'].std():.2f}")
print(f"Coefficient of Variation: {(df['Price'].std()/df['Price'].mean())*100:.2f}%")

# Risk-adjusted returns (Sharpe ratio approximation)
avg_return = df['Returns'].mean() * 252 * 100
risk_free_rate = 15  # Approximate Nigerian T-bill rate
sharpe_ratio = (avg_return - risk_free_rate) / vol_1yr
print(f"\n📊 RISK-ADJUSTED PERFORMANCE:")
print(f"Average Annual Return: {avg_return:.2f}%")
print(f"Risk-Free Rate (est): {risk_free_rate}%")
print(f"Sharpe Ratio: {sharpe_ratio:.2f}")

# Investment verdict
print(f"\n" + "="*80)
print(f"🎯 INVESTMENT VERDICT")
print(f"="*80)

score = 0
signals = []

# Scoring system
if current_price > df.iloc[-1]['MA_200']:
    score += 2
    signals.append("✅ Long-term uptrend (above 200-day MA)")
else:
    signals.append("❌ Long-term downtrend (below 200-day MA)")

if df.iloc[-1]['MA_7'] > df.iloc[-1]['MA_30']:
    score += 1
    signals.append("✅ Positive momentum (7-day > 30-day MA)")
else:
    signals.append("❌ Negative momentum")

if vol_1yr < 50:
    score += 1
    signals.append("✅ Moderate volatility")
elif vol_1yr > 100:
    score -= 1
    signals.append("❌ Very high volatility (HIGH RISK)")

if max_drawdown > -30:
    score += 1
    signals.append("✅ Manageable drawdown")
else:
    signals.append("❌ Large historical drawdowns")

# Recent returns check
if len(df) >= 60:
    three_month_return = ((current_price / df.iloc[-60]['Price']) - 1) * 100
    if three_month_return > 20:
        score += 1
        signals.append(f"✅ Strong 3-month performance (+{three_month_return:.1f}%)")
    elif three_month_return < -10:
        score -= 1
        signals.append(f"❌ Weak 3-month performance ({three_month_return:.1f}%)")

for signal in signals:
    print(signal)

print(f"\nInvestment Score: {score}/6")

if score >= 5:
    rating = "STRONG BUY ⭐⭐⭐⭐⭐"
    advice = "Excellent momentum and trend. Suitable for growth investors."
elif score >= 3:
    rating = "BUY ⭐⭐⭐"
    advice = "Positive trend but manage risk. Good for aggressive portfolios."
elif score >= 1:
    rating = "HOLD ⭐⭐"
    advice = "Mixed signals. Wait for clearer trend or better entry point."
else:
    rating = "AVOID ⭐"
    advice = "Weak signals. Too risky for most investors."

print(f"\n🏆 RATING: {rating}")
print(f"💡 ADVICE: {advice}")

print(f"\n" + "="*80)
