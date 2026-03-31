#!/usr/bin/env python3
"""Find 10x Potential Stocks - Low Price, High Growth Candidates"""

import pandas as pd
import numpy as np

print("="*100)
print("🚀 10X POTENTIAL STOCKS - PENNY STOCK MOONSHOTS")
print("="*100)
print("\nCriteria: Low price (<₦20) + Strong trend + High momentum\n")

# Load the comprehensive analysis
df = pd.read_csv('/home/Stock_pipeline/archive/data/raw/ngx_comprehensive_analysis.csv')

# Filter for 10x potential candidates
# Criteria:
# 1. Price < ₦20 (easier to 10x from low base)
# 2. Trend Score > 60 (in uptrend)
# 3. Recent strong performance (1M return > 20%)
# 4. High volatility (can make big moves)

print("="*100)
print("🎯 TIER 1: SUPER CHEAP ROCKETS (Under ₦10)")
print("="*100)
print("These can go from ₦5 to ₦50 = 10x return\n")

tier1 = df[
    (df['Price'] < 10) & 
    (df['Trend_Score'] >= 80) & 
    (df['Ret_1M'] > 20)
].sort_values('Ret_1M', ascending=False)

if len(tier1) > 0:
    tier1_display = tier1[['Ticker', 'Name', 'Price', 'Ret_1M', 'Ret_YTD', 'Trend_Score', 'Vol_30D']].head(15)
    tier1_display.index = range(1, len(tier1_display) + 1)
    print(tier1_display.to_string())
    
    print(f"\n💡 ANALYSIS:")
    print(f"   Found {len(tier1)} stocks under ₦10 with strong momentum")
    print(f"   Average 1-month gain: {tier1['Ret_1M'].mean():.1f}%")
    print(f"   Best performer: {tier1.iloc[0]['Ticker']} (+{tier1.iloc[0]['Ret_1M']:.1f}%)")
else:
    print("No stocks found in this category")

print("\n" + "="*100)
print("💎 TIER 2: AFFORDABLE GEMS (₦10-20)")
print("="*100)
print("These can go from ₦15 to ₦150 = 10x return\n")

tier2 = df[
    (df['Price'] >= 10) & 
    (df['Price'] < 20) & 
    (df['Trend_Score'] >= 80) & 
    (df['Ret_1M'] > 15)
].sort_values('Ret_1M', ascending=False)

if len(tier2) > 0:
    tier2_display = tier2[['Ticker', 'Name', 'Price', 'Ret_1M', 'Ret_YTD', 'Trend_Score', 'Vol_30D']].head(15)
    tier2_display.index = range(1, len(tier2_display) + 1)
    print(tier2_display.to_string())
    
    print(f"\n💡 ANALYSIS:")
    print(f"   Found {len(tier2)} stocks ₦10-20 with strong momentum")
    print(f"   Average 1-month gain: {tier2['Ret_1M'].mean():.1f}%")
    print(f"   Best performer: {tier2.iloc[0]['Ticker']} (+{tier2.iloc[0]['Ret_1M']:.1f}%)")
else:
    print("No stocks found in this category")

print("\n" + "="*100)
print("🔥 TIER 3: EXPLOSIVE MOVERS (Already Showing 10x Potential)")
print("="*100)
print("Stocks that ALREADY went 5x+ this year - showing they CAN do it\n")

tier3 = df[
    (df['Price'] < 30) & 
    (df['Ret_1Y'] > 400) &  # Already went 5x+ in past year
    (df['Trend_Score'] >= 60)
].sort_values('Ret_1Y', ascending=False)

if len(tier3) > 0:
    tier3_display = tier3[['Ticker', 'Name', 'Price', 'Ret_1Y', 'Ret_YTD', 'Ret_1M', 'Trend_Score']].head(15)
    tier3_display.index = range(1, len(tier3_display) + 1)
    print(tier3_display.to_string())
    
    print(f"\n💡 ANALYSIS:")
    print(f"   Found {len(tier3)} stocks that already proved 5x+ capability")
    print(f"   Average 1-year gain: {tier3['Ret_1Y'].mean():.1f}%")
    print(f"   These have shown they can make MASSIVE moves!")
else:
    print("No stocks found in this category")

print("\n" + "="*100)
print("⚡ TIER 4: TURNAROUND PLAYS (Deep Value)")
print("="*100)
print("Beaten down stocks starting to reverse - highest risk/reward\n")

tier4 = df[
    (df['Price'] < 15) & 
    (df['Dist_Low'] < 30) &  # Within 30% of 52-week low
    (df['Ret_1M'] > 30) &  # Strong recent reversal
    (df['Trend_Score'] >= 60)
].sort_values('Ret_1M', ascending=False)

if len(tier4) > 0:
    tier4_display = tier4[['Ticker', 'Name', 'Price', 'Dist_Low', 'Ret_1M', 'Trend_Score', 'Vol_30D']].head(15)
    tier4_display.index = range(1, len(tier4_display) + 1)
    print(tier4_display.to_string())
    
    print(f"\n💡 ANALYSIS:")
    print(f"   Found {len(tier4)} beaten-down stocks reversing higher")
    print(f"   Average 1-month gain: {tier4['Ret_1M'].mean():.1f}%")
    print(f"   These are bouncing from lows - catch the reversal!")
else:
    print("No stocks found in this category")

# Combine all tiers for final ranking
print("\n" + "="*100)
print("🏆 TOP 20 OVERALL 10X CANDIDATES")
print("="*100)
print("Ranked by 10x potential score (combines price, momentum, trend)\n")

# Calculate 10x potential score
df['10X_Score'] = (
    (20 / df['Price']) * 30 +  # Lower price = higher score
    (df['Ret_1M'] / 10) * 25 +  # Strong 1-month momentum
    (df['Trend_Score']) * 25 +  # Strong trend
    (df['Vol_30D'] / 10) * 10 +  # Volatility (enables big moves)
    (df['Ret_YTD'] / 10) * 10  # YTD performance
)

# Filter reasonable candidates
candidates = df[
    (df['Price'] < 30) & 
    (df['Trend_Score'] >= 60) &
    (df['Ret_1M'] > 0)
]

top20 = candidates.nlargest(20, '10X_Score')[
    ['Ticker', 'Name', 'Price', 'Ret_1M', 'Ret_YTD', 'Trend_Score', 'Vol_30D', '10X_Score']
]
top20.index = range(1, len(top20) + 1)
print(top20.to_string())

print("\n" + "="*100)
print("💡 DETAILED ANALYSIS - TOP 5 10X CANDIDATES")
print("="*100)

for idx, row in top20.head(5).iterrows():
    print(f"\n{idx}. {row['Ticker']} - {row['Name']}")
    print(f"   Current Price: ₦{row['Price']:.2f}")
    print(f"   10x Target: ₦{row['Price'] * 10:.2f}")
    print(f"   1-Month Return: +{row['Ret_1M']:.1f}%")
    print(f"   YTD Return: +{row['Ret_YTD']:.1f}%")
    print(f"   Trend Score: {row['Trend_Score']:.0f}/100")
    print(f"   Volatility: {row['Vol_30D']:.1f}%")
    
    # Calculate potential
    current = row['Price']
    target_10x = current * 10
    
    # Risk assessment
    if row['Vol_30D'] > 100:
        risk = "EXTREME RISK ⚠️⚠️⚠️"
    elif row['Vol_30D'] > 80:
        risk = "VERY HIGH RISK ⚠️⚠️"
    elif row['Vol_30D'] > 60:
        risk = "HIGH RISK ⚠️"
    else:
        risk = "MODERATE RISK"
    
    print(f"   Risk Level: {risk}")
    
    # Timeframe estimate
    if row['Ret_1M'] > 100:
        timeframe = "3-6 months (parabolic momentum)"
    elif row['Ret_1M'] > 50:
        timeframe = "6-12 months (strong momentum)"
    else:
        timeframe = "12-24 months (steady growth)"
    
    print(f"   Estimated Timeframe: {timeframe}")

print("\n" + "="*100)
print("⚠️ CRITICAL WARNINGS & STRATEGY")
print("="*100)

print(f"""
🎯 10X INVESTING STRATEGY:

1. POSITION SIZING (CRITICAL!)
   - Never put more than 2-5% per stock
   - Diversify across 5-10 different 10x candidates
   - Only use money you can afford to LOSE

2. ENTRY STRATEGY
   - Wait for pullbacks (don't chase parabolic moves)
   - Buy in 3 tranches (33% each)
   - Set alerts, don't FOMO buy

3. EXIT STRATEGY
   - Sell 50% at 3x gain (take initial investment off table)
   - Sell 25% at 5x gain
   - Let remaining 25% ride to 10x
   - ALWAYS use stop losses (20-30% below entry)

4. RISK MANAGEMENT
   - These are SPECULATIVE plays
   - 80% will fail or underperform
   - 1-2 winners can make up for all losses
   - This is NOT long-term investing!

5. TIMING
   - Don't buy extended stocks (>10% above 7-day MA)
   - Best entries: After 20-30% pullback from recent high
   - Watch for volume confirmation

⚠️ REALITY CHECK:
   - Most penny stocks go to ZERO
   - For every 10x winner, there are 10 losers
   - Only 10-20% of these will actually 10x
   - You need a PORTFOLIO approach, not single bets

✅ BEST APPROACH:
   - Pick 5-10 from the top 20 list
   - Invest equal amounts in each (2-5% portfolio each)
   - If 1-2 hit 10x, you win big overall
   - Cut losses quickly on failures
""")

print("\n" + "="*100)
print("📋 RECOMMENDED PORTFOLIO - 10X HUNTING")
print("="*100)

print(f"\nFor ₦100,000 investment budget:\n")

portfolio_stocks = top20.head(8)
investment_per_stock = 100000 / len(portfolio_stocks)

print(f"Diversify across {len(portfolio_stocks)} stocks:")
print(f"₦{investment_per_stock:,.0f} per stock\n")

for idx, row in portfolio_stocks.iterrows():
    shares = investment_per_stock / row['Price']
    target_value = shares * (row['Price'] * 10)
    print(f"{idx}. {row['Ticker']:12} - Buy ₦{investment_per_stock:,.0f} ({shares:,.0f} shares @ ₦{row['Price']:.2f})")
    print(f"    10x Target Value: ₦{target_value:,.0f}\n")

total_10x = (investment_per_stock * 10) * len(portfolio_stocks)
print(f"If ALL hit 10x: ₦{total_10x:,.0f} (unlikely!)")
print(f"If 2 hit 10x, rest fail: ₦{(investment_per_stock * 10 * 2):,.0f} (realistic best case)")
print(f"If 1 hits 10x, rest fail: ₦{(investment_per_stock * 10):,.0f} (realistic scenario)")

print("\n" + "="*100)
print("✅ ANALYSIS COMPLETE")
print("="*100)
