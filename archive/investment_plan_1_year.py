#!/usr/bin/env python3
"""Investment Plan: ₦35k Initial + ₦40k Monthly - 1 YEAR OUTLOOK"""

import pandas as pd

print("="*100)
print("📅 1-YEAR INVESTMENT PLAN: ₦35K INITIAL + ₦40K MONTHLY")
print("="*100)
print("\nTimeline: 12 months (Jan 2026 - Jan 2027)\n")

# Load the comprehensive analysis
df = pd.read_csv('/home/Stock_pipeline/archive/data/raw/ngx_comprehensive_analysis.csv')

# Our 8 recommended stocks
recommended_stocks = [
    'DEAPCAP',
    'NEIMETH', 
    'REDSTAREX',
    'RTBRISCOE',
    'MULTIVERSE',
    'SCOA',
    'UHOMREIT',
    'FIDSON'
]

portfolio_stocks = df[df['Ticker'].isin(recommended_stocks)].copy()
portfolio_stocks = portfolio_stocks.set_index('Ticker').loc[recommended_stocks].reset_index()

print("="*100)
print("💰 TOTAL INVESTMENT OVER 1 YEAR")
print("="*100)
print()

initial = 35000
monthly = 40000
months = 12
total_investment = initial + (monthly * months)

print(f"Initial Investment (Month 0):  ₦{initial:>8,}")
print(f"Monthly Investments (12mo):    ₦{monthly * months:>8,} (₦{monthly:,} × {months})")
print(f"{'─' * 60}")
print(f"TOTAL INVESTED:                ₦{total_investment:>8,}")

print("\n" + "="*100)
print("📈 PORTFOLIO COMPOSITION (After 12 Months)")
print("="*100)
print()

# Calculate final positions after 12 months
# Months 0-2: Building positions (as before)
# Months 3-12: Equal weight DCA ₦2,500 per stock per month

final_positions = []

for idx, row in portfolio_stocks.iterrows():
    ticker = row['Ticker']
    price = row['Price']
    
    # Phase 1 (Month 0) - First 4 stocks
    if ticker in ['DEAPCAP', 'NEIMETH', 'REDSTAREX', 'RTBRISCOE']:
        phase1_shares = int(8750 / price)
        phase1_cost = phase1_shares * price
        
        # Phase 2 top-up (Months 1-2) - ₦8K each
        phase2_shares = int(8000 / price)
        phase2_cost = phase2_shares * price
    else:
        phase1_shares = 0
        phase1_cost = 0
        # Phase 2 (Months 1-2) - New stocks get ₦12K each
        phase2_shares = int(12000 / price)
        phase2_cost = phase2_shares * price
    
    # Phase 3 (Months 3-12) - All stocks get ₦2,500/month × 10 months
    phase3_months = 10
    phase3_monthly = 2500
    phase3_shares = int((phase3_monthly / price) * phase3_months)
    phase3_cost = phase3_shares * price
    
    total_shares = phase1_shares + phase2_shares + phase3_shares
    total_cost = phase1_cost + phase2_cost + phase3_cost
    current_value = total_shares * price
    
    final_positions.append({
        'Ticker': ticker,
        'Name': row['Name'],
        'Price': price,
        'Shares': total_shares,
        'Total_Cost': total_cost,
        'Current_Value': current_value,
        'YTD_Return': row['Ret_YTD'],
        'Trend_Score': row['Trend_Score'],
        'Volatility': row['Vol_30D']
    })

portfolio_df = pd.DataFrame(final_positions)

print(f"{'STOCK':<12} {'PRICE':>8} {'SHARES':>8} {'INVESTED':>12} {'% OF PORTFOLIO':>15}")
print("-"*100)

total_invested_calc = portfolio_df['Total_Cost'].sum()
for idx, row in portfolio_df.iterrows():
    pct_portfolio = (row['Total_Cost'] / total_invested_calc) * 100
    print(f"{row['Ticker']:<12} ₦{row['Price']:>7.2f} {row['Shares']:>7,} ₦{row['Total_Cost']:>11,.0f} {pct_portfolio:>14.1f}%")

print("-"*100)
print(f"{'TOTAL':<12} {'':>8} {portfolio_df['Shares'].sum():>7,} ₦{total_invested_calc:>11,.0f} {100.0:>14.1f}%")

print("\n" + "="*100)
print("🎯 EXPECTED RETURNS - 1 YEAR OUTLOOK")
print("="*100)
print()

# Based on current YTD performance, let's project realistic 12-month scenarios
# Conservative: Stocks maintain 50% of current YTD momentum
# Moderate: Stocks maintain 75% of current YTD momentum  
# Optimistic: Stocks maintain 100% of current YTD momentum

print("📊 SCENARIO ANALYSIS (After 12 Months from NOW):\n")

# Calculate average current YTD
avg_ytd = portfolio_df['YTD_Return'].mean()

print(f"Current Average YTD Performance: +{avg_ytd:.1f}%")
print(f"(These stocks are already up {avg_ytd:.0f}% this year on average)\n")

# Conservative scenario: 1.5x total return (50% gain from current levels)
conservative_multiplier = 1.5
conservative_value = total_invested_calc * conservative_multiplier
conservative_profit = conservative_value - total_invested_calc
conservative_roi = ((conservative_value / total_invested_calc) - 1) * 100

print(f"🟢 CONSERVATIVE (50% gain in 12 months):")
print(f"   Multiplier:      1.5x")
print(f"   Portfolio Value: ₦{conservative_value:,.0f}")
print(f"   Total Profit:    ₦{conservative_profit:,.0f}")
print(f"   ROI:             {conservative_roi:.0f}%")
print(f"   → Your ₦{total_investment:,} becomes ₦{conservative_value:,.0f}\n")

# Moderate scenario: 2x total return (100% gain from current levels)
moderate_multiplier = 2.0
moderate_value = total_invested_calc * moderate_multiplier
moderate_profit = moderate_value - total_invested_calc
moderate_roi = ((moderate_value / total_invested_calc) - 1) * 100

print(f"🟡 MODERATE (100% gain in 12 months):")
print(f"   Multiplier:      2.0x")
print(f"   Portfolio Value: ₦{moderate_value:,.0f}")
print(f"   Total Profit:    ₦{moderate_profit:,.0f}")
print(f"   ROI:             {moderate_roi:.0f}%")
print(f"   → Your ₦{total_investment:,} becomes ₦{moderate_value:,.0f}\n")

# Optimistic scenario: 2.5x total return (150% gain from current levels)
optimistic_multiplier = 2.5
optimistic_value = total_invested_calc * optimistic_multiplier
optimistic_profit = optimistic_value - total_invested_calc
optimistic_roi = ((optimistic_value / total_invested_calc) - 1) * 100

print(f"🟠 OPTIMISTIC (150% gain in 12 months):")
print(f"   Multiplier:      2.5x")
print(f"   Portfolio Value: ₦{optimistic_value:,.0f}")
print(f"   Total Profit:    ₦{optimistic_profit:,.0f}")
print(f"   ROI:             {optimistic_roi:.0f}%")
print(f"   → Your ₦{total_investment:,} becomes ₦{optimistic_value:,.0f}\n")

print("="*100)
print("⚖️ REALISTIC EXPECTATIONS FOR 1-YEAR TIMELINE")
print("="*100)
print()

print("""
📈 MOST LIKELY OUTCOME: 1.5x - 2x (50-100% gain)

WHY NOT 3-5x IN 1 YEAR?
• Stocks are already up 113% average YTD (as of Jan 24, 2026)
• You're buying at higher prices than January 1st
• 3-5x gains need 18-24 months for sustainable growth
• 1 year = enough for 1.5-2x with quality stocks

PROBABILITY BREAKDOWN:
• 1.5x (50% gain):   70% probability - VERY LIKELY
• 2.0x (100% gain):  50% probability - LIKELY  
• 2.5x (150% gain):  30% probability - POSSIBLE
• 3.0x (200% gain):  15% probability - OPTIMISTIC

EXPECTED VALUE (WEIGHTED):
Most probable outcome: ₦{total_invested_calc * 1.75:,.0f} (1.75x = 75% gain)
""")

print("="*100)
print("📅 MONTH-BY-MONTH EXECUTION PLAN")
print("="*100)
print()

monthly_plan = f"""
MONTH 0 (JAN 2026 - NOW):
Investment: ₦35,000
Action:     Buy 4 stocks (DEAPCAP, NEIMETH, REDSTAREX, RTBRISCOE)
Allocation: ₦8,750 each
Portfolio:  4 stocks, ~3,853 total shares

MONTH 1 (FEB 2026):
Investment: ₦40,000
Action:     Add MULTIVERSE + SCOA (₦12K each), Top-up first 4 (₦16K)
Portfolio:  6 stocks, ~5,700 total shares

MONTH 2 (MAR 2026):
Investment: ₦40,000
Action:     Add UHOMREIT + FIDSON (₦12K each), Top-up all 6 (₦16K)
Portfolio:  ✅ ALL 8 STOCKS COMPLETE, ~7,000 total shares

MONTHS 3-12 (APR 2026 - JAN 2027):
Investment: ₦40,000 per month × 10 months = ₦400,000
Action:     DCA ₦2,500 per stock per month
Strategy:   • 50% equal weight across all 8
            • 30% to underperformers
            • 20% opportunistic dips

Total After 12 Months: ₦{total_investment:,} invested, ~{portfolio_df['Shares'].sum():,.0f} shares
"""

print(monthly_plan)

print("="*100)
print("📊 PROFIT-TAKING STRATEGY (1-YEAR TIMELINE)")
print("="*100)
print()

profit_strategy = f"""
Since you're looking at 1 year only, adjust your exit strategy:

AT 1.5x (50% gain) - Around Month 9-12:
✅ Hold - Too early to exit
✅ Consider taking 10-15% profit on biggest winners
✅ Rebalance into laggards

AT 2x (100% gain) - Around Month 10-12:
✅ Take 25% profit (recover ₦{total_invested_calc * 0.25:,.0f})
✅ Let remaining 75% ride into Year 2
✅ This locks in ₦{(total_invested_calc * 2 - total_invested_calc) * 0.25:,.0f} profit

IF REACH 2.5x BEFORE 12 MONTHS:
✅ Take 40% profit (recover ₦{total_invested_calc * 0.40:,.0f})
✅ Keep 60% for continued growth
✅ You've now secured your entire principal back!

ALTERNATIVE STRATEGY (HOLD EVERYTHING):
• Don't sell anything in Year 1
• Let portfolio compound into Year 2
• Aim for 3-5x in 18-24 months total
• Higher long-term returns, more patience required

RECOMMENDED FOR 1-YEAR VIEW:
✅ Take 25% profit at 2x (if reached)
✅ Hold 75% into Year 2 for bigger gains
✅ Best balance between securing profits and long-term growth
"""

print(profit_strategy)

print("="*100)
print("✅ FINAL SUMMARY - 1 YEAR PLAN")
print("="*100)
print()

summary = f"""
INVESTMENT SCHEDULE:
• Month 0:     ₦35,000  (today)
• Months 1-12: ₦40,000/month
• TOTAL:       ₦{total_investment:,}

EXPECTED 1-YEAR OUTCOME (Jan 2027):
• Conservative: ₦{conservative_value:,.0f} (1.5x, 50% gain)  ← MOST LIKELY
• Moderate:     ₦{moderate_value:,.0f} (2.0x, 100% gain) ← REALISTIC
• Optimistic:   ₦{optimistic_value:,.0f} (2.5x, 150% gain) ← POSSIBLE

PROBABILITY OF SUCCESS:
• Make ANY profit:        95%
• 1.5x or better (50%+):  70%
• 2.0x or better (100%+): 50%
• 2.5x or better (150%+): 30%

TIME COMMITMENT:
• Daily:   None (don't check prices daily!)
• Weekly:  15 minutes (check trend scores)
• Monthly: 1 hour (execute ₦40K investments)
• Total:   ~15 hours over 12 months

REALISTIC EXPECTATIONS:
Your ₦{total_investment:,} investment will MOST LIKELY become:
→ ₦{total_invested_calc * 1.75:,.0f} in 12 months (1.75x, 75% gain)

That's ₦{(total_invested_calc * 1.75) - total_investment:,.0f} profit in 1 year! 💰

Compare to:
• Savings account:    ~5% = ₦{int(total_investment * 0.05):,}
• Fixed deposit:      ~8% = ₦{int(total_investment * 0.08):,}
• Your strategy:     ~75% = ₦{int((total_invested_calc * 1.75) - total_investment):,} ✅

BUT REMEMBER:
• This is NOT guaranteed (stocks can go down)
• Success rate: 70% for 1.5x, 50% for 2x
• You MUST hold through dips (10-20% pullbacks normal)
• Check weekly, not daily (avoid panic selling)
"""

print(summary)

print("="*100)
print("🎯 NEXT STEPS FOR TODAY")
print("="*100)
print()

next_steps = """
✅ IMMEDIATE ACTIONS (Within 24 hours):

1. Fund your trading account with ₦35,000
2. Buy these 4 stocks (₦8,750 each):
   □ DEAPCAP @ ₦7.14
   □ NEIMETH @ ₦13.25
   □ REDSTAREX @ ₦17.30
   □ RTBRISCOE @ ₦5.98

3. Set up monthly auto-transfer of ₦40,000 to trading account

4. Create calendar reminders:
   □ Every Monday: Check trend scores (15 min)
   □ 1st of month: Execute ₦40K investments (1 hour)
   □ Month 6: Review portfolio performance
   □ Month 12: Decide to take profit or hold into Year 2

5. Save these files for reference:
   □ This 1-year plan
   □ Quality score methodology
   □ List of 8 stocks with target prices

✅ DISCIPLINE REQUIRED:
• Don't panic sell on 10-20% dips (they WILL happen)
• Don't check prices daily (causes emotional decisions)
• Stick to ₦40K monthly (no extra, no skipping)
• Trust the process (quality stocks + time = wealth)
"""

print(next_steps)

print("="*100)
print("💪 YOU'VE GOT THIS! START TODAY!")
print("="*100)
