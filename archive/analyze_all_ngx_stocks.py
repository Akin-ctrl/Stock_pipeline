#!/usr/bin/env python3
"""Comprehensive Analysis of ALL Nigerian Stocks on Afrimarket"""

import afrimarket as afm
import pandas as pd
import numpy as np
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

print("="*100)
print("📊 COMPREHENSIVE NGX STOCK ANALYSIS - ALL 148 STOCKS")
print("="*100)
print(f"Analysis Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("\n⏳ Fetching market data... This will take a few minutes.\n")

# Initialize exchange
ngx = afm.Exchange(market='ngx')
all_stocks = ngx.get_listed_companies()

print(f"✅ Found {len(all_stocks)} listed stocks")
print(f"\n🔄 Analyzing individual stocks (this may take 5-10 minutes)...\n")

# Store results
results = []
failed_stocks = []

for idx, row in all_stocks.iterrows():
    ticker = row['Ticker']
    
    try:
        # Show progress
        if (idx + 1) % 10 == 0:
            print(f"Progress: {idx + 1}/{len(all_stocks)} stocks analyzed...")
        
        # Get stock data
        stock = afm.Stock(ticker=ticker.lower(), market='ngx')
        prices = stock.get_price()
        
        if len(prices) < 30:  # Skip stocks with insufficient data
            continue
            
        prices['Date'] = pd.to_datetime(prices['Date'])
        prices = prices.sort_values('Date')
        prices['Returns'] = prices['Price'].pct_change()
        
        # Current metrics
        current_price = prices.iloc[-1]['Price']
        
        # Moving averages
        ma_7 = prices['Price'].rolling(7).mean().iloc[-1] if len(prices) >= 7 else current_price
        ma_30 = prices['Price'].rolling(30).mean().iloc[-1] if len(prices) >= 30 else current_price
        ma_90 = prices['Price'].rolling(90).mean().iloc[-1] if len(prices) >= 90 else current_price
        ma_200 = prices['Price'].rolling(200).mean().iloc[-1] if len(prices) >= 200 else current_price
        
        # Performance metrics
        ret_1w = ((current_price / prices.iloc[-5]['Price']) - 1) * 100 if len(prices) >= 5 else 0
        ret_1m = ((current_price / prices.iloc[-20]['Price']) - 1) * 100 if len(prices) >= 20 else 0
        ret_3m = ((current_price / prices.iloc[-60]['Price']) - 1) * 100 if len(prices) >= 60 else 0
        ret_6m = ((current_price / prices.iloc[-126]['Price']) - 1) * 100 if len(prices) >= 126 else 0
        ret_1y = ((current_price / prices.iloc[-252]['Price']) - 1) * 100 if len(prices) >= 252 else 0
        
        # YTD return
        ytd_start = prices[prices['Date'].dt.year == 2026].iloc[0]['Price'] if len(prices[prices['Date'].dt.year == 2026]) > 0 else current_price
        ret_ytd = ((current_price / ytd_start) - 1) * 100
        
        # Volatility
        vol_30d = prices.tail(30)['Returns'].std() * np.sqrt(252) * 100 if len(prices) >= 30 else 0
        vol_90d = prices.tail(90)['Returns'].std() * np.sqrt(252) * 100 if len(prices) >= 90 else 0
        
        # Max drawdown
        rolling_max = prices['Price'].expanding().max()
        drawdown = (prices['Price'] - rolling_max) / rolling_max
        max_dd = drawdown.min() * 100
        
        # 52-week high/low
        high_52w = prices.tail(252)['Price'].max() if len(prices) >= 252 else prices['Price'].max()
        low_52w = prices.tail(252)['Price'].min() if len(prices) >= 252 else prices['Price'].min()
        dist_from_high = ((current_price / high_52w) - 1) * 100
        dist_from_low = ((current_price / low_52w) - 1) * 100
        
        # Trend strength score (0-100)
        trend_score = 0
        if current_price > ma_7: trend_score += 15
        if current_price > ma_30: trend_score += 20
        if current_price > ma_90: trend_score += 25
        if current_price > ma_200: trend_score += 40
        if ma_7 > ma_30: trend_score += 10
        if ma_30 > ma_90: trend_score += 10
        trend_score = min(trend_score, 100)
        
        # Risk-adjusted return (Sharpe approximation)
        avg_return = prices['Returns'].mean() * 252 * 100
        risk_free = 15  # Nigerian T-bill rate
        sharpe = (avg_return - risk_free) / vol_90d if vol_90d > 0 else 0
        
        # Store results
        results.append({
            'Ticker': ticker,
            'Name': row['Name'],
            'Price': current_price,
            'Volume': row['Volume'],
            'Change': row['Change'],
            'MA_7': ma_7,
            'MA_30': ma_30,
            'MA_90': ma_90,
            'MA_200': ma_200,
            'Ret_1W': ret_1w,
            'Ret_1M': ret_1m,
            'Ret_3M': ret_3m,
            'Ret_6M': ret_6m,
            'Ret_1Y': ret_1y,
            'Ret_YTD': ret_ytd,
            'Vol_30D': vol_30d,
            'Vol_90D': vol_90d,
            'Max_DD': max_dd,
            'High_52W': high_52w,
            'Low_52W': low_52w,
            'Dist_High': dist_from_high,
            'Dist_Low': dist_from_low,
            'Trend_Score': trend_score,
            'Sharpe': sharpe,
            'Data_Points': len(prices)
        })
        
    except Exception as e:
        failed_stocks.append((ticker, str(e)))
        continue

print(f"\n✅ Analysis complete! Successfully analyzed {len(results)} stocks")
if failed_stocks:
    print(f"⚠️ Failed to analyze {len(failed_stocks)} stocks")

# Create DataFrame
df = pd.DataFrame(results)

# Save to CSV
output_file = '/home/Stock_pipeline/archive/data/raw/ngx_comprehensive_analysis.csv'
df.to_csv(output_file, index=False)
print(f"\n💾 Full data saved to: {output_file}")

print("\n" + "="*100)
print("📊 MARKET OVERVIEW")
print("="*100)

print(f"\nTotal Stocks Analyzed: {len(df)}")
print(f"Average Price: ₦{df['Price'].mean():.2f}")
print(f"Median Price: ₦{df['Price'].median():.2f}")
print(f"Average YTD Return: {df['Ret_YTD'].mean():.2f}%")
print(f"Average Volatility (30d): {df['Vol_30D'].mean():.2f}%")

print("\n" + "="*100)
print("🏆 TOP 20 BEST PERFORMERS - YTD 2026")
print("="*100)
top_ytd = df.nlargest(20, 'Ret_YTD')[['Ticker', 'Name', 'Price', 'Ret_YTD', 'Ret_1M', 'Vol_30D', 'Trend_Score']]
top_ytd.index = range(1, len(top_ytd) + 1)
print(top_ytd.to_string())

print("\n" + "="*100)
print("📈 TOP 20 STRONGEST TRENDS (Trend Score)")
print("="*100)
top_trend = df.nlargest(20, 'Trend_Score')[['Ticker', 'Name', 'Price', 'Trend_Score', 'Ret_YTD', 'Ret_1M', 'Vol_30D']]
top_trend.index = range(1, len(top_trend) + 1)
print(top_trend.to_string())

print("\n" + "="*100)
print("💎 TOP 20 VALUE OPPORTUNITIES (Near 52-Week Low)")
print("="*100)
# Stocks within 20% of 52-week low, positive trend
value = df[(df['Dist_Low'] < 20) & (df['Trend_Score'] > 40)].nlargest(20, 'Trend_Score')[
    ['Ticker', 'Name', 'Price', 'Dist_Low', 'Trend_Score', 'Ret_1M', 'Vol_30D']]
value.index = range(1, len(value) + 1)
print(value.to_string())

print("\n" + "="*100)
print("🚀 TOP 20 MOMENTUM STOCKS (1-Month Performance + Trend)")
print("="*100)
# High 1-month return with strong trend
df['Momentum_Score'] = df['Ret_1M'] * 0.6 + df['Trend_Score'] * 0.4
momentum = df.nlargest(20, 'Momentum_Score')[['Ticker', 'Name', 'Price', 'Ret_1M', 'Trend_Score', 'Vol_30D']]
momentum.index = range(1, len(momentum) + 1)
print(momentum.to_string())

print("\n" + "="*100)
print("⚖️ TOP 20 RISK-ADJUSTED PERFORMERS (Sharpe Ratio)")
print("="*100)
top_sharpe = df[df['Sharpe'] > 0].nlargest(20, 'Sharpe')[['Ticker', 'Name', 'Price', 'Sharpe', 'Ret_YTD', 'Vol_30D']]
top_sharpe.index = range(1, len(top_sharpe) + 1)
print(top_sharpe.to_string())

print("\n" + "="*100)
print("🎯 RECOMMENDED BUY LIST - TOP 10 BEST OPPORTUNITIES")
print("="*100)
print("\nCriteria: Strong trend (>60) + Positive YTD + Moderate volatility (<80%)\n")

buy_list = df[
    (df['Trend_Score'] >= 60) & 
    (df['Ret_YTD'] > 0) & 
    (df['Vol_30D'] < 80)
].nlargest(10, 'Trend_Score')[['Ticker', 'Name', 'Price', 'Trend_Score', 'Ret_YTD', 'Ret_1M', 'Vol_30D', 'Sharpe']]
buy_list.index = range(1, len(buy_list) + 1)
print(buy_list.to_string())

print("\n" + "="*100)
print("⚠️ WATCH LIST - HIGH RISK, HIGH REWARD")
print("="*100)
print("\nCriteria: High volatility (>80%) + Strong recent performance\n")

watch_list = df[
    (df['Vol_30D'] > 80) & 
    (df['Ret_1M'] > 20)
].nlargest(10, 'Ret_1M')[['Ticker', 'Name', 'Price', 'Ret_1M', 'Ret_YTD', 'Vol_30D', 'Trend_Score']]
watch_list.index = range(1, len(watch_list) + 1)
print(watch_list.to_string())

print("\n" + "="*100)
print("📉 BOTTOM 20 WORST PERFORMERS - YTD 2026")
print("="*100)
bottom_ytd = df.nsmallest(20, 'Ret_YTD')[['Ticker', 'Name', 'Price', 'Ret_YTD', 'Ret_1M', 'Trend_Score']]
bottom_ytd.index = range(1, len(bottom_ytd) + 1)
print(bottom_ytd.to_string())

print("\n" + "="*100)
print("📊 SECTOR SUMMARY STATISTICS")
print("="*100)

# Volume-based grouping (top, mid, small cap)
df['Market_Cap_Tier'] = pd.qcut(df['Volume'].fillna(0), q=3, labels=['Small Cap', 'Mid Cap', 'Large Cap'], duplicates='drop')
sector_stats = df.groupby('Market_Cap_Tier').agg({
    'Ret_YTD': 'mean',
    'Vol_30D': 'mean',
    'Trend_Score': 'mean',
    'Price': 'count'
}).round(2)
sector_stats.columns = ['Avg YTD %', 'Avg Vol %', 'Avg Trend', 'Count']
print(sector_stats)

print("\n" + "="*100)
print("💡 KEY INSIGHTS & RECOMMENDATIONS")
print("="*100)

# Market breadth
bullish_stocks = len(df[df['Trend_Score'] >= 60])
bearish_stocks = len(df[df['Trend_Score'] < 40])
neutral_stocks = len(df) - bullish_stocks - bearish_stocks

print(f"\n📈 Market Breadth:")
print(f"   Bullish (Trend >60): {bullish_stocks} stocks ({bullish_stocks/len(df)*100:.1f}%)")
print(f"   Neutral (Trend 40-60): {neutral_stocks} stocks ({neutral_stocks/len(df)*100:.1f}%)")
print(f"   Bearish (Trend <40): {bearish_stocks} stocks ({bearish_stocks/len(df)*100:.1f}%)")

# Market condition
avg_ytd = df['Ret_YTD'].mean()
if avg_ytd > 20:
    market_condition = "🔥 BULL MARKET - Strong uptrend"
elif avg_ytd > 5:
    market_condition = "📈 BULLISH - Moderate uptrend"
elif avg_ytd > -5:
    market_condition = "➡️ SIDEWAYS - Range-bound"
else:
    market_condition = "📉 BEARISH - Downtrend"

print(f"\n🌍 Overall Market Condition: {market_condition}")
print(f"   Average YTD Return: {avg_ytd:.2f}%")

# Best opportunities
print(f"\n🎯 Trading Opportunities:")
print(f"   Strong Buy (Trend >80, YTD >20%): {len(df[(df['Trend_Score'] > 80) & (df['Ret_YTD'] > 20)])} stocks")
print(f"   Buy (Trend >60, YTD >0%): {len(df[(df['Trend_Score'] > 60) & (df['Ret_YTD'] > 0)])} stocks")
print(f"   Value Plays (Near 52w low, Trend >40): {len(df[(df['Dist_Low'] < 20) & (df['Trend_Score'] > 40)])} stocks")

print(f"\n⚠️ Risk Warning:")
print(f"   High volatility stocks (>100%): {len(df[df['Vol_30D'] > 100])} stocks")
print(f"   Stocks at 52-week high: {len(df[df['Dist_High'] > -2])} stocks")

print("\n" + "="*100)
print("✅ ANALYSIS COMPLETE")
print("="*100)
print(f"\nFull dataset with {len(df)} stocks saved to CSV for further analysis.")
print("Review the rankings above to identify investment opportunities.\n")
