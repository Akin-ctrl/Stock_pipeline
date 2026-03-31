#!/usr/bin/env python3
"""Check all columns returned by Afrimarket API"""

import afrimarket as afm
import pandas as pd

print("="*70)
print("AFRIMARKET API - COLUMN REFERENCE")
print("="*70)

# Initialize NGX Exchange
ngx = afm.Exchange(market='ngx')

# 1. Listed Companies
print("\n1️⃣ LISTED COMPANIES (Exchange.get_listed_companies())")
print("-" * 70)
df_companies = ngx.get_listed_companies()
print(f"Total columns: {len(df_companies.columns)}")
print(f"Columns: {df_companies.columns.tolist()}")
print(f"\nSample data:")
print(df_companies.head(3))

# 2. Top Gainers
print("\n\n2️⃣ TOP GAINERS (Exchange.get_top_gainers())")
print("-" * 70)
df_gainers = ngx.get_top_gainers()
print(f"Total columns: {len(df_gainers.columns)}")
print(f"Columns: {df_gainers.columns.tolist()}")
print(f"\nSample data:")
print(df_gainers.head(3))

# 3. Bottom Losers
print("\n\n3️⃣ BOTTOM LOSERS (Exchange.get_bottom_losers())")
print("-" * 70)
df_losers = ngx.get_bottom_losers()
print(f"Total columns: {len(df_losers.columns)}")
print(f"Columns: {df_losers.columns.tolist()}")
print(f"\nSample data:")
print(df_losers.head(3))

# 4. Index Price History
print("\n\n4️⃣ INDEX PRICE HISTORY (Exchange.get_index_price())")
print("-" * 70)
df_index = ngx.get_index_price()
print(f"Total columns: {len(df_index.columns)}")
print(f"Columns: {df_index.columns.tolist()}")
print(f"\nSample data:")
print(df_index.tail(3))

# 5. Individual Stock - Historical Prices
print("\n\n5️⃣ STOCK HISTORICAL PRICES (Stock.get_price())")
print("-" * 70)
stock = afm.Stock(ticker='uba', market='ngx')
df_prices = stock.get_price()
print(f"Total columns: {len(df_prices.columns)}")
print(f"Columns: {df_prices.columns.tolist()}")
print(f"\nSample data:")
print(df_prices.tail(3))

# 6. Stock Competitors
print("\n\n6️⃣ STOCK COMPETITORS (Stock.get_competitors())")
print("-" * 70)
df_competitors = stock.get_competitors()
print(f"Total columns: {len(df_competitors.columns)}")
print(f"Columns: {df_competitors.columns.tolist()}")
print(f"\nSample data:")
print(df_competitors.head(3))

# 7. Growth & Valuation
print("\n\n7️⃣ GROWTH & VALUATION (Stock.get_growth_and_valuation())")
print("-" * 70)
df_valuation = stock.get_growth_and_valuation()
print(f"Total columns: {len(df_valuation.columns)}")
print(f"Columns: {df_valuation.columns.tolist()}")
print(f"\nSample data:")
print(df_valuation)

# 8. Stock Market Performance (Period)
print("\n\n8️⃣ MARKET PERFORMANCE - PERIOD (Stock.get_stock_market_performance_period())")
print("-" * 70)
df_perf_period = stock.get_stock_market_performance_period()
print(f"Total columns: {len(df_perf_period.columns)}")
print(f"Columns: {df_perf_period.columns.tolist()}")
print(f"\nSample data:")
print(df_perf_period)

# 9. Stock Market Performance (Date)
print("\n\n9️⃣ MARKET PERFORMANCE - DATE (Stock.get_stock_market_performance_date())")
print("-" * 70)
df_perf_date = stock.get_stock_market_performance_date()
print(f"Total columns: {len(df_perf_date.columns)}")
print(f"Columns: {df_perf_date.columns.tolist()}")
print(f"\nSample data:")
print(df_perf_date.tail(3))

print("\n" + "="*70)
print("SUMMARY OF ALL AFRIMARKET API ENDPOINTS")
print("="*70)
print(f"""
1. get_listed_companies()        : {len(df_companies.columns)} columns
2. get_top_gainers()             : {len(df_gainers.columns)} columns
3. get_bottom_losers()           : {len(df_losers.columns)} columns
4. get_index_price()             : {len(df_index.columns)} columns
5. get_price()                   : {len(df_prices.columns)} columns
6. get_competitors()             : {len(df_competitors.columns)} columns
7. get_growth_and_valuation()    : {len(df_valuation.columns)} columns
8. get_stock_market_performance_period() : {len(df_perf_period.columns)} columns
9. get_stock_market_performance_date()   : {len(df_perf_date.columns)} columns
""")
