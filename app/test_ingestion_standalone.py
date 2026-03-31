#!/usr/bin/env python3
"""
Standalone staging ingestion test - simulates production workflow WITHOUT database.

Tests the sequence:
1. Fetch from NGX
2. Fetch from Afrimarket  
3. Verify data structure and source tags
4. Verify dates
"""

import sys
from datetime import date
import pandas as pd

# Add app to path
sys.path.insert(0, '/home/Stock_pipeline')

from app.services.data_sources import NGXDataSource
from app.services.data_sources.afrimarket_source import AfrimarketDataSource

def print_separator():
    print("=" * 80)

def test_staging_ingestion_workflow():
    """Test complete ingestion workflow without database."""
    
    print_separator()
    print("STAGING INGESTION WORKFLOW TEST")
    print_separator()
    
    execution_date = date.today()
    print(f"\nExecution date: {execution_date}")
    
    # STEP 1: Fetch from NGX
    print("\n[STEP 1] Fetching from NGX...")
    print("-" * 80)
    
    ngx_source = NGXDataSource()
    ngx_data = pd.DataFrame()
    
    try:
        ngx_data = ngx_source.fetch(execution_date)
        if not ngx_data.empty:
            print(f"✅ NGX fetched {len(ngx_data)} records")
            print(f"   Columns: {list(ngx_data.columns)}")
            print(f"   Sample stocks: {', '.join(ngx_data['stock_code'].head(5).tolist())}")
            print(f"   Price dates: {ngx_data['price_date'].unique().tolist()}")
            
            # Verify source tag
            if 'source' in ngx_data.columns:
                sources = ngx_data['source'].unique()
                print(f"   Source values: {sources.tolist()}")
                assert (ngx_data['source'] == 'ngx').all(), "❌ FAIL: Not all records tagged with source='ngx'"
                print("   ✅ All records correctly tagged with source='ngx'")
            else:
                print("   ❌ FAIL: Missing 'source' column in NGX data")
                return False
                
        else:
            print("⚠️  NGX returned no data (may be blocked or no market data)")
    except Exception as e:
        print(f"⚠️  NGX fetch failed: {e}")
    
    # STEP 2: Fetch from Afrimarket
    print("\n[STEP 2] Fetching from Afrimarket...")
    print("-" * 80)
    
    afm_source = AfrimarketDataSource()
    afm_data = pd.DataFrame()
    
    try:
        afm_data = afm_source.fetch()
        if not afm_data.empty:
            # Simulate orchestrator: tag with execution_date
            afm_data['price_date'] = execution_date
            
            print(f"✅ Afrimarket fetched {len(afm_data)} records")
            print(f"   Columns: {list(afm_data.columns)}")
            print(f"   Sample stocks: {', '.join(afm_data['stock_code'].head(5).tolist())}")
            print(f"   Price dates (after tagging): {afm_data['price_date'].unique().tolist()}")
            
            # Verify source tag
            if 'source' in afm_data.columns:
                sources = afm_data['source'].unique()
                print(f"   Source values: {sources.tolist()}")
                assert (afm_data['source'] == 'afrimarket').all(), "❌ FAIL: Not all records tagged with source='afrimarket'"
                print("   ✅ All records correctly tagged with source='afrimarket'")
            else:
                print("   ❌ FAIL: Missing 'source' column in Afrimarket data")
                return False
        else:
            print("❌ FAIL: Afrimarket returned no data")
            return False
    except Exception as e:
        print(f"❌ FAIL: Afrimarket fetch failed: {e}")
        return False
    
    # STEP 3: Combine and verify
    print("\n[STEP 3] Combining data...")
    print("-" * 80)
    
    all_data = []
    if not ngx_data.empty:
        all_data.append(ngx_data)
    if not afm_data.empty:
        all_data.append(afm_data)
    
    if len(all_data) == 0:
        print("❌ FAIL: No data from any source")
        return False
    
    combined = pd.concat(all_data, ignore_index=True)
    print(f"✅ Combined {len(combined)} total records")
    
    # Source distribution
    source_counts = combined['source'].value_counts()
    print(f"\n   Source distribution:")
    for source, count in source_counts.items():
        print(f"   - {source}: {count} records")
    
    # Verify NO NULL sources
    null_sources = combined['source'].isnull().sum()
    if null_sources > 0:
        print(f"❌ FAIL: Found {null_sources} records with NULL source")
        return False
    print("   ✅ No NULL source values")
    
    # Date distribution
    print(f"\n   Date distribution:")
    date_source = combined.groupby(['price_date', 'source']).size().reset_index(name='count')
    for _, row in date_source.iterrows():
        print(f"   - {row['price_date']} | {row['source']:12} | {row['count']} records")
    
    # STEP 4: Verify required columns
    print("\n[STEP 4] Verifying data structure...")
    print("-" * 80)
    
    required_cols = ['stock_code', 'price_date', 'close_price', 'source']
    missing = [col for col in required_cols if col not in combined.columns]
    
    if missing:
        print(f"❌ FAIL: Missing required columns: {missing}")
        return False
    
    print(f"✅ All required columns present: {required_cols}")
    
    # Verify no nulls in critical fields
    for col in ['stock_code', 'price_date', 'source']:
        nulls = combined[col].isnull().sum()
        if nulls > 0:
            print(f"❌ FAIL: {col} has {nulls} NULL values")
            return False
    print("✅ No NULL values in critical fields")
    
    # STEP 5: Summary
    print("\n" + "=" * 80)
    print("✅ STAGING INGESTION WORKFLOW TEST PASSED")
    print("=" * 80)
    print(f"Total records ready for staging: {len(combined)}")
    print(f"Sources: {', '.join(source_counts.index.tolist())}")
    print(f"Dates: {combined['price_date'].nunique()} unique date(s)")
    print(f"Stocks: {combined['stock_code'].nunique()} unique stocks")
    print("\n✅ All data properly tagged and ready for database insertion")
    
    return True

if __name__ == "__main__":
    try:
        success = test_staging_ingestion_workflow()
        sys.exit(0 if success else 1)
    except Exception as e:
        print(f"\n❌ TEST FAILED WITH EXCEPTION: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
