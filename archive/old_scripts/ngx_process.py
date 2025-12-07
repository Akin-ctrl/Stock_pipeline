"""
NGX Data Processing Script
Cleans and transforms raw NGX data
"""

import pandas as pd
from datetime import datetime
import os


def ngx_process_data(df_raw):
    """
    Clean and transform NGX data
    
    Args:
        df_raw: Raw DataFrame from ingestion
        
    Returns:
        Processed DataFrame
    """
    print("="*80)
    print(f"🔧 NGX DATA PROCESSING - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*80)
    
    df = df_raw.copy()
    
    # Convert price to float (handle '-' and currency symbols)
    print("\n📊 Cleaning price data...")
    df['price'] = df['price'].replace('-', None)
    df['price'] = pd.to_numeric(df['price'], errors='coerce')
    
    # Convert percentage columns
    print("📊 Cleaning percentage data...")
    for col in ['1d', 'ytd']:
        if col in df.columns:
            df[col] = df[col].str.replace('%', '').str.replace('+', '').replace('-', None)
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    # Rename columns for clarity
    df.rename(columns={
        '1d': 'change_1d_pct',
        'ytd': 'change_ytd_pct',
        'mcap': 'market_cap'
    }, inplace=True)
    
    # Add derived fields
    df['has_price_data'] = df['price'].notna()
    df['is_active'] = df['price'].notna() & (df['price'] > 0)
    
    # Clean market cap
    if 'market_cap' in df.columns:
        df['market_cap'] = df['market_cap'].replace('-', None)
    
    # Data quality summary
    print(f"\n✅ Processed {len(df)} records")
    print(f"   - Active stocks: {df['is_active'].sum()}")
    print(f"   - Stocks with price data: {df['has_price_data'].sum()}")
    print(f"   - Missing price: {df['price'].isna().sum()}")
    
    # Sector distribution
    print(f"\n📊 Sector Distribution:")
    print(df['sector'].value_counts().head(10))
    
    return df


def save_processed_data(df_processed):
    """
    Save processed data to CSV
    
    Args:
        df_processed: Processed DataFrame
    """
    today = datetime.now().strftime("%Y-%m-%d")
    
    # Create output directory
    output_dir = "/home/Stock_pipeline/data/processed/ngx"
    os.makedirs(output_dir, exist_ok=True)
    
    # Save dated file
    output_file = f"{output_dir}/ngx_daily_{today}.csv"
    df_processed.to_csv(output_file, index=False)
    print(f"\n💾 Saved daily data to: {output_file}")
    
    # Save latest snapshot
    latest_file = f"{output_dir}/ngx_latest.csv"
    df_processed.to_csv(latest_file, index=False)
    print(f"💾 Saved latest snapshot to: {latest_file}")
    
    return output_file


if __name__ == "__main__":
    # Load raw data from latest ingestion
    today = datetime.now().strftime("%Y-%m-%d")
    raw_file = f"/home/Stock_pipeline/data/raw/ngx/{today}/ngx_raw_{today}.csv"
    
    print(f"📂 Loading raw data from: {raw_file}")
    df_raw = pd.read_csv(raw_file)
    
    # Process data
    df_processed = ngx_process_data(df_raw)
    
    # Save processed data
    output_file = save_processed_data(df_processed)
    
    print(f"\n{'='*80}")
    print(f"✅ Processing complete!")
    print(f"{'='*80}")
