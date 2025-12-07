"""
NGX Daily Data Ingestion Script
Fetches current stock data from african-markets.com
"""

import requests
from requests.adapters import HTTPAdapter, Retry
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import os


def fetch_ngx_with_codes():
    """
    Fetch NGX company list including stock codes from href links
    """
    url = "https://www.african-markets.com/en/stock-markets/ngse/listed-companies"
    
    # Session with retries
    session = requests.Session()
    retries = Retry(total=5, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
    session.mount("http://", HTTPAdapter(max_retries=retries))
    session.mount("https://", HTTPAdapter(max_retries=retries))
    
    # Headers to look like a browser
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) "
                      "Chrome/120.0.0.0 Safari/537.36"
    }
    
    # Fetch page
    r = session.get(url, headers=headers, timeout=30)
    r.raise_for_status()
    
    soup = BeautifulSoup(r.text, "html.parser")
    table = soup.find("table")
    rows = table.find_all("tr")
    
    # Extract headers
    header_cells = rows[0].find_all(["th", "td"])
    headers = [cell.get_text(strip=True) for cell in header_cells]
    headers.append("Stock_Code")
    
    # Extract rows and stock codes
    data = []
    for row in rows[1:]:
        cols = [td.get_text(strip=True) for td in row.find_all("td")]
        
        # Extract stock code from link
        link = row.find("a")
        stock_code = None
        if link and 'href' in link.attrs:
            href = link['href']
            if '?code=' in href:
                stock_code = href.split('?code=')[1]
        
        if cols:
            cols.append(stock_code)
            data.append(cols)
    
    df = pd.DataFrame(data, columns=headers)
    return df


def ngx_daily_ingest(save_raw=True):
    """
    Main ingestion function
    
    Args:
        save_raw: Whether to save raw data to CSV
        
    Returns:
        DataFrame with NGX data
    """
    print("="*80)
    print(f"📥 NGX DAILY DATA INGESTION - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*80)
    
    # Fetch data
    print("\n🔍 Fetching current NGX data from african-markets.com...")
    df = fetch_ngx_with_codes()
    
    # Add metadata
    df['ingest_date'] = datetime.now().date()
    df['ingest_timestamp'] = datetime.now()
    df['source'] = 'african-markets.com'
    
    # Clean column names
    df.columns = df.columns.str.lower().str.replace(' ', '_').str.replace('.', '')
    
    print(f"✅ Successfully fetched {len(df)} stocks")
    
    # Save raw data if requested
    if save_raw:
        today = datetime.now().strftime("%Y-%m-%d")
        raw_dir = f"/home/Stock_pipeline/data/raw/ngx/{today}"
        os.makedirs(raw_dir, exist_ok=True)
        
        raw_file = f"{raw_dir}/ngx_raw_{today}.csv"
        df.to_csv(raw_file, index=False)
        print(f"💾 Saved raw data to: {raw_file}")
    
    return df


if __name__ == "__main__":
    # Run ingestion
    df_ngx = ngx_daily_ingest(save_raw=True)
    
    print(f"\n{'='*80}")
    print(f"✅ Ingestion complete: {len(df_ngx)} stocks")
    print(f"{'='*80}")
    
    # Display sample
    print("\nSample data:")
    print(df_ngx[['company', 'stock_code', 'price', 'sector']].head(10))
