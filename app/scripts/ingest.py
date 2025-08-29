import requests
import pandas as pd
import os
from dotenv import load_dotenv
from datetime import datetime
from alpha_vantage.timeseries import TimeSeries as TS

# Load environment variables
load_dotenv()
API_KEY = os.getenv("ALPHA_VANTAGE_API_KEY")
BASE_URL = "https://www.alphavantage.co/query"

# Function to fetch stock data
TICKERS = ["AAPL", "MSFT", "NVDA", "TSLA", "AMZN"]
ts= TS(key=API_KEY, output_format='pandas')

def fetch_and_save_stock_data(symbol):
    try:
        data, meta_data = ts.get_daily(symbol=symbol, outputsize='full')
        data['ticker'] = symbol
        data.index = pd.to_datetime(data.index)
        data.sort_index(ascending=True, inplace=True)

        timestamp = datetime.now().strftime("%Y-%m-%d")
        
        ingest_filepath = f"/home/Stock_pipeline/data/raw/{timestamp}/"
        os.makedirs(ingest_filepath, exist_ok=True)
        
        filename = f"{ingest_filepath}{symbol}_daily_{timestamp}.csv"

        data.to_csv(filename)
        print(f"{symbol} data saved to {filename}.")
    except Exception as e:
        print(f"Error fetching {symbol}: {e}")

def main():
    for ticker in TICKERS:
        fetch_and_save_stock_data(ticker)

if __name__ == "__main__":
    main()