import os
import pandas as pd
from datetime import datetime

# Directories
timestamp = datetime.now().strftime("%Y-%m-%d")
ingest_filepath = f"/home/Stock_pipeline/data/raw/{timestamp}/"
processed_filepath = f"/home/Stock_pipeline/data/processed/{timestamp}/"
os.makedirs(processed_filepath, exist_ok=True)

# Function to process individual stock files
def process_stock_file(filepath):
    df = pd.read_csv(filepath, parse_dates=["date"])

    df.rename(columns={"1. open": "open", "2. high": "high", "3. low": "low", "4. close": "close", "5. volume": "volume"}, inplace=True)
    df = df[["date", "open","high","low", "close", "volume", "ticker"]].copy()

    df["date"] = pd.to_datetime(df["date"])
    df["percentage_change"] = df["close"].pct_change() * 100
    df["movingAverage_7"] = df["close"].rolling(window=7).mean()
    df["movingAverage_30"] = df["close"].rolling(window=30).mean()
    df["volatility"] = df["close"].rolling(window=30).std()

    df.dropna(inplace=True)
    df.drop_duplicates(inplace=True)

    return df

# Function to process all stock files and return a combined DataFrame
def get_processed_dataframe(save_files=False):
    combined_df = []

    for filename in os.listdir(ingest_filepath):
        if filename.endswith(".csv"):
            filepath = os.path.join(ingest_filepath, filename)
            df_processed = process_stock_file(filepath)

            if save_files:
                out_file = os.path.join(processed_filepath, filename.replace("_daily", "_processed"))
                df_processed.to_csv(out_file, index=False)
                print(f"Processed and saved: {out_file}")

            combined_df.append(df_processed)

    if combined_df:
        return pd.concat(combined_df, ignore_index=True)
    else:
        return pd.DataFrame()

def processed_df():
    return get_processed_dataframe(save_files=False)

if __name__ == "__main__":
    get_processed_dataframe(save_files=True)
    print("All files processed and saved.")
