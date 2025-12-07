import os
import pandas as pd
from sqlalchemy import create_engine, MetaData, Table, Column, String, Float, Integer, Date
from sqlalchemy.dialects.postgresql import DOUBLE_PRECISION, BIGINT
from sqlalchemy.exc import SQLAlchemyError
from process import processed_df
from dotenv import load_dotenv

# Connect to PostgreSQL via SQLAlchemy
load_dotenv()
user = os.getenv("PGUSER")
password = os.getenv("PGPASSWORD")
host = os.getenv("PGHOST", "localhost")
port = os.getenv("PGPORT", "5432")
database = os.getenv("PGDATABASE")

pg_conn = f"postgresql://{user}:{password}@{host}:{port}/{database}"
engine = create_engine(pg_conn, echo=True)


metadata = MetaData(schema="public")

# Define table schema
stock_prices = Table(
    'stock_prices', metadata,
    Column('date', Date, primary_key=True),
    Column('open', DOUBLE_PRECISION),
    Column('high', DOUBLE_PRECISION),
    Column('low', DOUBLE_PRECISION),
    Column('close', DOUBLE_PRECISION),
    Column('volume', BIGINT),
    Column('ticker', String, primary_key=True),
    Column('percentage_change', DOUBLE_PRECISION),
    Column('movingAverage_7', DOUBLE_PRECISION),
    Column('movingAverage_30', DOUBLE_PRECISION),
    Column('volatility', DOUBLE_PRECISION),
)

# Create the table in DB (if not exists)
stock_prices.drop(engine, checkfirst=True)  # Drops the table if it exists
metadata.create_all(engine)  # Recreates it with new schema


# Function to load df into DB
def load_data(df, table, conn):
    try:
        # Insert data row by row (you can optimize with bulk_insert_mappings or df.to_sql)
        conn.execute(table.delete())  # Optional: clear existing data
        conn.execute(table.insert(), df.to_dict(orient='records'))
        print("Data loaded successfully!")
    except SQLAlchemyError as e:
        print(f"Error loading data: {e}")

# Load and process CSV file 
def main3():
    df_all = processed_df()
    df_all["date"] = pd.to_datetime(df_all["date"]).dt.date

    with engine.begin() as conn:  # transaction scope
        load_data(df_all, stock_prices, conn)

if __name__ == "__main__":
    main3()
    print("Data loaded into PostgreSQL database.")
