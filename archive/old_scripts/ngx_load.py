"""
NGX Data Load Script
Loads processed data into PostgreSQL
"""

import pandas as pd
from sqlalchemy import create_engine, MetaData, Table, Column, String, Float, Date, DateTime, Boolean
from sqlalchemy.dialects.postgresql import DOUBLE_PRECISION
from sqlalchemy.exc import SQLAlchemyError
from datetime import datetime
import os
from dotenv import load_dotenv


def create_ngx_table(engine):
    """
    Create NGX daily prices table
    """
    metadata = MetaData(schema="public")
    
    ngx_daily_prices = Table(
        'ngx_daily_prices', metadata,
        Column('ingest_date', Date, primary_key=True),
        Column('stock_code', String, primary_key=True),
        Column('company', String),
        Column('sector', String),
        Column('price', DOUBLE_PRECISION),
        Column('change_1d_pct', DOUBLE_PRECISION),
        Column('change_ytd_pct', DOUBLE_PRECISION),
        Column('market_cap', String),
        Column('is_active', Boolean),
        Column('has_price_data', Boolean),
        Column('ingest_timestamp', DateTime),
        Column('source', String),
    )
    
    # Drop and recreate for now (will change to upsert later)
    ngx_daily_prices.drop(engine, checkfirst=True)
    metadata.create_all(engine)
    
    print("✅ Table 'ngx_daily_prices' created")
    return ngx_daily_prices


def load_to_postgres(df, table, engine):
    """
    Load DataFrame to PostgreSQL
    """
    try:
        # Use pandas to_sql for simplicity
        df.to_sql(
            name=table.name,
            con=engine,
            if_exists='append',
            index=False,
            method='multi'
        )
        print(f"✅ Loaded {len(df)} records to PostgreSQL")
        return True
    except SQLAlchemyError as e:
        print(f"❌ Error loading data: {e}")
        return False


def ngx_load_data(df_processed=None):
    """
    Main load function
    
    Args:
        df_processed: Processed DataFrame (if None, loads from latest file)
    """
    print("="*80)
    print(f"💾 NGX DATA LOAD - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*80)
    
    # Load environment variables
    load_dotenv()
    user = os.getenv("PGUSER")
    password = os.getenv("PGPASSWORD")
    host = os.getenv("PGHOST", "localhost")
    port = os.getenv("PGPORT", "5432")
    database = os.getenv("PGDATABASE")
    
    # Create connection
    pg_conn = f"postgresql://{user}:{password}@{host}:{port}/{database}"
    engine = create_engine(pg_conn, echo=False)
    
    print(f"🔌 Connected to PostgreSQL: {host}:{port}/{database}")
    
    # Create table
    table = create_ngx_table(engine)
    
    # Load data from file if not provided
    if df_processed is None:
        latest_file = "/home/Stock_pipeline/data/processed/ngx/ngx_latest.csv"
        print(f"\n📂 Loading data from: {latest_file}")
        df_processed = pd.read_csv(latest_file)
    
    # Convert date columns
    df_processed['ingest_date'] = pd.to_datetime(df_processed['ingest_date']).dt.date
    df_processed['ingest_timestamp'] = pd.to_datetime(df_processed['ingest_timestamp'])
    
    # Select columns that match table schema
    columns_to_load = [
        'ingest_date', 'stock_code', 'company', 'sector', 
        'price', 'change_1d_pct', 'change_ytd_pct', 'market_cap',
        'is_active', 'has_price_data', 'ingest_timestamp', 'source'
    ]
    
    df_to_load = df_processed[columns_to_load]
    
    # Load to database
    print(f"\n📊 Loading {len(df_to_load)} records to database...")
    success = load_to_postgres(df_to_load, table, engine)
    
    if success:
        print(f"\n{'='*80}")
        print(f"✅ Load complete!")
        print(f"   - Date: {df_to_load['ingest_date'].iloc[0]}")
        print(f"   - Records: {len(df_to_load)}")
        print(f"   - Active stocks: {df_to_load['is_active'].sum()}")
        print(f"{'='*80}")
    
    return success


if __name__ == "__main__":
    success = ngx_load_data()
    
    if not success:
        print("\n❌ Load failed!")
        exit(1)
