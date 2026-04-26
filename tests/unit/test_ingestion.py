from app.services.data_sources import NGXDataSource
from app.services.data_sources.afrimarket_source import AfrimarketDataSource
from datetime import date

execution_date = date.today()
print("="*80)
print("STAGING INGESTION TEST")
print("="*80)
print(f"Execution date: {execution_date}")

# Test NGX
print("\n[1] NGX Fetch...")
try:
    ngx = NGXDataSource()
    ngx_data = ngx.fetch(execution_date)
    if not ngx_data.empty:
        print(f"✅ NGX: {len(ngx_data)} records")
        print(f"   Dates: {list(ngx_data['price_date'].unique())}")
        print(f"   Sources: {list(ngx_data['source'].unique())}")
    else:
        print("⚠️  NGX: No data")
except Exception as e:
    print(f"⚠️  NGX error: {e}")

# Test Afrimarket  
print("\n[2] Afrimarket Fetch...")
afm = AfrimarketDataSource()
afm_data = afm.fetch()
afm_data['price_date'] = execution_date
print(f"✅ Afrimarket: {len(afm_data)} records")
print(f"   Dates: {list(afm_data['price_date'].unique())}")
print(f"   Sources: {list(afm_data['source'].unique())}")

print("\n" + "="*80)
print("✅ INGESTION TEST COMPLETE")
