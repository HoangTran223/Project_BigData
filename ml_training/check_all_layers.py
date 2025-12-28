"""
Check records count in all layers (Bronze, Silver, Gold).
"""

import s3fs
import pyarrow.parquet as pq
import pandas as pd

def check_layer(layer_name, fs, bucket='air-quality-data'):
    """Check records and countries in a layer."""
    path = f'{bucket}/{layer_name}/'
    files = sorted(fs.glob(f'{path}**/*.parquet'))
    
    if not files:
        print(f"\n{layer_name.upper()}: No files found")
        return None
    
    print(f"\n{layer_name.upper()} Layer:")
    print(f"  Files: {len(files)}")
    
    # Sample a few files to get structure
    sample_files = files[:min(10, len(files))]
    dfs = []
    for file_path in sample_files:
        try:
            with fs.open(file_path, 'rb') as f:
                df = pq.read_table(f).to_pandas()
                dfs.append(df)
        except:
            continue
    
    if not dfs:
        print(f"  Could not read sample files")
        return None
    
    sample_df = pd.concat(dfs, ignore_index=True)
    
    # Get countries
    countries = sorted(sample_df['country'].unique()) if 'country' in sample_df.columns else []
    
    print(f"  Sample records: {len(sample_df):,}")
    if countries:
        print(f"  Countries in sample: {countries} ({len(countries)} countries)")
    
    # Try to estimate total records (rough estimate)
    if len(files) > 0:
        avg_records_per_file = len(sample_df) / len(sample_files)
        estimated_total = avg_records_per_file * len(files)
        print(f"  Estimated total records: ~{estimated_total:,.0f}")
    
    return {
        'files': len(files),
        'sample_records': len(sample_df),
        'countries': countries,
        'estimated_total': estimated_total if len(files) > 0 else 0
    }

def main():
    print("=" * 80)
    print("Checking Records in All Layers")
    print("=" * 80)
    
    fs = s3fs.S3FileSystem(
        key='minioadmin',
        secret='minioadmin123',
        endpoint_url='http://localhost:9000',
        use_ssl=False
    )
    
    # Check all layers
    bronze_info = check_layer('bronze', fs)
    silver_info = check_layer('silver', fs)
    gold_info = check_layer('gold', fs)
    
    # Summary
    print("\n" + "=" * 80)
    print("Summary:")
    print("=" * 80)
    
    if bronze_info:
        print(f"Bronze: ~{bronze_info['estimated_total']:,.0f} records, {len(bronze_info['countries'])} countries")
    if silver_info:
        print(f"Silver: ~{silver_info['estimated_total']:,.0f} records, {len(silver_info['countries'])} countries")
    if gold_info:
        print(f"Gold: ~{gold_info['estimated_total']:,.0f} records, {len(gold_info['countries'])} countries")
    
    # Check missing countries
    expected = {'BN', 'KH', 'ID', 'LA', 'MY', 'MM', 'PH', 'SG', 'TH', 'TL', 'VN'}
    
    if bronze_info and bronze_info['countries']:
        bronze_countries = set(bronze_info['countries'])
        missing_bronze = expected - bronze_countries
        if missing_bronze:
            print(f"\n⚠️  Missing in Bronze: {sorted(missing_bronze)}")
        else:
            print(f"\n✓ Bronze has all 11 countries")
    
    if gold_info and gold_info['countries']:
        gold_countries = set(gold_info['countries'])
        missing_gold = expected - gold_countries
        if missing_gold:
            print(f"⚠️  Missing in Gold: {sorted(missing_gold)}")
        else:
            print(f"✓ Gold has all 11 countries")
    
    print("\n" + "=" * 80)
    print("Note: Estimated totals are rough. For exact counts, check ClickHouse or run full aggregation.")
    print("=" * 80)

if __name__ == "__main__":
    main()

