#!/usr/bin/env python3
"""Quick script to check records in Parquet files"""
import s3fs
import pyarrow.parquet as pq

fs = s3fs.S3FileSystem(
    key='minioadmin',
    secret='minioadmin123',
    endpoint_url='http://localhost:9000',
    use_ssl=False
)

# Sample Bronze files
bronze_files = list(fs.glob('air-quality-data/bronze/**/*.parquet'))[:10]
if bronze_files:
    total = 0
    for f in bronze_files:
        table = pq.read_table(fs.open(f))
        total += len(table)
    avg_per_file = total / len(bronze_files)
    print(f"Sample {len(bronze_files)} Bronze files: {total} records")
    print(f"Average: ~{avg_per_file:.0f} records per file")
    
    # Estimate total
    all_bronze = len(fs.glob('air-quality-data/bronze/**/*.parquet'))
    estimated = avg_per_file * all_bronze
    print(f"\nTotal Bronze files: {all_bronze}")
    print(f"Estimated total Bronze records: ~{estimated:,.0f}")

