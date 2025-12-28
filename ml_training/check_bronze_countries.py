"""
Check countries in Bronze layer using ClickHouse or direct MinIO access.
"""

import subprocess
import sys

def check_via_clickhouse():
    """Check via ClickHouse if available."""
    try:
        result = subprocess.run(
            ['docker', 'exec', 'clickhouse', 'clickhouse-client', '--password', 'default123', 
             '--query', 'SELECT country, count(*) as cnt FROM bronze_measurements GROUP BY country ORDER BY country'],
            capture_output=True,
            text=True,
            timeout=10
        )
        if result.returncode == 0:
            print("Bronze Layer (from ClickHouse):")
            print(result.stdout)
            return True
    except:
        pass
    return False

def main():
    print("=" * 80)
    print("Checking Countries in Bronze Layer")
    print("=" * 80)
    
    if not check_via_clickhouse():
        print("\n⚠️  ClickHouse not available or bronze_measurements table not found")
        print("\nTo check Bronze layer:")
        print("1. Load Bronze to ClickHouse: python load_to_clickhouse.py --layer bronze")
        print("2. Or check directly in MinIO using Spark")
        print("\nExpected: 11 countries (BN, KH, ID, LA, MY, MM, PH, SG, TH, TL, VN)")
        print("If Bronze has 11 countries but Gold only has 9, check:")
        print("  - Bronze → Silver filter (value_standard is null or < 0)")
        print("  - Silver → Gold aggregation (may lose some records)")

if __name__ == "__main__":
    main()

