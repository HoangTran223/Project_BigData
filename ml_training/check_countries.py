"""
Check countries and date ranges in Gold layer.
"""

from data_loader import AirQualityDataLoader

def main():
    print("=" * 80)
    print("Checking Countries and Date Ranges in Gold Layer")
    print("=" * 80)
    
    loader = AirQualityDataLoader()
    
    # Load ALL data without date filter
    print("\nLoading all data (no date filter)...")
    df = loader.load_gold_layer(
        start_date=None,
        end_date=None
    )
    
    print(f"\nTotal records: {len(df):,}")
    print(f"Overall date range: {df['datetime'].min()} to {df['datetime'].max()}")
    
    # Check countries
    countries = sorted(df['country'].unique())
    print(f"\nCountries found: {countries}")
    print(f"Total countries: {len(countries)}")
    
    # Expected countries
    expected = {'BN', 'KH', 'ID', 'LA', 'MY', 'MM', 'PH', 'SG', 'TH', 'TL', 'VN'}
    missing = expected - set(countries)
    
    if missing:
        print(f"\n⚠️  Missing countries: {sorted(missing)}")
        print("These countries may not have data in Gold layer")
    else:
        print("\n✓ All 11 countries found!")
    
    # Date range per country
    print("\n" + "=" * 80)
    print("Date Range per Country:")
    print("=" * 80)
    for country in countries:
        country_data = df[df['country'] == country]
        print(f"{country}: {country_data['datetime'].min()} to {country_data['datetime'].max()} ({len(country_data):,} records)")
    
    # Records per country
    print("\n" + "=" * 80)
    print("Records per Country:")
    print("=" * 80)
    country_counts = df.groupby('country').size().sort_values(ascending=False)
    for country, count in country_counts.items():
        print(f"{country}: {count:,} records")
    
    print("\n" + "=" * 80)

if __name__ == "__main__":
    main()

