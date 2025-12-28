"""
Check number of stations (locations) per country in Gold layer.
"""

from data_loader import AirQualityDataLoader

def main():
    print("=" * 80)
    print("Checking Stations per Country in Gold Layer")
    print("=" * 80)
    
    loader = AirQualityDataLoader()
    
    # Load ALL data without date filter
    print("\nLoading all data...")
    df = loader.load_gold_layer(
        start_date=None,
        end_date=None
    )
    
    # Count unique locations per country
    print("\n" + "=" * 80)
    print("Stations (Unique Locations) per Country:")
    print("=" * 80)
    
    stations_per_country = df.groupby('country')['location_id'].nunique().sort_values(ascending=False)
    
    for country, count in stations_per_country.items():
        print(f"{country}: {count} stations")
    
    print(f"\nTotal countries: {len(stations_per_country)}")
    print(f"Total stations: {stations_per_country.sum()}")
    
    # Also show records per country for reference
    print("\n" + "=" * 80)
    print("Records per Country (for reference):")
    print("=" * 80)
    
    records_per_country = df.groupby('country').size().sort_values(ascending=False)
    for country, count in records_per_country.items():
        print(f"{country}: {count:,} records")
    
    print("\n" + "=" * 80)
    
    # Return data for documentation
    return stations_per_country.to_dict()

if __name__ == "__main__":
    main()

