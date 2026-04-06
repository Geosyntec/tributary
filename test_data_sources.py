# test_data_sources.py
"""
Test script for all data sources.

This tests both USGS (always works) and Aquarius (needs credentials).
"""

from datetime import datetime, timedelta
import logging

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%H:%M:%S'
)

# Import our data sources
from data_sources import AquariusDataSource


def test_usgs():
    """Test USGS data source (always works - no auth needed)."""
    
    print("\n" + "=" * 60)
    print("TESTING USGS DATA SOURCE")
    print("=" * 60)
    
    usgs = USGSDataSource()
    
    # Test 1: Get sites
    print("\n1. Finding stream sites in Oregon...")
    sites = usgs.get_sites(state='OR', site_type='ST')
    print(f"   Found {len(sites)} sites")
    
    if sites:
        print("   First 3 sites:")
        for site in sites[:3]:
            print(f"   - {site.site_id}: {site.name}")
    
    # Test 2: Get data
    test_site = '14211720'
    print(f"\n2. Getting discharge data for {test_site}...")
    
    end_date = datetime.now()
    start_date = end_date - timedelta(days=3)
    
    data = usgs.get_data(
        site_ids=[test_site],
        parameter='00060',
        start_date=start_date,
        end_date=end_date
    )
    print(f"   Retrieved {len(data)} data points")
    
    if data:
        print(f"   First point: {data[0].timestamp} = {data[0].value} {data[0].unit}")
        print(f"   Last point: {data[-1].timestamp} = {data[-1].value} {data[-1].unit}")
    
    # Test 3: Convert to DataFrame
    print("\n3. Converting to DataFrame...")
    df = usgs.to_dataframe(data)
    print(f"   Shape: {df.shape}")
    print(f"   Columns: {list(df.columns)}")
    
    print("\n✓ USGS tests completed!")
    return df


def test_aquarius():
    """Test Aquarius data source (requires credentials)."""
    
    print("\n" + "=" * 60)
    print("TESTING AQUARIUS DATA SOURCE")
    print("=" * 60)
    
    # Try to load credentials from config
    try:
        from config import BASE_URL, USERNAME, PASSWORD
    except ImportError:
        print("   Skipping - config.py not found")
        print("   Create config.py with BASE_URL, USERNAME, PASSWORD")
        return None
    
    if not BASE_URL or not USERNAME:
        print("   Skipping - credentials not configured in config.py")
        return None
    
    try:
        # Connect to Aquarius
        aquarius = AquariusDataSource(
            base_url=BASE_URL,
            username=USERNAME,
            password=PASSWORD,
            verify_ssl=False
        )
        
        # Test 1: Get sites
        print("\n1. Getting locations...")
        sites = aquarius.get_sites()
        print(f"   Found {len(sites)} locations")
        
        if sites:
            print("   First 3 locations:")
            for site in sites[:3]:
                print(f"   - {site.site_id}: {site.name}")
        
        # Test 2: Get datasets
        print("\n2. Getting available datasets...")
        datasets = aquarius.get_datasets()
        print(f"   Found {len(datasets)} datasets")
        
        if datasets:
            print("   First 3 datasets:")
            for ds in datasets[:3]:
                print(f"   - {ds.get('identifier')}")
        
        # Test 3: Get parameters for a site (if we have sites)
        if sites:
            test_site = sites[0].site_id
            print(f"\n3. Getting parameters for {test_site}...")
            params = aquarius.get_available_parameters(test_site)
            print(f"   Found {len(params)} parameters")
            for p in params:
                print(f"   - {p['code']} ({p['unit']})")
        
        print("\n✓ Aquarius tests completed!")
        
    except Exception as e:
        print(f"   Error: {e}")
        return None


def test_combined_workflow():
    """Show how to combine data from multiple sources."""
    
    print("\n" + "=" * 60)
    print("COMBINED WORKFLOW EXAMPLE")
    print("=" * 60)
    
    import pandas as pd
    
    # Get USGS data
    print("\n1. Getting USGS discharge data...")
    usgs = USGSDataSource()
    
    end_date = datetime.now()
    start_date = end_date - timedelta(days=3)
    
    usgs_data = usgs.get_data(
        site_ids=['14211720'],
        parameter='00060',
        start_date=start_date,
        end_date=end_date
    )
    usgs_df = usgs.to_dataframe(usgs_data)
    print(f"   USGS: {len(usgs_df)} records")
    
    # Show combining would work
    print("\n2. Combining data from multiple sources...")
    print("   (Aquarius data would go here if configured)")
    
    # The key insight: all DataFrames have the same columns!
    print(f"\n   DataFrame columns: {list(usgs_df.columns)}")
    print("   Any data source produces these same columns!")
    print("   This means you can pd.concat() them together.")
    
    # Show the 'source' column
    print(f"\n   Sources in data: {usgs_df['source'].unique()}")
    
    print("\n✓ Combined workflow example complete!")


def main():
    """Run all tests."""
    
    print("\n" + "#" * 60)
    print("# DATA SOURCE TESTS")
    print("#" * 60)
    
    # Test USGS (always works)
    #test_usgs()
    
    # Test Aquarius (may skip if not configured)
    test_aquarius()
    
    # Show combined workflow
    test_combined_workflow()
    
    print("\n" + "#" * 60)
    print("# ALL TESTS COMPLETE!")
    print("#" * 60)


if __name__ == "__main__":
    main()