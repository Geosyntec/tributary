# tests/test_aquarius.py
"""
Tests for the Aquarius data source.

Run with:
    python tests/test_aquarius.py
"""

import sys
from pathlib import Path

# Add the parent directory to Python's path
# This allows us to import from data_sources
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# Now the imports will work
from datetime import datetime
import logging


# Set up logging so we can see what's happening
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)


def test_imports():
    """Test that we can import everything."""
    
    print("\n" + "=" * 60)
    print("TEST: Imports")
    print("=" * 60)
    
    try:
        from data_sources import AquariusDataSource
        print("  ✓ Can import AquariusDataSource")
    except ImportError as e:
        print(f"  ✗ Failed to import AquariusDataSource: {e}")
        return False
    
    try:
        from data_sources import DataPoint, SiteInfo
        print("  ✓ Can import DataPoint and SiteInfo")
    except ImportError as e:
        print(f"  ✗ Failed to import DataPoint/SiteInfo: {e}")
        return False
    
    try:
        from data_sources import BaseDataSource
        print("  ✓ Can import BaseDataSource")
    except ImportError as e:
        print(f"  ✗ Failed to import BaseDataSource: {e}")
        return False
    
    # Verify AquariusDataSource inherits from BaseDataSource
    from data_sources import AquariusDataSource, BaseDataSource
    if issubclass(AquariusDataSource, BaseDataSource):
        print("  ✓ AquariusDataSource inherits from BaseDataSource")
    else:
        print("  ✗ AquariusDataSource does NOT inherit from BaseDataSource")
        return False
    
    return True


def test_datapoint():
    """Test that DataPoint works correctly."""
    
    print("\n" + "=" * 60)
    print("TEST: DataPoint")
    print("=" * 60)
    
    from data_sources import DataPoint
    
    # Create a DataPoint
    try:
        point = DataPoint(
            timestamp=datetime(2024, 1, 15, 12, 0, 0),
            value=0.15,
            location_id="HYDRA-1",
            parameter="Precipitation",
            unit="inches",
            source="Aquarius"
        )
        print("  ✓ Created DataPoint successfully")
    except Exception as e:
        print(f"  ✗ Failed to create DataPoint: {e}")
        return False
    
    # Check attributes
    if point.value == 0.15:
        print("  ✓ point.value is correct")
    else:
        print(f"  ✗ point.value is wrong: {point.value}")
        return False
    
    if point.location_id == "HYDRA-1":
        print("  ✓ point.location_id is correct")
    else:
        print(f"  ✗ point.location_id is wrong: {point.location_id}")
        return False
    
    # Test to_dict()
    try:
        d = point.to_dict()
        print("  ✓ to_dict() works")
        
        if d['value'] == 0.15:
            print("  ✓ to_dict()['value'] is correct")
        else:
            print(f"  ✗ to_dict()['value'] is wrong: {d['value']}")
            return False
        
        # Note: to_dict() uses 'location' not 'location_id'
        if d['location'] == "HYDRA-1":
            print("  ✓ to_dict()['location'] is correct")
        else:
            print(f"  ✗ to_dict()['location'] is wrong: {d['location']}")
            return False
            
    except Exception as e:
        print(f"  ✗ to_dict() failed: {e}")
        return False
    
    return True


def test_siteinfo():
    """Test that SiteInfo works correctly."""
    
    print("\n" + "=" * 60)
    print("TEST: SiteInfo")
    print("=" * 60)
    
    from data_sources import SiteInfo
    
    # Create a SiteInfo
    try:
        site = SiteInfo(
            site_id="HYDRA-1",
            name="Hydra Rain Gauge 1",
            latitude=45.52,
            longitude=-122.68,
            source="Aquarius"
        )
        print("  ✓ Created SiteInfo successfully")
    except Exception as e:
        print(f"  ✗ Failed to create SiteInfo: {e}")
        return False
    
    # Check attributes
    if site.site_id == "HYDRA-1":
        print("  ✓ site.site_id is correct")
    else:
        print(f"  ✗ site.site_id is wrong: {site.site_id}")
        return False
    
    if site.source == "Aquarius":
        print("  ✓ site.source is correct")
    else:
        print(f"  ✗ site.source is wrong: {site.source}")
        return False
    
    # Test that optional fields work
    try:
        minimal_site = SiteInfo(site_id="TEST", name="Test Site")
        
        if minimal_site.latitude is None:
            print("  ✓ Optional fields default to None")
        else:
            print(f"  ✗ Optional field should be None: {minimal_site.latitude}")
            return False
            
    except Exception as e:
        print(f"  ✗ Minimal SiteInfo failed: {e}")
        return False
    
    return True


def test_aquarius_connection():
    """Test connecting to Aquarius (requires credentials)."""
    
    print("\n" + "=" * 60)
    print("TEST: Aquarius Connection")
    print("=" * 60)
    
    # Try to load credentials
    try:
        from config import BASE_URL, USERNAME, PASSWORD
        print("  ✓ Loaded credentials from config.py")
    except ImportError:
        print("  - SKIPPED: config.py not found")
        return None
    
    if not BASE_URL or not USERNAME:
        print("  - SKIPPED: Credentials not set in config.py")
        return None
    
    print(f"  Base URL: {BASE_URL[:50]}...")
    print(f"  Username: {USERNAME}")
    
    # Try to connect
    try:
        from data_sources import AquariusDataSource
        
        aquarius = AquariusDataSource(
            base_url=BASE_URL,
            username=USERNAME,
            password=PASSWORD,
            verify_ssl=False
        )
        print("  ✓ Connected to Aquarius!")
        
    except Exception as e:
        print(f"  ✗ Connection failed: {e}")
        return False
    
    # Test source_name property
    if aquarius.source_name == "Aquarius":
        print("  ✓ source_name is 'Aquarius'")
    else:
        print(f"  ✗ source_name is wrong: {aquarius.source_name}")
        return False
    
    # Test base_url property
    if aquarius.base_url == BASE_URL:
        print("  ✓ base_url is correct")
    else:
        print(f"  ✗ base_url is wrong")
        return False
    
    return True


def test_get_sites():
    """Test getting sites from Aquarius."""
    
    print("\n" + "=" * 60)
    print("TEST: get_sites()")
    print("=" * 60)
    
    # Load credentials
    try:
        from config import BASE_URL, USERNAME, PASSWORD
    except ImportError:
        print("  - SKIPPED: config.py not found")
        return None
    
    if not BASE_URL or not USERNAME:
        print("  - SKIPPED: Credentials not set")
        return None
    
    # Connect
    try:
        from data_sources import AquariusDataSource
        
        aquarius = AquariusDataSource(
            base_url=BASE_URL,
            username=USERNAME,
            password=PASSWORD,
            verify_ssl=False
        )
    except Exception as e:
        print(f"  ✗ Connection failed: {e}")
        return False
    
    # Get sites
    try:
        sites = aquarius.get_sites()
        print(f"  ✓ get_sites() returned {len(sites)} sites")
    except Exception as e:
        print(f"  ✗ get_sites() failed: {e}")
        return False
    
    # Check the results
    if not sites:
        print("  ⚠ Warning: No sites returned (might be OK)")
        return True
    
    # Check first site
    first_site = sites[0]
    print(f"  First site: {first_site.site_id} - {first_site.name}")
    
    if hasattr(first_site, 'site_id') and first_site.site_id:
        print("  ✓ Site has site_id")
    else:
        print("  ✗ Site missing site_id")
        return False
    
    if first_site.source == "Aquarius":
        print("  ✓ Site source is 'Aquarius'")
    else:
        print(f"  ✗ Site source is wrong: {first_site.source}")
        return False
    
    return True


def test_get_datasets():
    """Test getting datasets from Aquarius."""
    
    print("\n" + "=" * 60)
    print("TEST: get_datasets()")
    print("=" * 60)
    
    # Load credentials
    try:
        from config import BASE_URL, USERNAME, PASSWORD
    except ImportError:
        print("  - SKIPPED: config.py not found")
        return None
    
    if not BASE_URL or not USERNAME:
        print("  - SKIPPED: Credentials not set")
        return None
    
    # Connect
    try:
        from data_sources import AquariusDataSource
        
        aquarius = AquariusDataSource(
            base_url=BASE_URL,
            username=USERNAME,
            password=PASSWORD,
            verify_ssl=False
        )
    except Exception as e:
        print(f"  ✗ Connection failed: {e}")
        return False
    
    # Get datasets
    try:
        datasets = aquarius.get_datasets()
        print(f"  ✓ get_datasets() returned {len(datasets)} datasets")
    except Exception as e:
        print(f"  ✗ get_datasets() failed: {e}")
        return False
    
    # Check results
    if not datasets:
        print("  ⚠ Warning: No datasets returned")
        return True
    
    # Show first few
    print("  First 3 datasets:")
    for ds in datasets[:3]:
        identifier = ds.get('identifier', 'unknown')
        print(f"    - {identifier}")
    
    return True

def debug_aquarius_response():
    """Peek at the raw API response to see field names."""
    
    print("\n" + "=" * 60)
    print("DEBUG: Raw Aquarius Response")
    print("=" * 60)
    
    # Load credentials
    try:
        from config import BASE_URL, USERNAME, PASSWORD
    except ImportError:
        print("  - SKIPPED: config.py not found")
        return
    
    from data_sources import AquariusDataSource
    
    aquarius = AquariusDataSource(
        base_url=BASE_URL,
        username=USERNAME,
        password=PASSWORD,
        verify_ssl=False
    )
    
    # Look at the RAW response directly
    response = aquarius._get("locations")
    
    if not response:
        print("  No response!")
        return
    
    # Print the top-level keys
    print(f"\nTop-level keys in response: {list(response.keys())}")
    
    # Get the locations list
    # Try different possible key names
    for key in ['locations', 'Locations', 'items', 'Items', 'results', 'Results']:
        if key in response:
            print(f"\nFound locations under key: '{key}'")
            locations = response[key]
            print(f"Number of locations: {len(locations)}")
            
            if locations:
                print(f"\nFirst location - ALL FIELDS:")
                first = locations[0]
                for field_name, field_value in first.items():
                    print(f"  '{field_name}': {field_value}")
            break
    else:
        print("\nCouldn't find locations list!")
        print("Full response (first 500 chars):")
        print(str(response)[:500])
    
    # Also check datasets endpoint
    print("\n" + "=" * 60)
    print("DEBUG: Datasets endpoint")
    print("=" * 60)
    
    # Try different endpoint names
    for endpoint in ['data-set', 'datasets', 'DataSets', 'dataset']:
        print(f"\nTrying endpoint: '{endpoint}'")
        ds_response = aquarius._get(endpoint)
        
        if ds_response:
            print(f"  SUCCESS! Keys: {list(ds_response.keys())}")
            
            # Find the datasets list
            for key in ds_response.keys():
                value = ds_response[key]
                if isinstance(value, list):
                    print(f"  Found list under key '{key}': {len(value)} items")
                    if value:
                        print(f"  First item fields: {list(value[0].keys())}")
            break
        else:
            print(f"  Failed (404 or error)")

def main():
    """Run all tests."""
    
    print("\n" + "#" * 60)
    print("#  AQUARIUS DATA SOURCE TESTS")
    print("#" * 60)
    
    results = []
    
    # Run tests
    results.append(("Imports", test_imports()))
    results.append(("DataPoint", test_datapoint()))
    results.append(("SiteInfo", test_siteinfo()))
    results.append(("Aquarius Connection", test_aquarius_connection()))
    results.append(("get_sites()", test_get_sites()))
    results.append(("get_datasets()", test_get_datasets()))
    
    # Add debug call
    debug_aquarius_response()
    
    # Summary
    print("\n" + "#" * 60)
    print("#  SUMMARY")
    print("#" * 60)
    
    passed = 0
    failed = 0
    skipped = 0
    
    for name, result in results:
        if result is True:
            status = "✓ PASSED"
            passed += 1
        elif result is False:
            status = "✗ FAILED"
            failed += 1
        else:
            status = "- SKIPPED"
            skipped += 1
        print(f"  {name}: {status}")
    
    print()
    print(f"  Passed:  {passed}")
    print(f"  Failed:  {failed}")
    print(f"  Skipped: {skipped}")
    print()
    
    if failed > 0:
        print("  ❌ SOME TESTS FAILED")
        return False
    else:
        print("  ✅ ALL TESTS PASSED!")
        return True


if __name__ == "__main__":
    success = main()
    
    # Exit with error code if tests failed
    import sys
    sys.exit(0 if success else 1)