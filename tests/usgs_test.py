# tests/test_usgs.py
"""
Tests for the USGS data source.

Run with:
    python tests/test_usgs.py
"""

import sys
from pathlib import Path

# Add project root to Python's path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from datetime import datetime, timedelta
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)


def test_imports():
    """Test that USGS imports work."""

    print("\n" + "=" * 60)
    print("TEST: Imports")
    print("=" * 60)

    try:
        from data_sources import USGSDataSource
        print("  ✓ Can import USGSDataSource")
    except ImportError as e:
        print(f"  ✗ Failed: {e}")
        return False

    from data_sources import BaseDataSource
    if issubclass(USGSDataSource, BaseDataSource):
        print("  ✓ USGSDataSource inherits from BaseDataSource")
    else:
        print("  ✗ USGSDataSource does NOT inherit from BaseDataSource")
        return False

    return True


def test_create_source():
    """Test creating a USGS data source."""

    print("\n" + "=" * 60)
    print("TEST: Create USGSDataSource")
    print("=" * 60)

    try:
        from data_sources import USGSDataSource
        usgs = USGSDataSource()
        print("  ✓ Created USGSDataSource (no auth needed!)")
    except Exception as e:
        print(f"  ✗ Failed: {e}")
        return False

    if usgs.source_name == "USGS":
        print("  ✓ source_name is 'USGS'")
    else:
        print(f"  ✗ source_name is wrong: {usgs.source_name}")
        return False

    if "waterservices.usgs.gov" in usgs.base_url:
        print("  ✓ base_url is correct")
    else:
        print(f"  ✗ base_url is wrong: {usgs.base_url}")
        return False

    return True


def test_get_sites():
    """Test getting sites from USGS."""

    print("\n" + "=" * 60)
    print("TEST: get_sites()")
    print("=" * 60)

    from data_sources import USGSDataSource
    usgs = USGSDataSource()

    # Test with state
    try:
        sites = usgs.get_sites(state='OR', site_type='ST')
        print(f"  ✓ Found {len(sites)} stream sites in Oregon")
    except Exception as e:
        print(f"  ✗ get_sites() failed: {e}")
        return False

    if not sites:
        print("  ✗ No sites returned")
        return False

    # Check first site
    first = sites[0]
    print(f"  First site: {first.site_id} - {first.name}")

    if first.site_id:
        print("  ✓ Site has site_id")
    else:
        print("  ✗ Site missing site_id")
        return False

    if first.source == "USGS":
        print("  ✓ Site source is 'USGS'")
    else:
        print(f"  ✗ Site source is wrong: {first.source}")
        return False

    return True


def test_get_sites_by_id():
    """Test getting specific sites by ID."""

    print("\n" + "=" * 60)
    print("TEST: get_sites() by ID")
    print("=" * 60)

    from data_sources import USGSDataSource
    usgs = USGSDataSource()

    test_site = '14211720'  # Columbia River at Vancouver

    try:
        sites = usgs.get_sites(site_ids=[test_site])
        print(f"  ✓ Found {len(sites)} sites")
    except Exception as e:
        print(f"  ✗ Failed: {e}")
        return False

    if not sites:
        print("  ✗ No sites returned")
        return False

    if sites[0].site_id == test_site:
        print(f"  ✓ Got correct site: {sites[0].name}")
    else:
        print(f"  ✗ Wrong site returned: {sites[0].site_id}")
        return False

    return True


def test_get_parameters():
    """Test getting available parameters."""

    print("\n" + "=" * 60)
    print("TEST: get_available_parameters()")
    print("=" * 60)

    from data_sources import USGSDataSource
    usgs = USGSDataSource()

    test_site = '14211720'

    try:
        params = usgs.get_available_parameters(test_site)
        print(f"  ✓ Found {len(params)} parameters")
    except Exception as e:
        print(f"  ✗ Failed: {e}")
        return False

    if params:
        for p in params:
            print(f"    - {p['code']}: {p['name']} ({p['unit']})")

    return True


def test_get_data():
    """Test getting actual data."""

    print("\n" + "=" * 60)
    print("TEST: get_data()")
    print("=" * 60)

    from data_sources import USGSDataSource
    usgs = USGSDataSource()

    test_site = '14211720'
    end_date = datetime.now()
    start_date = end_date - timedelta(days=3)

    try:
        data = usgs.get_data(
            site_ids=[test_site],
            parameter='00060',
            start_date=start_date,
            end_date=end_date
        )
        print(f"  ✓ Retrieved {len(data)} data points")
    except Exception as e:
        print(f"  ✗ Failed: {e}")
        return False

    if not data:
        print("  ✗ No data returned")
        return False

    first = data[0]
    print(f"  First point: {first.timestamp} = {first.value} {first.unit}")
    print(f"  Last point: {data[-1].timestamp} = {data[-1].value} {data[-1].unit}")

    if first.source == "USGS":
        print("  ✓ Data source is 'USGS'")
    else:
        print(f"  ✗ Source is wrong: {first.source}")
        return False

    return True


def test_to_dataframe():
    """Test converting data to DataFrame."""

    print("\n" + "=" * 60)
    print("TEST: to_dataframe()")
    print("=" * 60)

    from data_sources import USGSDataSource
    usgs = USGSDataSource()

    end_date = datetime.now()
    start_date = end_date - timedelta(days=3)

    data = usgs.get_data(
        site_ids=['14211720'],
        parameter='00060',
        start_date=start_date,
        end_date=end_date
    )

    try:
        df = usgs.to_dataframe(data)
        print(f"  ✓ Created DataFrame with shape {df.shape}")
    except Exception as e:
        print(f"  ✗ Failed: {e}")
        return False

    print(f"  Columns: {list(df.columns)}")
    print(f"\n  First 3 rows:")
    print(df.head(3).to_string(index=False))

    # Check column names match our standard
    expected_columns = ['timestamp', 'value', 'location', 'parameter',
                        'unit', 'source', 'quality_flag']
    for col in expected_columns:
        if col in df.columns:
            print(f"  ✓ Has column '{col}'")
        else:
            print(f"  ✗ Missing column '{col}'")
            return False

    return True


def test_convenience_methods():
    """Test convenience methods."""

    print("\n" + "=" * 60)
    print("TEST: Convenience Methods")
    print("=" * 60)

    from data_sources import USGSDataSource
    usgs = USGSDataSource()

    end_date = datetime.now()
    start_date = end_date - timedelta(days=2)

    # Test get_discharge
    try:
        data = usgs.get_discharge(['14211720'], start_date, end_date)
        print(f"  ✓ get_discharge() returned {len(data)} points")
    except Exception as e:
        print(f"  ✗ get_discharge() failed: {e}")
        return False

    # Test get_recent
    try:
        data = usgs.get_recent(['14211720'], '00060', days=2)
        print(f"  ✓ get_recent() returned {len(data)} points")
    except Exception as e:
        print(f"  ✗ get_recent() failed: {e}")
        return False

    return True


def test_search_near():
    """Test searching for sites near a location."""

    print("\n" + "=" * 60)
    print("TEST: search_sites_near()")
    print("=" * 60)

    from data_sources import USGSDataSource
    usgs = USGSDataSource()

    # Search near Portland, OR
    try:
        sites = usgs.search_sites_near(
            latitude=45.52,
            longitude=-122.68,
            radius_miles=15
        )
        print(f"  ✓ Found {len(sites)} sites within 15 miles of Portland")
    except Exception as e:
        print(f"  ✗ Failed: {e}")
        return False

    if sites:
        print(f"  First 3 nearby sites:")
        for site in sites[:3]:
            print(f"    - {site.site_id}: {site.name}")

    return True


def main():
    """Run all tests."""

    print("\n" + "#" * 60)
    print("#  USGS DATA SOURCE TESTS")
    print("#" * 60)

    results = []

    results.append(("Imports", test_imports()))
    results.append(("Create Source", test_create_source()))
    results.append(("get_sites()", test_get_sites()))
    results.append(("get_sites() by ID", test_get_sites_by_id()))
    results.append(("get_available_parameters()", test_get_parameters()))
    results.append(("get_data()", test_get_data()))
    results.append(("to_dataframe()", test_to_dataframe()))
    results.append(("Convenience Methods", test_convenience_methods()))
    results.append(("search_sites_near()", test_search_near()))

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

    import sys
    sys.exit(0 if success else 1)