# data_sources/aquarius.py
"""
Aquarius WebPortal Data Source

This module provides access to the Aquarius WebPortal APIs.

This is adapter code to work with the existing aquarius_data_downloader script.

Key field mappings discovered from API:
    Locations:
        'id'                 → site identifier (e.g., "VNB")
        'locationId'         → numeric database ID
        'name'               → display name
        'latitude/longitude' → coordinates
        'type'               → e.g., "Surface Water"
        'active'             → whether currently active
    
    Datasets:
        'identifier'         → dataset identifier
        'locationIdentifier' → which location this belongs to
        'parameter'          → what is measured
        'label'              → human readable label
        'startOfRecord'      → when data begins
        'endOfRecord'        → when data ends
"""

import requests
from datetime import datetime
from typing import List, Optional, Dict, Any
import logging

# Import Base classes from same package
from .base import BaseDataSource, DataPoint, SiteInfo

logger = logging.getLogger(__name__)


class AquariusDataSource(BaseDataSource):
    """
    Aquarius WebPortal API client.
    
    This class implements the BaseDataSource interface for Aquarius,
    allowing you to use Aquarius data alongside other data sources
    like USGS.
    
    SSL VERIFICATION:
    
        Many organizations run Aquarius on internal servers with
        self-signed SSL certificates. In these cases, you may need
        to set verify_ssl=False to connect. This is less secure but
        often necessary for internal servers.
        
    Example:
        aquarius = AquariusDataSource(
            base_url="https://aquarius.portlandoregon.gov/api/v1",
            username="analyst",
            password="secret123",
            verify_ssl=False
        )
        
        # Now use:
        sites = aquarius.get_sites()
        data = aquarius.get_data(...)
    """
    
    def __init__(
        self,
        base_url: str,
        username: str,
        password: str,
        verify_ssl: bool = True
    ):
        """
        Initialize the Aquarius data source.
        
        Args:
            base_url: Full URL to the Aquarius API endpoint
            username: Your Aquarius username
            password: Your Aquarius password
            verify_ssl: Whether to verify SSL certificates
        """
        
        # Call parent's __init__
        # Sets up: self.logger, self._cache, self._cache_enabled
        super().__init__()
        
        # Store configuration
        # Use _base_url because property base_url returns it
        self._base_url = base_url
        self.username = username
        self.verify_ssl = verify_ssl
        
        # Create HTTP session with authentication
        self.session = requests.Session()
        
        # Set up Basic Authentication
        # Adds "Authorization" header to every request
        self.session.auth = (username, password)
        
        # Handle SSL verification
        if not verify_ssl:
            import urllib3
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
            self.logger.warning("SSL certification verification is disabled")
        
        # Test the connection
        self._test_connection()
        
        self.logger.info(f"Aquarius data source initialized for {self._base_url}")
    
    def _test_connection(self) -> None:
        """
        Test the connection to the Aquarius server.
        
        Called during initialization to verify:
            - The server is reachable
            - The credentials are correct
            - The API is responding
            
        Uses /version endpoint because it is lightweight
        and does not require any parameters.
        
        Raises:
            Exception: If connection fails
        """
        
        try:
            url = f"{self._base_url}/version"
            
            self.logger.debug(f"Testing connection to {url}")
            
            response = self.session.get(
                url,
                verify=self.verify_ssl,
                timeout=10
            )
            
            if response.status_code == 200:
                try:
                    version_info = response.json()
                    version = version_info.get('webPortalVersion', 'unknown')
                    self.logger.info(f"Connected to Aquarius (version {version})")
                except ValueError:
                    self.logger.info("Connected to Aquarius")
                    
            elif response.status_code == 401:
                self.logger.error("Authentication failed, check username/password")
                raise Exception("Aquarius authentication failed")
            
            elif response.status_code == 403:
                self.logger.error("Access forbidden, check account permissions")
                raise Exception("Aquarius access forbidden")
            
            else:
                self.logger.error(
                    f"Connection failed with status {response.status_code}"
                )
                raise Exception(
                    f"Aquarius connection failed: HTTP {response.status_code}"
                )
        
        except requests.exceptions.Timeout:
            self.logger.error("Connection timed out, server may be unreachable")
            raise Exception("Aquarius connection timed out")
        
        except requests.exceptions.ConnectionError as e:
            self.logger.error(f"Could not connect to server: {e}")
            raise Exception(f"Could not connect to Aquarius: {e}")
        
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Connection error: {e}")
            raise Exception(f"Aquarius connection error: {e}")
    
    # -------------------------------------------------------------------------
    # REQUIRED PROPERTIES (from BaseDataSource)
    # -------------------------------------------------------------------------
    
    @property
    def source_name(self) -> str:
        """
        Return the name of this data source.
        
        This identifies the source in DataPoint objects and logs.
        When you combine data from multiple sources, this tells you
        which data came from Aquarius.

        Returns:
            "Aquarius"
        """
        return "Aquarius"
    
    @property
    def base_url(self) -> str:
        """
        Return the base URL for this Aquarius instance.
        
        Each Aquarius instance has a unique URL which is why
        it is stored in __init__.
        """
        return self._base_url
    
    # -------------------------------------------------------------------------
    # HTTP HELPER METHODS - These handle the low-level API communication
    # -------------------------------------------------------------------------
    
    def _get(self, endpoint: str, params: Optional[Dict] = None) -> Optional[Dict]:
        """
        Make a GET request to the Aquarius API.
        
        Args:
            endpoint: API endpoint (e.g., "locations", "data-set")
            params: Optional query parameters
            
        Returns:
            Parsed JSON response as a dictionary, or None if failed
        """
        
        url = f"{self._base_url}/{endpoint}"
        
        try:
            self.logger.debug(f"GET {url}")
            if params:
                self.logger.debug(f"  Params: {params}")
            
            response = self.session.get(
                url,
                params=params,
                verify=self.verify_ssl,
                timeout=30
            )
            
            if response.status_code == 200:
                return response.json()
            else:
                self.logger.error(
                    f"GET {endpoint} failed: HTTP {response.status_code}"
                )
                self.logger.debug(f"Response: {response.text[:500]}")
                return None
            
        except requests.exceptions.Timeout:
            self.logger.error(f"GET {endpoint} timed out")
            return None
        
        except requests.exceptions.RequestException as e:
            self.logger.error(f"GET {endpoint} failed: {e}")
            return None
        
        except ValueError as e:
            self.logger.error(f"Failed to parse response as JSON: {e}")
            return None
    
    def _post(self, endpoint: str, data: Dict) -> Optional[Dict]:
        """
        Make a POST request to the Aquarius API.
        
        POST is used when sending data to the server.
        The Aquarius bulk export endpoint uses POST because we send
        a complex request body with datasets, date ranges, etc.
        
        Args:
            endpoint: API endpoint (e.g., "export/bulk")
            data: Request body as a dictionary, sent as JSON
            
        Returns:
            Parsed JSON response as a dictionary, or None if failed
            
        Example:
            result = self._post("export/bulk", {
                "Datasets": [{"identifier": "Precip@HYDRA-1"}],
                "StartTime": "2024-01-01",
                "EndTime": "2024-01-31"
            })
        """
        
        url = f"{self._base_url}/{endpoint}"
        
        # Headers tell the server what is being sent and what to expect back
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json"
        }
        
        try:
            self.logger.debug(f"POST {url}")
            self.logger.debug(f"  Body: {str(data)[:200]}...")
            
            response = self.session.post(
                url,
                json=data,
                headers=headers,
                verify=self.verify_ssl,
                timeout=240
            )
            
            if response.status_code == 200:
                return response.json()
            else:
                self.logger.error(
                    f"POST {endpoint} failed: HTTP {response.status_code}"
                )
                self.logger.debug(f"Response: {response.text[:500]}")
                return None
            
        except requests.exceptions.Timeout:
            self.logger.error(f"POST {endpoint} timed out")
            return None
        
        except requests.exceptions.RequestException as e:
            self.logger.error(f"POST {endpoint} failed: {e}")
            return None
        
        except ValueError as e:
            self.logger.error(f"Failed to parse response as JSON: {e}")
            return None
    
    # -------------------------------------------------------------------------
    # REQUIRED METHODS (from BaseDataSource)
    # -------------------------------------------------------------------------
    
    def get_sites(
        self,
        state: Optional[str] = None,
        bbox: Optional[tuple] = None,
        site_ids: Optional[List[str]] = None,
        **kwargs
    ) -> List[SiteInfo]:
        """
        Get locations from Aquarius.
        
        Called 'locations' not 'sites' in Aquarius terminology.
        
        Field mapping (from API debug):
            'id'          → site_id  (e.g., "VNB")
            'locationId'  → numeric database ID (stored in metadata)
            'name'        → name
            'latitude'    → latitude
            'longitude'   → longitude
            'type'        → location type (stored in metadata)
            'active'      → whether active (stored in metadata)
        
        Args:
            state: Not used for Aquarius (included for interface compatibility)
            bbox: Not used for Aquarius (included for interface compatibility)
            site_ids: If provided, filters to only these locations
            **kwargs: Additional parameters
            
        Returns:
            List of SiteInfo objects representing Aquarius locations
            
        Example:
            # Get all locations
            sites = aquarius.get_sites()
            
            # Get specific locations
            sites = aquarius.get_sites(site_ids=['VNB', 'HYDRA-1'])
            
        Note:
            Unlike USGS which requires a location filter, Aquarius can return
            all locations at once since it has far fewer sites.
        """
        
        self.logger.info("Fetching Aquarius locations...")
        
        response = self._get("locations")
        
        if not response:
            self.logger.error("Failed to get locations from Aquarius")
            return []
        
        locations = response.get("locations", [])
        
        self.logger.debug(f"  Raw response contains {len(locations)} locations")
        
        sites = []
        
        for loc in locations:
            
            # 'id' is the short identifier confirmed from API response
            # Example: 'id': 'VNB'
            loc_id = loc.get("id", "")
            
            if site_ids is not None:
                if loc_id not in site_ids:
                    continue
            
            site = SiteInfo(
                site_id=loc_id,
                name=loc.get("name", loc_id),
                latitude=loc.get("latitude"),
                longitude=loc.get("longitude"),
                source=self.source_name,
                metadata={
                    "locationId": loc.get("locationId"),
                    "type": loc.get("type"),
                    "folder": loc.get("folder"),
                    "active": loc.get("active"),
                    "utcOffset": loc.get("utcOffset"),
                    "description": loc.get("description"),
                }
            )
            
            sites.append(site)
        
        self.logger.info(f"  Found {len(sites)} locations")
        
        if sites and self.logger.isEnabledFor(logging.DEBUG):
            self.logger.debug("  First few locations:")
            for site in sites[:3]:
                self.logger.debug(f"    - {site.site_id}: {site.name}")
        
        return sites
    
    def get_available_parameters(self, site_id: str) -> List[Dict[str, str]]:
        """
        Get list of available parameters at an Aquarius location.
        
        Finds all datasets that belong to a specific location.
        
        Field mapping (from API debug):
            'locationIdentifier' → which location (e.g., "VNB")
            'parameter'          → parameter name
            'label'              → human readable label
        
        Args:
            site_id: Location identifier (e.g., "VNB")
            
        Returns:
            List of dicts with 'code', 'name', 'unit' keys
            
        Example:
            params = aquarius.get_available_parameters("VNB")
            # Returns:
            # [
            #     {'code': 'Precipitation', 'name': 'Precipitation', 'unit': 'mm'},
            # ]
        """
        
        self.logger.info(f"Getting available parameters for {site_id}")
        
        response = self._get("data-set")
        
        if not response:
            self.logger.error("Failed to get datasets from Aquarius")
            return []
        
        datasets = response.get("datasets", [])
        
        self.logger.debug(f"  Total datasets in system: {len(datasets)}")
        
        parameters = []
        seen_params = set()
        
        for ds in datasets:
            
            # 'locationIdentifier' is the direct location ID (e.g., "VNB")
            # confirmed from API debug - no need to split on "@"
            dataset_location = ds.get("locationIdentifier", "")
            
            if dataset_location.lower() != site_id.lower():
                continue
            
            param = ds.get("parameter", "")
            unit = ds.get("unit", "")
            label = ds.get("label", param)
            
            if param and param not in seen_params:
                seen_params.add(param)
                parameters.append({
                    'code': param,
                    'name': label,
                    'unit': unit
                })
        
        self.logger.info(f"  Found {len(parameters)} parameters at {site_id}")
        
        for p in parameters:
            self.logger.debug(f"    - {p['code']} ({p['unit']})")
        
        return parameters
    
    def get_data(
        self,
        site_ids: List[str],
        parameter: str,
        start_date: datetime,
        end_date: datetime,
        **kwargs
    ) -> List[DataPoint]:
        """
        Get time series data from Aquarius.
        
        Finds all datasets matching the sites and parameter,
        exports data for each using the bulk export API, and
        converts results to standard DataPoint format.
        
        Args:
            site_ids: List of location identifiers
            parameter: Parameter name (e.g., 'Precipitation')
            start_date: Start of data period
            end_date: End of data period
            **kwargs: Additional parameters
            
        Returns:
            List of DataPoint objects
            
        Example:
            data = aquarius.get_data(
                site_ids=['VNB', 'HYDRA-1'],
                parameter='Precipitation',
                start_date=datetime(2020, 1, 1),
                end_date=datetime(2024, 1, 1)
            )
            
            print(f"Got {len(data)} data points")
            
            df = aquarius.to_dataframe(data)
        """
        
        self.logger.info("Fetching Aquarius data...")
        self.logger.info(f"  Sites: {', '.join(site_ids)}")
        self.logger.info(f"  Parameter: {parameter}")
        self.logger.info(f"  Date range: {start_date.date()} to {end_date.date()}")
        
        dataset_identifiers = self._find_dataset_identifiers(
            site_ids=site_ids,
            parameter=parameter,
            start_date=start_date,
            end_date=end_date
        )
        
        if not dataset_identifiers:
            self.logger.warning("No matching datasets found")
            return []
        
        self.logger.info(f"  Found {len(dataset_identifiers)} matching datasets")
        
        all_data_points = []
        
        for i, identifier in enumerate(dataset_identifiers, 1):
            self.logger.info(
                f"  [{i}/{len(dataset_identifiers)}] Exporting {identifier}"
            )
            
            points = self._export_dataset(
                identifier=identifier,
                start_time=start_date,
                end_time=end_date
            )
            
            all_data_points.extend(points)
            self.logger.debug(f"    Got {len(points)} points")
        
        self.logger.info(f"  Total: {len(all_data_points):,} data points")
        
        return all_data_points
    
    def _find_dataset_identifiers(
        self,
        site_ids: List[str],
        parameter: str,
        start_date: datetime,
        end_date: datetime
    ) -> List[str]:
        """
        Find dataset identifiers matching the given criteria.
        
        Searches all datasets and returns identifiers that:
        1. Match one of the requested site IDs
        2. Match the requested parameter (case-insensitive)
        3. Have data within the requested date range
        
        Args:
            site_ids: List of location identifiers to match
            parameter: Parameter name to match
            start_date: Start of requested date range
            end_date: End of requested date range
            
        Returns:
            List of dataset identifier strings
        """
        
        self.logger.debug("Finding matching datasets...")
        
        response = self._get("data-set")
        if not response:
            return []
        
        datasets = response.get("datasets", [])
        
        self.logger.debug(f"  Total datasets available: {len(datasets)}")
        
        start_str = start_date.strftime('%Y-%m-%d')
        end_str = end_date.strftime('%Y-%m-%d')
        
        matching = []
        
        for ds in datasets:
            identifier = ds.get("identifier", "")
            ds_param = ds.get("parameter", "")
            ds_start = (ds.get("startOfRecord") or "")[:10]
            ds_end = (ds.get("endOfRecord") or "")[:10]
            
            # 'locationIdentifier' is the direct location ID
            # confirmed from API debug output
            dataset_location = ds.get("locationIdentifier", "")
            
            # Does the parameter match?
            if parameter.lower() not in ds_param.lower():
                continue
            
            # Does the site match?
            site_match = any(
                site_id.lower() == dataset_location.lower()
                for site_id in site_ids
            )
            if not site_match:
                continue
            
            # Does the date range overlap?
            if ds_end and ds_end < start_str:
                continue
            if ds_start and ds_start > end_str:
                continue
            
            matching.append(identifier)
            self.logger.debug(f"  Matched: {identifier}")
        
        return matching
    
    def _export_dataset(
        self,
        identifier: str,
        start_time: datetime,
        end_time: datetime
    ) -> List[DataPoint]:
        """
        Export time series data for a single Aquarius dataset.
        
        Uses the Aquarius bulk export API to retrieve data points.
        
        Args:
            identifier: Dataset identifier
            start_time: Start of export period
            end_time: End of export period
            
        Returns:
            List of DataPoint objects
        """
        
        body = {
            "Datasets": [
                {"identifier": identifier}
            ],
            "StartTime": start_time.isoformat(),
            "EndTime": end_time.isoformat(),
            "Interval": "PointsAsRecorded",
            "RoundData": False,
            "IncludeGradeCodes": False,
            "IncludeQualifiers": False,
            "IncludeApprovalLevels": False,
            "IncludeInterpolationTypes": False
        }
        
        result = self._post("export/bulk", body)
        
        if not result:
            self.logger.warning(f"No data returned for {identifier}")
            return []
        
        return self._parse_export_response(result)
    
    def _parse_export_response(self, result: Dict) -> List[DataPoint]:
        """
        Parse the Aquarius bulk export response into DataPoint objects.
        
        Response structure:
        {
            "series": [
                {
                    "dataset": {
                        "locationIdentifier": "VNB",
                        "parameter": "Precipitation",
                        "unit": "mm"
                    },
                    "numPoints": 1000,
                    "points": [
                        {"timestamp": "2024-01-15T12:00:00Z", "value": 0.05},
                        ...
                    ]
                }
            ]
        }
        
        Args:
            result: Parsed JSON response from export API
            
        Returns:
            List of DataPoint objects
        """
        
        data_points = []
        
        series_list = result.get("series", [])
        
        for series in series_list:
            dataset_info = series.get("dataset", {})
            
            location = dataset_info.get("locationIdentifier", "unknown")
            parameter = dataset_info.get("parameter", "unknown")
            unit = dataset_info.get("unit", "unknown")
            
            points = series.get("points", [])
            
            num_points = series.get("numPoints", len(points))
            self.logger.debug(f"    Processing {num_points} points for {location}")
            
            for point in points:
                try:
                    ts_str = point.get("timestamp", "")
                    if not ts_str:
                        continue
                    
                    timestamp = self._parse_timestamp(ts_str)
                    
                    if timestamp is None:
                        continue
                    
                    value = point.get("value")
                    
                    if value is None:
                        continue
                    
                    value = float(value)
                    
                    data_points.append(DataPoint(
                        timestamp=timestamp,
                        value=value,
                        location_id=location,
                        parameter=parameter,
                        unit=unit,
                        source=self.source_name
                    ))
                    
                except (ValueError, TypeError) as e:
                    self.logger.debug(f"    Skipping invalid point: {e}")
                    continue
        
        return data_points
    
    def _parse_timestamp(self, ts_str: str) -> Optional[datetime]:
        """
        Parse an Aquarius timestamp string into a datetime object.
        
        Handles all Aquarius timestamp format variations:
            "2024-01-15T12:00:00Z"
            "2024-01-15T12:00:00.000Z"
            "2024-01-15T12:00:00-08:00"
            "2024-01-15T12:00:00.000-08:00"
        
        Args:
            ts_str: Timestamp string from Aquarius
            
        Returns:
            datetime object, or None if parsing fails
        """
        
        if not ts_str:
            return None
        
        try:
            # Python's fromisoformat doesn't like 'Z', wants '+00:00'
            if ts_str.endswith('Z'):
                ts_str = ts_str[:-1] + '+00:00'
            
            # Strip milliseconds but keep timezone
            if '.' in ts_str:
                before_decimal = ts_str.split('.')[0]
                after_decimal = ts_str.split('.')[1]
                
                tz_part = ""
                for i, char in enumerate(after_decimal):
                    if char in ['+', '-']:
                        tz_part = after_decimal[i:]
                        break
                
                ts_str = before_decimal + tz_part
            
            return datetime.fromisoformat(ts_str)
        
        except ValueError as e:
            self.logger.debug(f"Failed to parse timestamp '{ts_str}': {e}")
            return None
    
    # -------------------------------------------------------------------------
    # CONVENIENCE METHODS - Make the API easier to use
    # -------------------------------------------------------------------------
    
    def get_precipitation(
        self,
        site_ids: List[str],
        start_date: datetime,
        end_date: datetime
    ) -> List[DataPoint]:
        """
        Get precipitation data from specified sites.
        
        Convenience method that calls get_data() with
        parameter='Precipitation'. Saves typing for common use case.
        
        Args:
            site_ids: List of location identifiers
            start_date: Start of data period
            end_date: End of data period
            
        Returns:
            List of DataPoint objects
        """
        return self.get_data(
            site_ids=site_ids,
            parameter='Precipitation',
            start_date=start_date,
            end_date=end_date
        )
    
    def get_datasets(self) -> List[Dict]:
        """
        Get all available datasets from Aquarius.
        
        Returns raw dataset info, useful for exploring
        what data is available.
        
        Returns:
            List of dataset dictionaries with fields like:
            - identifier
            - locationIdentifier
            - parameter
            - label
            - startOfRecord
            - endOfRecord
            - active
            
        Example:
            datasets = aquarius.get_datasets()
            for ds in datasets[:5]:
                print(f"{ds['identifier']}: {ds['parameter']}")
        """
        
        self.logger.info("Fetching all datasets...")
        
        response = self._get("data-set")
        
        if not response:
            self.logger.error("No response from data-set endpoint")
            return []
        
        self.logger.debug(f"Response keys: {list(response.keys())}")
        
        datasets = response.get("datasets", [])
        
        self.logger.info(f"Found {len(datasets)} datasets")
        
        return datasets
    
    def get_alerts(self) -> List[Dict]:
        """
        Get active alerts from Aquarius.
        
        Alerts indicate unusual conditions like high water levels
        or equipment malfunctions.
        
        Returns:
            List of alert dictionaries
        """
        response = self._get("alerts")
        return response if response else []
    
    def find_datasets(
        self,
        must_contain_all: Optional[List[str]] = None,
        must_contain_any: Optional[List[str]] = None
    ) -> List[Dict]:
        """
        Find datasets matching search criteria.
        
        Searches dataset identifiers for matching text.
        Similar to find_datasets() from the original downloader script.
        
        Args:
            must_contain_all: Strings that must ALL appear
                              in the identifier (case-insensitive)
            
            must_contain_any: Strings where AT LEAST ONE must
                              match the location identifier exactly
            
        Returns:
            List of matching dataset dictionaries
            
        Example:
            # Find all precipitation datasets
            datasets = aquarius.find_datasets(
                must_contain_all=['Precipitation']
            )
            
            # Find datasets for specific stations
            datasets = aquarius.find_datasets(
                must_contain_any=['VNB', 'HYDRA-1', 'HYDRA-2']
            )
            
            # Combine filters
            datasets = aquarius.find_datasets(
                must_contain_all=['Precipitation'],
                must_contain_any=['VNB', 'HYDRA-1']
            )
        """
        
        all_datasets = self.get_datasets()
        matching = []
        
        for ds in all_datasets:
            identifier = ds.get("identifier", "")
            location = ds.get("locationIdentifier", "")
            
            matches = True
            
            # Check must_contain_any (exact location match)
            if must_contain_any:
                any_match = any(
                    text.lower() == location.lower()
                    for text in must_contain_any
                )
                if not any_match:
                    matches = False
            
            # Check must_contain_all (substring match in identifier)
            if must_contain_all and matches:
                for text in must_contain_all:
                    if text.lower() not in identifier.lower():
                        matches = False
                        break
            
            if matches:
                matching.append(ds)
        
        self.logger.info(f"Found {len(matching)} matching datasets")
        return matching
