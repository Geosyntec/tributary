"""
Aquarius WebPortal Data Source

This module provides acces to the Aquarius WebPortal APIS

This is adapter code to work with the existing aquarius_data_downloader script
"""

import requests
from datetime import datetime
from typing import List, Optional, Dict, Any
import logging

#Import Base classes from same package
from .base import BaseDataSource, DataPoint, SiteInfo # .base means "from this pacakge" (relative import)

logger = logging.getLogger(__name__)

class AquariusDatazSource(BaseDataSource):
    """
    Aquarius WebPortal API client
    
    This class implements the BaseDataSource interface for Aquarius,
    allowing you to use Aquarius data alongside other data sources 
    like USGS
    
    SSL VERIFICATION:
    
        Many organizations run Aquarius on internal servers with
        self-signed SSL certificates. In these cases, you may need
        to set verify_ssl=False to connect. This is less secure but
        often necessary for internal servers.
        
    Example:
        aquarius = AquariusDataSource(
            base_url="https://aquarius.myorg.com/AQUARIUS/Publish/v2",
            username="analyst",
            password="secret123",
            verify_ssl=False
        )
        
        # Now use:
        sites = aquarius.get_sites()
        data = aquarius.get_data(...)"""
    
    def __init__(
            self,
            base_url: str,
            username: str,
            password: str,
            verify_ssl: bool = True
    ):
        # Call Parent's __init__
        # This sets up self.logger, self._cache, self._cache_enabled
        super().__init__()

        # Store configuration
        # Use _base_url because property base_url returns it
        self._base_url = base_url
        self.username = username
        self.verify_ssl = verify_ssl
        
        # Create HTTP session with authentication
        self.session = requests.Session()

        # Set up Basic Authentication
        # This adds an "Authorization" header to every request
        self.session.auth = (username, password)

        # Handle SSL verification
        if not verify_ssl:
            # Disable SSL warnings when verification is off
            import urllib3
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
            self.logger.warning("SSL certification verification is disabled")

        # Test the connection
        self._test_connection()

        self.logger.info(f"Aquarius data source initialized for {self._base_url}")

    def _test_connection(self) -> None:
        """
        Test the connection to the Aquarius server.
        
        This is caleld during initialization to verify: 
            - The server is reachable
            - The credentials are correct
            - The API is responding
            
        /version is used because it is lightweight and does not require any parameters
        
        Raises:
            Exceptions: If connection fails
        """
        
        try:
            # Get version info
            url = f"{self._base_url}/version"

            self.logger.debug(f"Testing connection to {url}")

            response = self.session.get(
                url,
                verify=self.verify_ssl,
                timeout=10  # 10 second timeout for connection test
            )

            # Check response
            if response.status_code == 200:
                # Success
                try:
                    version_info = response.json()
                    version = version_info.get('webPortalVersion', 'unknown')
                    self.logger.info(f"Connected to Aquarius (version {version})")
                except ValueError:
                    # Resopnse was not JSON, but connection worked
                    self.logger.info("Connected to Aquarius")

            elif response.status_code == 401:
                # Unauthorized (bad creds)
                self.logger.error("Authentication failed, check username/password")
                raise Exception("Aquarius authentication failed")
            
            elif response.status_code == 403:
                # 403 = Forbidden (credentials ok but no permission)
                self.logger.error("Access forbidden, check account permissions")
                raise Exception("Aquarius access forbidden")
            
            else:
                # Something else
                self.logger.error(f"Connection failed with status {response.status_code}")
                raise Exception(f"Aquarius connection failed: HTTP {response.status_code}")
        
        except requests.exceptions.Timeout:
            self.logger.error("Connectoin timed out, server may be unreachable")
            raise Exception("Aquarius connection timed out")
        
        except requests.exceptions.ConnectionError as e:
            self.logger.error(f"Could not connect to server: {e}")
            raise Exception(f"Could not connect to Aquarius: {e}")
        
        except requests.exceptions.RequestsException as e:
            self.logger.error(f"Connection error: {e}")
            raise Exception(f"Aquarius connection error: {e}")
        
    
    # ---------------------------------------------------------
    # REQUIRED PROPERTIES (from BaseDataSource)
    # ---------------------------------------------------------

    @property
    def source_name(self) -> str:
        """
        Return the name of this data source.
        
        This identifies the source in DataPoint objects and logs.
        When you combine data from multiple sources, this tells you
        which data came from Aquarius

        Returns:
            "Aquarius
        """
        return "Aquarius"
    
    @property
    def base_url(self) -> str:
        """ 
        Returns the base URL for this Aquarius instance
        Each Aquarius instance has a unique URL which is why its stored in __init__
        """
        return self._base_url
    
    # ---------------------------------------------------------
    # HTTP HELPER METHODS - These handle the low-level API communication
    # ---------------------------------------------------------

    def _get(self, endpoint: str, params: Optional[Dict] = None) -> Optional[Dict]:
        """
        Make a GET request to the Aquarius API
        """

        # Build the full URL
        url = f"{self._base_url}/{endpoint}"

        try:
            self.logger.debug(f"GET {url}")
            if params:
                self.logger.debut(f"  Params: {params}")

            # Make the request
            # - verify: whether to check SSL
            # - timeout: seconds to wait for response
            # - params: query parameters (added to URL as ?key=value&...)
            response = self.session.get(
                url,
                params=params,
                verify=self.verify_ssl,
                timeout=30
            )

            # Check for success
            if response.status_code == 200:
                return response.json()
            else:
                self.logger.error(f"GET {endpoint} failed: HTTP {response.status_code}")
                self.logger.debug(f"Response: {response.text[:500]}")
                return None
            
        except requests.exceptions.Timeout:
            self.logger.error(f"GET {endpoint} timed out")
            return None
        
        except requests.exceptions.RequestException as e:
            self.logger.error(f"GET {endpoint} failed: {e}")
            return None
        
        except ValueError as e:
            # JSON parsing failed
            self.logger.error(f"Failed to parse response as JSON: {e}")
            return None
    
    def _post(self, endpoint: str, data: Dict) -> Optional[Dict]:
        """
        Make a POST request to the Aquarius API
        
        Arguments:
            endpoint: API endpoint

            data: the request body as a dictionary in JSON.

        Returns:
            Parsed JSON response as a dictionary, or None if fail
        
        Example:
            result = self._post("export/bulk", {
                "Datasets": [{"identifier": "Precip@HYDRA-1"}],
                "StartTime": "2024-01-01",
                "EndTime": "2024-01-31"
            })
        """

        url = f"({self._base_url}/{endpoint})"

        # Headers tell the server what is being sent and what to expect back
        headers = {
            "Accept": "application/json",       #Asking for json response
            "Content-Type": "application/json"  # We're sending JSON data
        }

        try:
            self.logger.debug(f"POST {url}")
            self.logger.debug(f"  Body: {str(data)[:200]}...")

            # Making the request
            # - json=data: Automatically coverts dict to JSON string
            # - headers: Additional HTTP headers
            response = self.session.post(
                url,
                json=data,      # Converts dict to JSON and sets Content-Type
                headers=headers,
                verify=self.verify_ssl,
                timeout=240     # Longer timeout for bulk exports
            )

            if response.status_code == 200:
                return response.json()
            else:
                self.logger.error(f"Post {endpoint} failed: HTTP {response.status_code}")
                self.logger.debug(f"Resposne: {response.text:500}")
                return None
            
        except requests.exceptions.Timeout:
            self.logger.error(f"POST {endpoint} timed out")
            return None
        
        except requests.exceptions.RequestException as e:
            self.logger.error(f"POST {endpoint} failed: {e}")
            return None
        
        except ValueError as e:
            # JSON parsing failed
            self.logger.error(f"Failed to parse response as JSON: {e}")
            return None
        
    # ---------------------------------------------------------
    # REQUIRED METHODS (from BaseDataSource)
    # ---------------------------------------------------------

    def get_sites(
            self,
            state: Optional[str] = None,
            bbox: Optional[tuple] = None,
            site_ids: Optional[List[str]] = None,
            **kwargs
    ) -> List[SiteInfo]:
        """
        Get locations from Aquarius
        
        Called locations not sites in Aquarius
        
        Arguments:
            state: not used for Aquarius
            
            bbox: Not used for Aquarius
            
            site_ids: If provided, filters results to only these locations (optional)
            
            **kwargs: Additional parameters

        returns:
            List of SiteInfo objects representing Aquarius locations

        Example:
            # Get all locations
            sites = aquarius.get_sites()
            
            # Get specific locations
            sites = aquarius.get_sites(site_ids=['HYDRA-1', 'HYDRA-2'])

        Unlike USGS which requires a location filter, Aquarius can return
        all locations at once. This is because Aquarius typically has far
        fewer locations than USGS's millions of sites.
        """

        self.logger.info("Fetching Aquarius locations...")

        # Make API request
        # The Aquarius endpoint is "locations"
        response = self._get("locations")

        # Handle failure
        if not response:
            self.loger.error("Failed to get locations from Aquarius")
            return []
        
        # Extract the locations list from response
        # Aquarius returns: {"locations:" [...], "otherField": ...}
        locations = response.get("locations", [])

        self.logger.debug(f"  Raw response contains {len(locations)} locations")

        # Convert each Aquarius location to out SiteInfo format
        sites = []

        for loc in locations:
            # Extract the location identifier
            loc_id = loc.get("identifier", "")

            # If site_ids filter is provided, skips non-matching locations
            if site_ids is not None:
                if loc_id not in site_ids:
                    continue
            
            # Create standardized SiteInfo object
            # Map Aquarius field to the standard fields
            site = SiteInfo(
                site_id=loc_id,
                name=loc.get("name", loc_id),   # Use ID as name if none provided
                latitude=loc.get("latitude"),   # Probably None
                longitude=loc.get("longitude"), # Probably None
                source=self.source_name,        # Aquarius

                # Store extra Aquarius-specific info in metadata
                metadata={
                    "description": loc.get("description"),
                    "locationType": loc.get("locationType"),
                    "identifier": loc_id,
                }
            )

            sites.append(site)

        self.logger.info(f"  Foudn {len(sites)} locations")

        # Log some details if we have sites
        if sites and self.logger.isEnabledFor(logging.DEBUG):
            self.logger.debug("  First few locations:")
            for site in sites[:3]:
                    self.logger.debug(f"   - {site.site_id}: {site.name}")
        
        return sites
    
    def get_available_parameters(self, site_id: str) -> List[Dict[str,str]]:
        """
        Get list of avaialable parameters at an Aquarius location
        
        This means finding all dtasets taht belong to a specific location
        Typically looks like: "Parameter@Location" (e.g., "Precipitation@HYDRA-1")
        
        Arguments:
            site_id: Location identifier
            
        Returns:
            List of dictionaries, each containing:
            - 'code': The paramenter name
            - 'name': TSame as code for Aquarius
            - 'unit': The unit of measurement

        Example:
            params = aquarius.get_available_paramenters("HYDRA-1")
        """

        self.logger.info(f"Getting available parameters for {site_id}")

        # Get all datasets from Aquarius
        response = self._get("data-set")

        if not response:
            self.logger.error("Failed to get datasets from Aquarius")
            return []
        
        datasets = response.get("datsets", [])

        self.logger.debug(f"  Total datasets in system: {len(datasets)}")

        # Find datsets belonging to this location
        parameters = []
        seen_params = set() # Track unique parameters

        for ds in datasets:
            identifier = ds.get("identifier", "")

            # Check if this datasets belongs to our location

            # Extract location from the identifier

            if "@" in identifier:
                # Split on @ and take the last part
                dataset_location = identifier.split("@")[-1]
            else:
                # No @ in identifier, use whole thing
                dataset_location = identifier
            
            # Case insensitive comparison
            if dataset_location.lower() != site_id.lower():
                continue

            # Extract parameter information
            param = ds.get("parameters", "")
            unit = ds.get("unit", "unknown")

            # Skip if parameter already seen
            if param in seen_params:
                continue

            seen_params.add(param)

            parameters.append({
                'code': param,      # Paramter name acts as code
                'name': param,      # Same as code for Aquarius
                'unit': unit
            })

            self.logger.info(f"  Found {len(parameters)} parameters at {site_id}")

            for p in parameters:
                self.logger.debut(f"   - {p['code'] ({p['unit']})}")

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
            Get time series data from Aquarius
            
            This method finds all datasets matching the sites and parameter,
            exports data for each dataset using the bulk export API,
            and converts the results to the standard DataPoint format
            
            Arguments:
                site_ids: List of location identifiers
                
                parameter: The Parameters name (ex. 'Precipitation')
                
                start_date: start date of the data period
                
                end_date: end date of the data period
                
                **kwargs: Additional parameters
                
            Returns:
                List of DataPoint objects containing the time series data
                
            Example:
                data = aquarius.get_data(
                    site_ids=['HYDRA-1', 'HYDRA-2'],
                    parameter='Precipitation',
                    start_date=datetime(2020, 1, 1),
                    end_date=datetime(2024, 1, 1)
                )
                
                print(f"Got {len(data)} data points")
                
                # Convert to DataFrame
                df = aquarius.to_dataframe(data)
            """

            self.logger.info("Fetching Aquarius data...")
            self.logger.info(f"  Sites: {', '.join(site_ids)}")
            self.logger.info(f"  Paramter: {parameter}")
            self.logger.info(f"  Date range: {start_date.date()} to {end_date.date()}")

            # Find the matchign datasets
            # We need to build datset identifiers like "Precipitation@HYDRA-1"
            dataset_identifiers = self._find_dataset_identifiers(
                site_id=site_ids,
                parameter=parameter,
                start_date=start_date,
                end_date=end_date
            )

            if not dataset_identifiers:
                self.logger.warning("No matching datasets found")
                return []
            
            self.logger.info(f"  Found {len(dataset_identifiers)} matching datasets")

            # Export data for each dataset
            all_data_points = []

            for i, identifier in enumerate(dataset_identifiers, 1):
                self.logger.info(f"  [{i}/{len(dataset_identifiers)}] Exporting {identifier}")

                # Export this dataset
                points = self._export_dataset(
                    identifier=identifier,
                    start_time=start_date,
                    end_time=end_date
                )

                all_data_points.extend(points)

                self.logger.debug(f"    Got {len(points)} points")
            
            self.logger.info(f"  Total: {len(all_data_points):,} data points")

            return all_data_points
    
    def _export_dataset(
            self,
            identifier: str,
            start_time: datetime,
            end_time: datetime
    ) -> List[DataPoint]:
        """
        Export time series data for a single Aquarius dataset.
        
        This uses the Aquarius bulk export API to retrieve data points.
        
        Args:
            identifier: The dataset identifier
            start_time: start of the export period
            end_time: end of the export period
            
        Returns:
            List of DataPoint objects
        """
        
        # Build the request body for the bulk export API
        body = {
            "Datasets": [
                {"identifier": identifier}
            ],
            "StartTime": start_time.isoformat(),
            "EndTime": end_time.isoformat(),
            "Interval": "PointAsRecorded", # Gives us raw data
            "RoundData": False, 
            "IncludeGradeCodes": False,
            "IncludeQualifiers": False,
            "IncludeApprovalLevels": False,
            "IncludeInterpolationTypes": False
        }

        # Make the API request
        result = self._post("export/bulk", body)

        if not result:
            self.logger.warning(f"No data returned for {identifier}")
            return []
        
        # Parse the response into DataPoint objects
        return self._parse_export_response(result)
    
    def _parse_export_response(self, result: Dict) -> List[DataPoint]:
        """
        Parse the Aquarius bulk export response into DataPoint objects

        AQUARIUS EXPORT RESPONSE STRUCTURE:

        {
            "series": [
                {
                    "dataset": {
                        "identifier": "Precipitation@HYDRA-1",
                        "locationIdentifier": "HYDRA-1",
                        "parameter": "Precipitation",
                        "unit": "inches",
                        ...
                    },
                    "numPoints": 1000,
                    "points": [
                        {
                            "timestamp": "2024-01-15T12:00:00Z",
                            "value": 0.05
                        },
                        {
                            "timestamp": "2024-01-15T12:15:00Z", 
                            "value": 0.02
                        },
                        ...
                    ]
                }
            ]
        }

        Arguments:
            result: The parsed JSON response form the export API

        Returns:
            List of DataPoint objects
        """

        data_points = []

        # Get the series list (usually just one series per dataset)
        series_list = result.get("series", [])

        for series in series_list:
            # Extract dataset metadata
            dataset_info = series.get("dataset", {})

            location = dataset_info.get("locationIdentifier", "unknown")
            parameter = dataset_info.get("parameter", "unknown")
            unit = dataset_info.get("unit", "unknown")

            # Get The data points
            points = series.get("points", [])

            num_points = series.get("numPoints", len(points))
            self.logger.debug(f"   Processing {num_points} points for {location}")

            # Convert each point
            for point in points:
                try:
                    # Parse timestamp
                    ts_str = point.get("timestamp", "")
                    if not ts_str:
                        continue

                    # Handle Aquarius timestamp format
                    # Usually ISO format "2024-01-15T12:00:00Z"

                    timestamp = self._parse_timestamp(ts_str)

                    if timestamp is None:
                        continue
                    
                    # Get the value
                    value = point.get("value")

                    # Skip missing values
                    if value is None:
                        continue

                    # Convert to float (might be string)
                    value = float(value)

                    # Create  standardized DataPoint
                    data_points.append(DataPoint(
                        timestamp=timestamp,
                        value=value,
                        location_id=location,
                        parameter=parameter,
                        unit=unit,
                        source=self.source_name
                    ))

                except (ValueError, TypeError) as e:
                    self.logger.debug(f"   Skipping invalud point: {e}")
                    continue

            return data_points
        
    def _parse_timestamp(self, ts_str: str) -> Optional[datetime]:
        """
        Parse and Aquarius timestamp string into a datetime object
        
        This method haldes all format variations
        
        Arguments:
            ts_str: the timestamp string from aquarius
            
        Returns:
            datetime object, or None if parsing fails
        """

        if not ts_str:
            return None
        
        try:
            # Handle 'Z' suffix (UTC)
            # Pythons fromisoformat doesnt like 'Z', wants +00:00
            if ts_str.endswith('Z'):
                ts_str = ts_str[:-1] + '+00:00'

            # Handle milliseconds
            if '.' in ts_str:
                # Split on the decimal point
                before_decimal = ts_str.split('.')[0]
                after_decimal = ts_str.split('.')[1]

                # Find where timezone starts 
                tz_part = ""
                for i, char in enumerate(after_decimal):
                    if char in ['+', "-"]:
                        tz_part = after_decimal[i:]
                        break
                
                # Reconstruct, no milliseconds
                ts_str = before_decimal + tz_part

            # Parse with fromisoformat
            return datetime.fromisoformat(ts_str)
        
        except ValueError as e:
            self.logger.debug(f"Failed to parse timestamp '{ts_str}': {e}")
            return None

    # ---------------------------------------------------------
    # CONVENIENCE METHODS - Make the API easier to use
    # ---------------------------------------------------------       
    
    def get_precipitation(
        self,
        site_ids: List[str],
        start_date: datetime,
        end_date: datetime
    ) -> List[DataPoint]:
        """
        Get precipitation data from specified sites
        
        This is a convenience method taht calsl get_data() with parameter='Precipitation' Saves typing for common use case
        
        Arguments:
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
        Get all available datasets from Aquarius
        
        Returns raw dataset info from Aquarius
        
        Returns:
            List of dataset dictionaries with fiels like:
            - identifier
            - parameter
            - unit
            - startOfRecord
            - endOfRecord

        Example:
            datasets = aquarius.get_datasets()
            for ds in datasets[:5]:
                print(f"{ds['identifier']}: {ds['parameter']}")
        """
        response = self._get("data-set")
        return response.get("datsets", []) if response else []
    
    def get_alerts(self) -> List[Dict]:
        """
        Get active alerts from Aquarius
        
        Alerts indicate unusual conditions
        
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
        Find datasets matching search criteria
        
        SEarches dataset identifiers for matching text
        Similar to find_datasets() from aquarius_data_downloader
        
        Arguments:
            must_contain_all: List of strings that must ALL appear in the identifier (case-insensitive)
            
            must_contain_any: List of strings where AT LEAST ONE must match the station name exactly

        Returns:
            List of matching dataset dictionaries

        Example:
            # Find all precipitation datasets
            datasets = aquarius.find_datasets(must_contain_all=['Precipitation'])
            
            # Find datasets for specific stations
            datasets = aquarius.find_datasets(
                must_contain_any=['HYDRA-1', 'HYDRA-2', 'HYDRA-3']
            )
            
            # Combine filters
            datasets = aquarius.find_datasets(
                must_contain_all=['Precipitation'],
                must_contain_any=['HYDRA-1', 'HYDRA-2']
            )
        """

        all_datasets = self.get_datasets()
        matching = []

        for ds in all_datasets:
            identifier = ds.get("identifier", "")

            # Extract station name from identifier
            if "@" in identifier:
                station = identifier.split("@")[-1]
            else:
                station = identifier

            matches = True

            # Check must_contain_any (extract station match)
            if must_contain_any:
                any_match = False
                for text in must_contain_any:
                    if text.lower() == station.lower():
                        any_match = True
                        break
                if not any_match:
                    matches = False

            # Check must_contain_all (substring mach)
            if must_contain_all and matches:
                for text in must_contain_all:
                    if text.lower() not in identifier.lower():
                        matches = False
                        break
        
            if matches:
                matching.append(ds)

        self.logger.info(f"Found {len(matching)} matching datasets")
        return matching


        
