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