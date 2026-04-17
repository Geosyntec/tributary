# data_sources/usgs.py
"""
USGS National Water Information System (NWIS) Data Source

This module provides access to USGS water data through their
public REST API. No authentication required!

API Documentation:
    https://waterservices.usgs.gov/

Common Parameter Codes:
    00060 - Discharge (cubic feet per second)
    00065 - Gage height (feet)
    00045 - Precipitation (inches)
    00010 - Water temperature (degrees Celsius)
    72019 - Depth to water level (feet below surface)
    00400 - pH (standard units)
    00300 - Dissolved oxygen (mg/L)

Site Types:
    ST - Stream (rivers, creeks)
    GW - Groundwater (wells)
    SP - Spring
    LK - Lake/Reservoir
    ES - Estuary
    AT - Atmosphere (weather stations)

Data Types:
    iv - Instantaneous values (real-time, typically every 15 minutes)
    dv - Daily values (daily statistics like mean, min, max)

Example:
    from data_sources import USGSDataSource
    from datetime import datetime, timedelta

    usgs = USGSDataSource()

    # Find stream sites in Oregon
    sites = usgs.get_sites(state='OR')

    # Get discharge data
    data = usgs.get_data(
        site_ids=['14211720'],
        parameter='00060',
        start_date=datetime(2024, 1, 1),
        end_date=datetime(2024, 1, 31)
    )

    # Convert to DataFrame
    df = usgs.to_dataframe(data)
"""

import requests
from datetime import datetime, timedelta
from typing import List, Optional, Dict, Any
import logging
from math import cos, radians

# Import Base classes from same package
from .base import BaseDataSource, DataPoint, SiteInfo

logger = logging.getLogger(__name__)


class USGSDataSource(BaseDataSource):
    """
    USGS National Water Information System API client.

    This class implements the BaseDataSource interface for USGS data.
    It handles all USGS-specific details (URL formats, response parsing)
    and converts everything to the standardized DataPoint format.

    No authentication is required - the USGS API is free and public!

    Example:
        usgs = USGSDataSource()

        # Find sites
        sites = usgs.get_sites(state='OR')

        # Get data
        data = usgs.get_data(
            site_ids=['14211720'],
            parameter='00060',
            start_date=datetime(2024, 1, 1),
            end_date=datetime(2024, 1, 31)
        )

        # Convert to DataFrame
        df = usgs.to_dataframe(data)
    """

    # Common parameter codes with their names and units
    PARAMETERS: Dict[str, Dict[str, str]] = {
        '00060': {'name': 'Discharge', 'unit': 'cfs'},
        '00065': {'name': 'Gage height', 'unit': 'ft'},
        '00045': {'name': 'Precipitation', 'unit': 'in'},
        '00010': {'name': 'Water temperature', 'unit': 'degC'},
        '00400': {'name': 'pH', 'unit': 'std units'},
        '00300': {'name': 'Dissolved oxygen', 'unit': 'mg/L'},
        '00095': {'name': 'Specific conductance', 'unit': 'uS/cm'},
        '72019': {'name': 'Depth to water level', 'unit': 'ft'},
    }

    # Site type codes
    SITE_TYPES: Dict[str, str] = {
        'ST': 'Stream',
        'GW': 'Groundwater',
        'SP': 'Spring',
        'LK': 'Lake/Reservoir',
        'ES': 'Estuary',
        'AT': 'Atmosphere',
    }

    def __init__(self):
        """
        Initialize the USGS data source.

        No authentication needed! Just create an instance and go.
        """

        # Call parent's __init__
        # Sets up: self.logger, self._cache, self._cache_enabled
        super().__init__()

        # Create a requests Session for connection reuse
        # Session reuses connections which is faster for multiple requests
        self.session = requests.Session()

        self.session.headers.update({
            'Accept': 'application/json',
        })

        # DISABLE SSL
        self.session.verify = False
        import urllib3
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

        self.logger.info("USGS data source initialized (no authentication required)")

    # -------------------------------------------------------------------------
    # REQUIRED PROPERTIES (from BaseDataSource)
    # -------------------------------------------------------------------------

    @property
    def source_name(self) -> str:
        """
        Return the name of this data source.

        This identifies the source in DataPoint objects and logs.
        When combining data from multiple sources, this tells you
        which data came from USGS.
        """
        return "USGS"

    @property
    def base_url(self) -> str:
        """
        Return the base URL for USGS water services.

        Endpoints built from this URL:
            {base_url}/iv/   - Instantaneous values
            {base_url}/dv/   - Daily values
            {base_url}/site/ - Site information
        """
        return "https://waterservices.usgs.gov/nwis"

    # -------------------------------------------------------------------------
    # REQUIRED METHODS (from BaseDataSource)
    # -------------------------------------------------------------------------

    def get_sites(
        self,
        state: Optional[str] = None,
        bbox: Optional[tuple] = None,
        site_ids: Optional[List[str]] = None,
        site_type: str = 'ST',
        has_data: bool = True,
        **kwargs
    ) -> List[SiteInfo]:
        """
        Get USGS monitoring sites.

        At least one of state, bbox, or site_ids must be provided.
        USGS has over 1.5 million sites so you can't get all of them at once.

        Args:
            state: Two-letter state code (e.g., 'OR', 'WA', 'CA')
            bbox: Bounding box as (west, south, east, north) in decimal degrees
                  Example: (-123.5, 45.0, -122.0, 46.5) for Portland area
            site_ids: List of specific USGS site numbers
                      Example: ['14211720', '14211550']
            site_type: Type of site to return. Default is 'ST' (streams).
                       Options: 'ST', 'GW', 'SP', 'LK', 'ES', 'AT'
            has_data: If True (default), only return sites that have data
            **kwargs: Additional parameters

        Returns:
            List of SiteInfo objects

        Raises:
            ValueError: If none of state, bbox, or site_ids is provided

        Example:
            # Get all stream sites in Oregon
            sites = usgs.get_sites(state='OR')

            # Get groundwater sites in Washington
            sites = usgs.get_sites(state='WA', site_type='GW')

            # Get specific sites by ID
            sites = usgs.get_sites(site_ids=['14211720', '14211550'])

            # Get sites in a geographic area
            portland_bbox = (-122.9, 45.3, -122.4, 45.7)
            sites = usgs.get_sites(bbox=portland_bbox)
        """

        self.logger.info("Fetching USGS sites...")

        url = f"{self.base_url}/site/"

        # Build query parameters
        params = {
            'format': 'rdb',
            'siteOutput': 'expanded',
        }

        # Location filter (at least one is required by USGS)
        if site_ids:
            params['sites'] = ','.join(site_ids)
            self.logger.info(f"  Searching for {len(site_ids)} specific sites")

        elif state:
            params['stateCd'] = state.upper()
            self.logger.info(f"  Searching in state: {state.upper()}")

        elif bbox:
            # USGS wants: west,south,east,north
            params['bBox'] = ','.join(str(coord) for coord in bbox)
            self.logger.info(f"  Searching in bbox: {bbox}")

        else:
            raise ValueError(
                "Must provide at least one of: state, bbox, or site_ids. "
                "USGS requires a location filter."
            )

        if site_type:
            params['siteType'] = site_type
            type_name = self.SITE_TYPES.get(site_type, site_type)
            self.logger.info(f"  Site type: {type_name}")

        if has_data:
            params['hasDataTypeCd'] = 'iv,dv'

        # Make the request
        try:
            self.logger.debug(f"  Request URL: {url}")
            self.logger.debug(f"  Parameters: {params}")

            response = self.session.get(url, params=params, timeout=30)
            response.raise_for_status()

            sites = self._parse_rdb_sites(response.text)

            self.logger.info(f"  Found {len(sites)} sites")
            return sites

        except requests.exceptions.Timeout:
            self.logger.error("Request timed out")
            return []

        except requests.exceptions.HTTPError as e:
            self.logger.error(f"HTTP error: {e.response.status_code}")
            self.logger.debug(f"Response: {e.response.text[:200]}")
            return []

        except requests.exceptions.RequestException as e:
            self.logger.error(f"Request failed: {e}")
            return []

    def get_available_parameters(self, site_id: str) -> List[Dict[str, str]]:
        """
        Get parameters available at a USGS site.

        Queries the site catalog to see what data types
        are available for a specific monitoring site.

        Args:
            site_id: USGS site number (e.g., '14211720')

        Returns:
            List of dicts with 'code', 'name', 'unit' keys

        Example:
            params = usgs.get_available_parameters('14211720')
            # Returns:
            # [
            #     {'code': '00060', 'name': 'Discharge', 'unit': 'cfs'},
            #     {'code': '00065', 'name': 'Gage height', 'unit': 'ft'},
            # ]
        """

        self.logger.info(f"Getting available parameters for site {site_id}")

        url = f"{self.base_url}/site/"

        params = {
            'format': 'rdb',
            'sites': site_id,
            'seriesCatalogOutput': 'true',
        }

        try:
            response = self.session.get(url, params=params, timeout=30)
            response.raise_for_status()

            parameters = []
            seen_codes = set()

            for line in response.text.split('\n'):
                if line.startswith('#') or not line.strip():
                    continue

                for part in line.split('\t'):
                    part = part.strip()
                    if part in self.PARAMETERS and part not in seen_codes:
                        seen_codes.add(part)
                        info = self.PARAMETERS[part]
                        parameters.append({
                            'code': part,
                            'name': info['name'],
                            'unit': info['unit']
                        })

            self.logger.info(f"  Found {len(parameters)} parameters")
            return parameters

        except requests.exceptions.RequestException as e:
            self.logger.error(f"Request failed: {e}")
            return []

    def get_data(
        self,
        site_ids: List[str],
        parameter: str,
        start_date: datetime,
        end_date: datetime,
        data_type: str = 'iv',
        **kwargs
    ) -> List[DataPoint]:
        """
        Get time series data from USGS.

        Retrieves actual measurement data for the specified sites
        and parameter over the given time period.

        Args:
            site_ids: List of USGS site numbers (e.g., ['14211720'])
            parameter: USGS parameter code (e.g., '00060' for discharge)
            start_date: Start of data period
            end_date: End of data period
            data_type: Type of data to retrieve:
                       'iv' = Instantaneous values (~15 min intervals)
                       'dv' = Daily values (daily statistics)
            **kwargs: Additional parameters

        Returns:
            List of DataPoint objects

        Notes:
            Instantaneous values ('iv') are typically available for
            the last 120 days with 15-minute resolution. Daily values
            ('dv') are available for the entire period of record.

        Example:
            # Get discharge data for the last week
            data = usgs.get_data(
                site_ids=['14211720'],
                parameter='00060',
                start_date=datetime(2024, 1, 1),
                end_date=datetime(2024, 1, 7)
            )

            # Get daily values instead of instantaneous
            data = usgs.get_data(
                site_ids=['14211720'],
                parameter='00060',
                start_date=datetime(2020, 1, 1),
                end_date=datetime(2024, 1, 1),
                data_type='dv'
            )
        """

        self.logger.info("Fetching USGS data...")
        self.logger.info(f"  Sites: {', '.join(site_ids)}")
        self.logger.info(f"  Parameter: {parameter}")

        # Look up parameter name if we know it
        param_info = self.PARAMETERS.get(parameter, {})
        param_name = param_info.get('name', parameter)
        self.logger.info(f"  Parameter name: {param_name}")

        self.logger.info(f"  Date range: {start_date.date()} to {end_date.date()}")
        self.logger.info(
            f"  Data type: {data_type} "
            f"({'Instantaneous' if data_type == 'iv' else 'Daily'} values)"
        )

        url = f"{self.base_url}/{data_type}/"

        params = {
            'format': 'json',
            'sites': ','.join(site_ids),
            'parameterCd': parameter,
            'startDT': start_date.strftime('%Y-%m-%d'),
            'endDT': end_date.strftime('%Y-%m-%d'),
        }

        try:
            response = self.session.get(url, params=params, timeout=120)
            response.raise_for_status()

            data = response.json()
            data_points = self._parse_json_data(data)

            self.logger.info(f"  Retrieved {len(data_points):,} data points")
            return data_points

        except requests.exceptions.Timeout:
            self.logger.error("Request timed out (data request may be too large)")
            return []

        except requests.exceptions.HTTPError as e:
            self.logger.error(f"HTTP error: {e.response.status_code}")
            if e.response.status_code == 400:
                self.logger.error("Bad request - check site IDs and parameter code")
            return []

        except ValueError as e:
            self.logger.error(f"Failed to parse JSON response: {e}")
            return []

        except requests.exceptions.RequestException as e:
            self.logger.error(f"Request failed: {e}")
            return []

    # -------------------------------------------------------------------------
    # RESPONSE PARSING METHODS
    # -------------------------------------------------------------------------

    def _parse_rdb_sites(self, rdb_text: str) -> List[SiteInfo]:
        """
        Parse USGS RDB (tab-delimited) format into SiteInfo objects.

        RDB format structure:
            - Lines starting with # are comments (skip)
            - First non-comment line is the header (column names)
            - Second line is format specifiers like "5s" (skip)
            - Remaining lines are data
            - Columns are separated by tabs

        Args:
            rdb_text: Raw text response from USGS

        Returns:
            List of SiteInfo objects
        """

        sites = []
        lines = rdb_text.strip().split('\n')

        # Find the header line (first line not starting with #)
        header_idx = None
        for i, line in enumerate(lines):
            if not line.startswith('#'):
                header_idx = i
                break

        if header_idx is None or header_idx >= len(lines) - 2:
            self.logger.warning("No data found in USGS response")
            return sites

        # Parse header to find column positions
        headers = lines[header_idx].lower().split('\t')

        self.logger.debug(f"  Headers found: {headers[:5]}...")

        # Helper to safely get column index
        def col_index(name: str) -> int:
            try:
                return headers.index(name)
            except ValueError:
                return -1

        idx_site_no = col_index('site_no')
        idx_name = col_index('station_nm')
        idx_lat = col_index('dec_lat_va')
        idx_lon = col_index('dec_long_va')
        idx_state = col_index('state_cd')
        idx_county = col_index('county_cd')
        idx_elev = col_index('alt_va')

        # Parse data lines (skip header and format line)
        for line in lines[header_idx + 2:]:
            if not line.strip() or line.startswith('#'):
                continue

            values = line.split('\t')

            # Helpers to safely extract values from the row
            def get_val(idx: int) -> Optional[str]:
                if idx < 0 or idx >= len(values):
                    return None
                val = values[idx].strip()
                return val if val else None

            def get_float(idx: int) -> Optional[float]:
                val = get_val(idx)
                if val is None:
                    return None
                try:
                    return float(val)
                except ValueError:
                    return None

            try:
                sites.append(SiteInfo(
                    site_id=get_val(idx_site_no) or 'unknown',
                    name=get_val(idx_name) or 'Unknown Station',
                    latitude=get_float(idx_lat),
                    longitude=get_float(idx_lon),
                    source=self.source_name,
                    state=get_val(idx_state),
                    county=get_val(idx_county),
                    elevation_ft=get_float(idx_elev),
                ))

            except Exception as e:
                self.logger.debug(f"  Skipping malformed line: {e}")
                continue

        return sites

    def _parse_json_data(self, data: Dict) -> List[DataPoint]:
        """
        Parse USGS JSON response into DataPoint objects.

        USGS JSON structure:
        {
            "value": {
                "timeSeries": [
                    {
                        "sourceInfo": {
                            "siteName": "COLUMBIA RIVER AT VANCOUVER, WA",
                            "siteCode": [{"value": "14211720"}]
                        },
                        "variable": {
                            "variableCode": [{"value": "00060"}],
                            "variableName": "Streamflow, ft³/s",
                            "unit": {"unitCode": "ft3/s"}
                        },
                        "values": [
                            {
                                "value": [
                                    {
                                        "value": "123000",
                                        "dateTime": "2024-01-15T12:00:00.000-08:00",
                                        "qualifiers": ["P"]
                                    },
                                    ...
                                ]
                            }
                        ]
                    }
                ]
            }
        }

        Args:
            data: Parsed JSON response from USGS

        Returns:
            List of DataPoint objects
        """

        data_points = []

        time_series_list = data.get('value', {}).get('timeSeries', [])

        self.logger.debug(f"  Found {len(time_series_list)} time series in response")

        for ts in time_series_list:

            # Extract site information
            source_info = ts.get('sourceInfo', {})
            site_codes = source_info.get('siteCode', [{}])
            site_id = site_codes[0].get('value', 'unknown') if site_codes else 'unknown'

            # Extract variable (parameter) information
            variable = ts.get('variable', {})
            var_codes = variable.get('variableCode', [{}])
            param_code = var_codes[0].get('value', 'unknown') if var_codes else 'unknown'
            param_name = variable.get('variableName', param_code)
            unit_info = variable.get('unit', {})
            unit = unit_info.get('unitCode', 'unknown')

            self.logger.debug(f"  Processing: {site_id} - {param_name}")

            # Extract the actual values
            values_container = ts.get('values', [{}])
            if not values_container:
                continue

            values_list = values_container[0].get('value', [])

            self.logger.debug(f"    Found {len(values_list)} values")

            for val in values_list:
                try:
                    # Parse the timestamp
                    timestamp_str = val.get('dateTime', '')
                    if not timestamp_str:
                        continue

                    timestamp = self._parse_timestamp(timestamp_str)
                    if timestamp is None:
                        continue

                    # Parse the value
                    value = val.get('value')
                    if value is None or value == '' or value == '-999999':
                        continue

                    value = float(value)

                    # Get quality flags
                    # Common: 'P' = Provisional, 'A' = Approved
                    qualifiers = val.get('qualifiers', [])
                    quality_flag = ','.join(qualifiers) if qualifiers else None

                    data_points.append(DataPoint(
                        timestamp=timestamp,
                        value=value,
                        location_id=site_id,
                        parameter=f"{param_code}:{param_name}",
                        unit=unit,
                        source=self.source_name,
                        quality_flag=quality_flag
                    ))

                except (ValueError, TypeError, KeyError) as e:
                    self.logger.debug(f"    Skipping invalid value: {e}")
                    continue

        return data_points

    def _parse_timestamp(self, ts_str: str) -> Optional[datetime]:
        """
        Parse a USGS timestamp string into a datetime object.

        USGS timestamps look like:
            "2024-01-15T12:00:00.000-08:00"
            "2024-01-15T12:00:00Z"

        Args:
            ts_str: Timestamp string from USGS

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
    # CONVENIENCE METHODS - Make common tasks easier
    # -------------------------------------------------------------------------

    def get_discharge(
        self,
        site_ids: List[str],
        start_date: datetime,
        end_date: datetime,
        instantaneous: bool = True
    ) -> List[DataPoint]:
        """
        Get streamflow/discharge data.

        Convenience method for the most common use case.
        Discharge (parameter code 00060) is the primary data type
        for USGS stream gages.

        Args:
            site_ids: List of USGS site numbers
            start_date: Start of data period
            end_date: End of data period
            instantaneous: If True (default), get 15-minute data.
                           If False, get daily values.

        Returns:
            List of DataPoint objects

        Example:
            end = datetime.now()
            start = end - timedelta(days=7)
            data = usgs.get_discharge(['14211720'], start, end)
        """
        return self.get_data(
            site_ids=site_ids,
            parameter='00060',
            start_date=start_date,
            end_date=end_date,
            data_type='iv' if instantaneous else 'dv'
        )

    def get_gage_height(
        self,
        site_ids: List[str],
        start_date: datetime,
        end_date: datetime,
        instantaneous: bool = True
    ) -> List[DataPoint]:
        """
        Get gage height (water level) data.

        Gage height is the water surface elevation above a reference point.

        Args:
            site_ids: List of USGS site numbers
            start_date: Start of data period
            end_date: End of data period
            instantaneous: If True, get 15-minute data. If False, daily.

        Returns:
            List of DataPoint objects
        """
        return self.get_data(
            site_ids=site_ids,
            parameter='00065',
            start_date=start_date,
            end_date=end_date,
            data_type='iv' if instantaneous else 'dv'
        )

    def get_precipitation(
        self,
        site_ids: List[str],
        start_date: datetime,
        end_date: datetime
    ) -> List[DataPoint]:
        """
        Get precipitation data.

        Note: Not all USGS sites record precipitation.
        This parameter is more common at weather stations.

        Args:
            site_ids: List of USGS site numbers
            start_date: Start of data period
            end_date: End of data period

        Returns:
            List of DataPoint objects
        """
        return self.get_data(
            site_ids=site_ids,
            parameter='00045',
            start_date=start_date,
            end_date=end_date,
            data_type='iv'
        )

    def get_recent(
        self,
        site_ids: List[str],
        parameter: str,
        days: int = 7
    ) -> List[DataPoint]:
        """
        Get the most recent N days of data.

        Convenience method for quickly checking recent conditions.

        Args:
            site_ids: List of USGS site numbers
            parameter: Parameter code (e.g., '00060')
            days: Number of days of data to retrieve (default: 7)

        Returns:
            List of DataPoint objects

        Example:
            # Get last 7 days of discharge
            data = usgs.get_recent(['14211720'], '00060')

            # Get last 30 days of gage height
            data = usgs.get_recent(['14211720'], '00065', days=30)
        """
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days)

        return self.get_data(
            site_ids=site_ids,
            parameter=parameter,
            start_date=start_date,
            end_date=end_date
        )

    def search_sites_near(
        self,
        latitude: float,
        longitude: float,
        radius_miles: float = 10,
        site_type: str = 'ST'
    ) -> List[SiteInfo]:
        """
        Search for sites near a geographic location.

        USGS doesn't have a direct radius search, so we create a
        bounding box around the location.

        Latitude degrees per mile is roughly constant (~1/69).
        Longitude degrees per mile varies with latitude because
        longitude lines converge at the poles. We use cos(latitude)
        to adjust for this.

        Args:
            latitude: Latitude in decimal degrees
            longitude: Longitude in decimal degrees
            radius_miles: Search radius in miles (default: 10)
            site_type: Type of site to find (default: 'ST' for streams)

        Returns:
            List of SiteInfo objects

        Example:
            # Find stream sites within 10 miles of Portland
            sites = usgs.search_sites_near(45.52, -122.68, radius_miles=10)
        """

        # Convert miles to approximate degrees
        lat_deg = radius_miles / 69.0
        lon_deg = radius_miles / (69.0 * abs(cos(radians(latitude))))

        bbox = (
            longitude - lon_deg,  # West
            latitude - lat_deg,   # South
            longitude + lon_deg,  # East
            latitude + lat_deg    # North
        )

        self.logger.info(
            f"Searching within {radius_miles} miles of ({latitude}, {longitude})"
        )
        self.logger.debug(f"  Bounding box: {bbox}")

        return self.get_sites(bbox=bbox, site_type=site_type)