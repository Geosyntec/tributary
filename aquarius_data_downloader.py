import requests
from datetime import datetime
import logging
import csv
import os
import pandas as pd
import sqlite3
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Get credentials from environment
BASE_URL = os.getenv("AQUARIUS_BASE_URL")
USERNAME = os.getenv("AQUARIUS_USERNAME")
PASSWORD = os.getenv("AQUARIUS_PASSWORD")
OUTPUT_DIR = os.getenv("OUTPUT_DIRECTORY")

DATASET_CONFIGS = {
    "15_min_precip_longest": {
        "must_contain_all": ["precip", "15min"],
        "must_contain_any": None,
        "must_start_before": "1980-01-01",
        "must_end_after": "2026-01-01"
    },

    "single_hydra": {
        "must_contain_all": ["precip", "15min"],
        "must_contain_any": ["HYDRA-2"],
        "must_start_before": None,
        "must_end_after": None
    }
}

ACTIVE_CONFIG = "15_min_precip_longest"

SAVE_CSV = True
SAVE_SQL = True


# Set up logging
logging.basicConfig(
    level = logging.DEBUG,
    format = "%(asctime)s - %(levelname)s - %(message)s",
    datefmt = "%H:%M:%S"
)
logger = logging.getLogger(__name__)

class AquariusPortal:
    def __init__(self, base_url, username, password, verify_ssl=True):
        self.base_url = base_url
        self.username = username
        self.password = password

        self.session = requests.Session()
        self.session.auth = (username, password)

        self.verify_ssl = verify_ssl

        if not verify_ssl:
            import urllib3
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
            logger.warning("SSL verification disabled")

        logger.info(f"Connecting to {base_url}...")

        self.test_connection()

    def _get(self, endpoint):
        # Get Helper Function
        
        url = f"{self.base_url}/{endpoint}" # Building the full URL
        response = self.session.get(url, verify = self.verify_ssl) # Getting data

        if response.status_code == 200: # 200 success, 404 not found, 500 server error
            return response.json()
        else:
            logger.error(f"Error on {endpoint}: {response.status_code}")
            return
    
    def _post(self, endpoint, data):
        # POST Helper Function


        url = f"{self.base_url}/{endpoint}" # Building the full URL

        headers = {
            "Accept": "application/json", # Requesting JSON data from server
            "Content-Type": "application/json" # Telling server I am sending data in JSON format (Since POST requires data send)
        }
        # json = data converts python dictionary to json
        response = self.session.post(url, json = data, headers = headers, verify = self.verify_ssl)

        if response.status_code == 200:
            return response.json()
        else:
            logger.error(f"Error on POST {endpoint}: {response.status_code}")
            logger.error(f"Response: {response.text[:500]}") # Error details
            return None

    def test_connection(self):
        # Testing connection to API
        url = f"{self.base_url}/version"
        response = self.session.get(url, verify=self.verify_ssl)

        if response.status_code == 200:
            version_info = response.json()
            logger.info(f"Connected Successfully! Version: {version_info['webPortalVersion']}")
        else:
            logger.error(f"Connection failed: {response.status_code}")

    
    # BASIC ENDPOINTS
    
    ## Locations ##

    def get_locations(self):
        return self._get("locations")
    
    def get_location(self, location_id):
        return self._get(f"locations/{location_id}")
    
    ## Datasets ##

    def get_datasets(self):
        return self._get("data-set")
    
    ## Alerts ##
    
    def get_alerts(self):
        return self._get("alerts")
    
    def get_alert(self, alert_id):
        return self._get(f"alerts/{alert_id}")
    
    ## Statistics ##
    
    def get_statistic_definitions(self):
        return self._get("statistics/latest")
    
    def get_statistic_definitions_by_param(self, param):
        return self._get(f"statistics/latest/{param}")
    
    def get_latest_values(self):
        return self._get("statistic-values/latest")
    
    def get_latest_values_by_param(self, param):
        return self._get(f"statistic-values/latest/{param}")
    
    ## Filters and Parameters
    
    def get_filters(self):
        return self._get("filters")
    
    def get_parameter_ranges(self):
        return self._get("parameter-ranges")
    
    def get_parameter_range(self, parameter_range_id):
        return self._get(f"parameters-ranges/{parameter_range_id}")
    
    ## Map Data (GeoJSON)

    def get_map_locations(self):
        return self._get("map/locations")
    
    def get_map_datasets(self, param):
        return self._get(f"map/datasets/{param}")
    
    ## Time Series Data Export

    def export_bulk(
        self,
        dataset_identifiers, # List of strings
        date_range = None, # Predefined range ex. Days7
        start_time = None,
        end_time = None,
        interval = "PointsAsRecorded", # How to group the data
        step = None, # Interval Multiplier
        timezone = None,
        round_data = False,
        include_grade_codes = False,
        include_qualifiers = False,
        include_approval_levels = False,
        include_interpolation_types = False):
        
        # Building datasets list
        datasets = []
        for identifier in dataset_identifiers:
            datasets.append({"identifier": identifier})

        # Building the request body
        body = {
            "Datasets": datasets, # See above
            "Interval": interval, # Aggreagation ex. "Hourly"
            "RoundData": round_data, 
            "IncludeGradeCodes": include_grade_codes,
            "IncludeQualifiers": include_qualifiers,
            "IncludeApprovalLevels": include_approval_levels,
            "IncludeInterpolationTypes": include_interpolation_types
        }

        if date_range: # Add date parameters (Only if specified)
            body["DateRange"] = date_range
        else: # Checks if start and end time were used instead of date_range
            if start_time:
                if isinstance(start_time, datetime):
                    start_time = start_time.isoformat()
                body["StartTime"] = start_time

            if end_time:
                if isinstance(end_time, datetime):
                    end_time = end_time.isoformat()
                body["EndTime"] = end_time
        
        # Optional Parameters
        if step is not None:
            body["Step"] = step

        if timezone is not None:
            body["Timezone"] = timezone
        
        # Logging
        logger.info(f"Exporting {len(dataset_identifiers)} datasets...")
        logger.info(f"  Interval: {interval}")
        if step:
            logger.info(f"  Step: {step}")
        
        if date_range:
            logger.info(f"  Date range: {date_range}")
        elif start_time or end_time:
            logger.info(f"  From: {start_time} To: {end_time}")

        # Request time!
        result = self._post("export/bulk", body)

        # Summarize results
        if result and "series" in result:
            total_points = 0
            for series in result["series"]:
                total_points += series.get("numPoints", 0)
            logger.info(f"Got {len(result['series'])} series with {total_points} total points")
        
        return result

    def find_datasets(
        self,
        must_contain_all = None,
        must_contain_any = None,
        must_start_before = None,
        must_end_after = None):

        datasets_response = self.get_datasets() # Gets all datasets
        datasets_list = datasets_response.get("datasets", []) 

        matching = []

        for ds in datasets_list:
            identifier = ds.get("identifier", "")
            start = (ds.get("startOfRecord") or "")[:10]
            end = (ds.get("endOfRecord") or "")[:10]

            matches = True

            if must_contain_any:
                contains = False
                for text in must_contain_any:
                    if text.lower() in identifier.lower():
                        contains = True
                        break
                if not contains:
                    matches = False

            if must_contain_all:
                for text in must_contain_all:
                    if text.lower() not in identifier.lower():
                        matches = False
                        break

            if must_start_before:
                if not start or start >= must_start_before:
                    matches = False
            
            if must_end_after:
                if not end or end <= must_end_after:
                    matches = False
            
            if matches:
                matching.append(ds)
            
        logger.info(f"Found {len(matching)}")
        return matching
    
def export_full_record(api, dataset_info):
    identifier = dataset_info.get("identifier")
    start_of_record = dataset_info.get("startOfRecord")
    end_of_record = dataset_info.get("endOfRecord")

    logger.info(f"Exporting: {identifier}")
    logger.info(f"  Period: {start_of_record[:10]} to {end_of_record}")

    data = api.export_bulk(
        dataset_identifiers=[identifier],
        start_time = start_of_record,
        end_time = end_of_record
    )

    return data

def export_multiple_datasets(api, datasets_list):
    results = []

    for i, dataset_info in enumerate(datasets_list, 1):
        identifier = dataset_info.get("identifier")
        logger.info(f"[{i}/{len(datasets_list)}] Processing {identifier}")

        data = export_full_record(api, dataset_info)

        if data:
            results.append((dataset_info, data))
        else:
            logger.warning(f"  Failed to export {identifier}")
    
    return results

def save_to_csv(data, filename):

    if not data or "series" not in data:
        logger.error("No data to save!")
        return
    
    full_path = os.path.abspath(filename)
    
    with open(filename, "w", newline = "") as file:
        writer = csv.writer(file)

        writer.writerow(["timestamp", "value", "location", "parameter", "unit"])

        full_path = os.path.abspath(filename)
        total_points = sum(s.get("numPoints", 0) for s in data["series"])
        logger.info(f"Saving {total_points} data points to {full_path}")

        for series in data["series"]:
            dataset_info = series["dataset"]
            location = dataset_info.get("locationIdentifier", "")
            parameter = dataset_info.get("parameter", "")
            unit = dataset_info.get("unit", "")

            points = series.get("points", [])

            for point in points:
                timestamp = point.get("timestamp", "")
                value = point.get("value", "")

                writer.writerow([timestamp, value, location, parameter, unit])
    
    logger.info(f"Saved to {full_path}")
    
    return

def clean_for_filename(text):

    if not text:
        return "unknown"
    
    replacements = {
        "@": "_at_",
        ".": "_",
        "/": "_",
        "\\": "_",
        ":": "_",
        "*": "_",
        "?": "_",
        '"': "_",
        "<": "_",
        ">": "_",
        "|": "_",
        " ": "_"
    }

    result = text
    for old, new in replacements.items():
        result = result.replace(old, new)

    return result

def generate_filename(dataset_info=None, output_dir=None, dl_csv=False, dl_sql=False):

    download_date = datetime.now().strftime("%Y-%m-%d")

    if dl_csv:
        identifier = dataset_info.get("identifier", "unknown")
        start_of_record = dataset_info.get("startOfRecord")[:10]
        end_of_record = dataset_info.get("endOfRecord")[:10]
        

        safe_identifier = clean_for_filename(identifier)

        filename = f"{safe_identifier}_{start_of_record}_to_{end_of_record}_downloaded_{download_date}.csv"
    
    elif dl_sql:
        filename = f"precipitation_15min_downloaded_{download_date}.db"

    else:
        raise ValueError("Must specify either dl_csv=True or dl_sql=True")

    if output_dir:
        os.makedirs(output_dir, exist_ok=True)
        return os.path.join(output_dir, filename)
    
    return filename

def save_dataset_to_csv(dataset_info, data, output_dir):

    filepath = generate_filename(dataset_info, output_dir, dl_csv = True)

    return save_to_csv(data, filepath)

def download_all_precipitation(api, output_dir, dl_csv, dl_sql, config_name):

    logger.info(f"Using configuration: {config_name}")

    config = DATASET_CONFIGS[config_name]
    
    logger.info(f"  Filters: {config['must_contain_all']}")
    logger.info(f"  Stations: {config['must_contain_any'] or 'All'}")

    matching_datasets = api.find_datasets(**config)

    if not matching_datasets:
        logger.error("No datasets found!")
        return
    
    logger.info(f"Found {len(matching_datasets)} datasets")
    
    all_data = [] # List used to collect data to then be appended onto the database at the end

    for i, dataset_info in enumerate(matching_datasets, 1):
        identifier = dataset_info.get("identifier")
        logger.info(f"[{i}/{len(matching_datasets)}] {identifier}")

        data = export_full_record(api, dataset_info)
        
        if not data:
            logger.warning("  Skipped - no data returned")
            continue

        if dl_csv:
            save_dataset_to_csv(dataset_info, data, output_dir)
        if dl_sql:
            all_data.append((dataset_info, data))
        
    if dl_sql and all_data:
        create_combined_database(all_data, output_dir)
    logger.info(f"Complete! Files saved to: {os.path.abspath(output_dir)}")

def create_combined_database(all_data, output_dir):

    if not all_data:
        logger.error("No data to save")
        return
    
    db_path = generate_filename(output_dir=output_dir, dl_sql = True)

    try:
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()

            cursor.execute("""
                CREATE TABLE IF NOT EXISTS precipitation_data ( 
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT NOT NULL,
                    value REAL,
                    location TEXT,
                    parameter TEXT,
                    unit TEXT,
                    dataset_identifier TEXT
                )
            """)
            # Creates table precipitation data
            # Creates column named id with type integer. 
            # Primary key is unique identifier for each row, automatically assigns 1, 2, 3 etc.
            # Cannot be a null value
            # Creates Column named values which contains REAL numbers (decimals 0.1, 3.22 etc.)

            rows_to_insert = []
            for dataset_info, data in all_data:

                if not data or "series" not in data:
                    continue

                for series in data["series"]:
                    series_info = series["dataset"]
                    location = series_info.get("locationIdentifier", "")
                    parameter = series_info.get("parameter", "")
                    unit = series_info.get("unit", "")
                    identifier = series_info.get("identifier")

                    points = series.get("points", [])

                    for point in points:
                        timestamp = point.get("timestamp", "")
                        value = point.get("value", "")

                        rows_to_insert.append((
                            timestamp,
                            value,
                            location,
                            parameter,
                            unit,
                            identifier
                        ))

            cursor.executemany("""
                INSERT INTO precipitation_data (timestamp, value, location, parameter, unit, dataset_identifier)
                VALUES (?, ?, ?, ?, ?, ?)
            """, rows_to_insert)

            conn.commit()
            logger.info(f"Saved to {db_path}")
        # Notes
        # conn.rollback() undo last commit changes 

    except sqlite3.Error as e:
        logger.error(f"Database error: {e}")
        raise

def check_database(output_dir):
    db_path = generate_filename(output_dir = output_dir, dl_sql = True)

    if not os.path.exists(db_path):
        logger.error(f"Database not found: {db_path}")
        return
    
    logger.info(f"Checking database: {db_path}")

    with sqlite3.connect(db_path) as conn:
    
        cursor = conn.cursor()

        cursor.execute("""
            SELECT location, COUNT(*) as count
            FROM precipitation_data
            GROUP BY location
            ORDER BY count DESC
        """)
        logger.info("Rows per location:")
        for row in cursor.fetchall():
            print(f"  {row[0]}: {row[1]:,} rows")

        cursor.execute("SELECT * FROM precipitation_data LIMIT 5")
        logger.info("First 5 rows:")
        for row in cursor.fetchall():
            print(row)

def main():
    #OUTPUT_DIR = "C:/Users/Guy.McFeeters/OneDrive - Geosyntec/BES_SPEC_MOD_ONCALL_TEAM - GC001_BES_Rainfall_Analysis_Task/Code/Outputs/Tests"
    api = AquariusPortal(
        base_url = BASE_URL,
        username = USERNAME,
        password = PASSWORD,
        verify_ssl = False # Skip SSL verification for work network
    )

    download_all_precipitation(api, OUTPUT_DIR, SAVE_CSV, SAVE_SQL, ACTIVE_CONFIG)

    check_database(OUTPUT_DIR)

if __name__ == "__main__":
    main()