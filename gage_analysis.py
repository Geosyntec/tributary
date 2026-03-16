import logging
from datetime import datetime
import os
import sqlite3
import pandas as pd
import numpy as np


# Import shared configuration
from config import OUTPUT_DIR

# Set up logging
logging.basicConfig(
    level = logging.DEBUG,
    format = "%(asctime)s - %(levelname)s - %(message)s",
    datefmt = "%H:%M:%S"
)
logger = logging.getLogger(__name__)


def find_latest_database(output_dir):
    logger.info(f"Looking for database files in {output_dir}...")

    # Get all files in the directory
    all_files = os.listdir(output_dir)
    
    # Filter to only .db files
    db_files = [f for f in all_files if f.endswith('.db')]

    if not db_files:
        logger.error(f"No .db files found in {output_dir}")
        return None
    
    logger.info(f"Found {len(db_files)} database file(s)")

    #If multiple databases, picks most recent
    if len(db_files) > 1:
        # Sort by modification time, with most recent first
        db_files.sort(
            key=lambda f: os.path.getmtime(os.path.join(output_dir, f)),
            reverse=True
        )
    
    # Builds the full path
    db_path = os.path.join(output_dir, db_files[0])
    logger.info(f"Using database: {db_path}")

    return db_path

def load_database_to_dataframe(db_path):
    logger.info("Loading data from database")

    # Connect to database
    with sqlite3.connect(db_path) as conn:
        # Read data into dataframe
        df = pd.read_sql("""
            SELECT timestamp, value, location
            FROM precipitation_data
            ORDER BY timestamp
        """, conn)
    
    logger.info(f"Loaded {len(df):,} rows from database")

    # Convert timestamp column to datetime objects
    df['timestamp'] = pd.to_datetime(df['timestamp'])

    logger.info("Pivoting to wide format (one column per gauge)...")

    # Pivot: rows = timestamps, columns = locations, values = rainfall
    wide = df.pivot_table(
        index='timestamp',          # Rows = timestamps
        columns='location',         # Columns will be gage names
        values='value',             # Cell values will be rainfall amounts
        aggfunc='first'             # If duplicate entries, take the first
    )

    logger.info(f"Result: {len(wide):,} timestamps x {len(wide.columns)} gages")
    logger.info(f"Gages: {', '.join(wide.columns[:5])}{'...' if len(wide.columns) > 5 else ''}")

    return wide

def analyze_single_gage(rain_df, gage_name):
    total_timesteps = len(rain_df)

    # Returns true for every NaN value
    missing_mask = rain_df[gage_name].isna()

    # Counts number of True values and then calculates missing
    n_missing = missing_mask.sum()
    pct_missing = (n_missing / total_timesteps) * 100

    logger.info(f"  {gage_name}: missing {n_missing:,} of {total_timesteps:,} timesteps ({pct_missing:.1f}%)")

    return {
        'gage': gage_name,
        'n_missing': n_missing,
        'pct_missing': round(pct_missing, 2)
    }

def investigate_data_quality(rain_df):

    logger.info("\n" + "="*60)
    logger.info("DATA QUALITY INVESTIGATION")
    logger.info("="*60)

    # Time range info
    print(f"\nTime Range:")
    print(f"  Start: {rain_df.index.min()}")
    print(f"  End:   {rain_df.index.max()}")
    print(f"  Total Timesteps: {len(rain_df):,}")

    # Calc expected years
    time_span = rain_df.index.max() - rain_df.index.min()
    print(f"  Time span: {time_span.days / 365:.1f} years")

    # Check when each gage FIRST has data
    print(f"\nFirst valid data point per gage:")
    for gage in rain_df.columns:
        first_valid = rain_df[gage].first_valid_index()
        print(f"  {gage}: {first_valid}")

    for gage in rain_df.columns:
        total = len(rain_df)
        n_nan = rain_df[gage].isna().sum()
        n_zero = (rain_df[gage] == 0).sum()
        n_positive = (rain_df[gage] > 0).sum()

        pct_nan = (n_nan / total) * 100
        pct_zero = (n_zero / total) * 100
        pct_positive = (n_positive / total) * 100

        print(f"\n{gage}:")
        print(f"  NaN (missing): {n_nan:>8,} ({pct_nan:>5.1f}%)")
        print(f"  Zero values: {n_zero:>8,} ({pct_zero:>5.1f}%)")
        print(f"  Positive values: {n_positive:>8,} ({pct_positive:>5.1f}%)")
        print(f"  Total: {total:>8,}")

def main():
    logger.info("Starting gauge analysis...")

    # Find the database
    db_path = find_latest_database(OUTPUT_DIR)
    
    if db_path is None:
        logger.error("Cannot continue without a database file")
        return
    
    # Load into DataFrame
    rain_df = load_database_to_dataframe(db_path)

    # Looking at a few lines
    #logger.info("\nFirst 5 rows of data:")
    #print(rain_df.head())

    investigate_data_quality(rain_df)

    """first_gage = rain_df.columns[0]
    logger.info(f"\nTesting analysis on first gage: {first_gage}")

    result = analyze_single_gage(rain_df, first_gage)

    print("\n Result:")
    for key, value in result.items():
        print(f"  {key}: {value}")
    
    logger.info(f"Output directory: {OUTPUT_DIR}")
    logger.info("Analysis complete!")
"""
if __name__ == "__main__":
    main()