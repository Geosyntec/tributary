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
    
    df = _load_from_database(db_path)
    df['timestamp_aligned'] = df['timestamp'].dt.round('15min')

    logger.info(f"  Example: {df['timestamp'].iloc[0]} -> {df['timestamp_aligned'].iloc[0]}")

    logger.info("Pivoting to wide format")

    wide = df.pivot_table(
        index='timestamp_aligned',  # Rows = timestamps that are aligned to the 00:00 mark
        columns='location',         # Columns will be gage names
        values='value',             # Cell values will be rainfall amounts
        aggfunc='first'             # If duplicate entries, take the first
    )

    logger.info(f"Result: {len(wide):,} timestamps x {len(wide.columns)} gages")
    logger.info(f"Gages: {', '.join(wide.columns[:5])}{'...' if len(wide.columns) > 5 else ''}")

    return wide

def _load_from_database(db_path):
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

    return df

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

def investigate_true_missingness(rain_df):
    print(f"\n{'Gage':<12} {'Started':<22} {'Total After':<12} {'Missing':<12} {'% Missing':<10}")
    print("-" * 70)

    for gage in rain_df.columns:
        # FInd when gage first has data
        first_valid = rain_df[gage].first_valid_index()

        if first_valid is None:
            print(f"{gage:<12} {'NO DATA':<22}")
            continue
        
        # Slice from first data onwards
        gage_data = rain_df.loc[first_valid:, gage]

        total_after_start = len(gage_data)
        n_missing = gage_data.isna().sum()
        pct_missing = (n_missing / total_after_start) * 100

        print(f"{gage:<12} {str(first_valid)[:19]:<22} {total_after_start:<12,} {n_missing:<12,} {pct_missing:<10.1f}%")

def investigate_yearly_pattern(rain_df, gage_name):
    """
    Show missingness by year for a single gage.
    """
    logger.info(f"\n{'='*60}")
    logger.info(f"YEARLY PATTERN FOR {gage_name}")
    logger.info(f"{'='*60}")
    
    # Get just this gage's data
    gage_data = rain_df[gage_name]
    
    # Group by year
    gage_data_with_year = gage_data.to_frame()
    gage_data_with_year['year'] = gage_data_with_year.index.year
    
    print(f"\n{'Year':<8} {'Total':<10} {'Has Data':<10} {'Missing':<10} {'% Missing':<10}")
    print("-" * 50)
    
    for year in sorted(gage_data_with_year['year'].unique()):
        year_data = gage_data_with_year[gage_data_with_year['year'] == year][gage_name]
        
        total = len(year_data)
        n_missing = year_data.isna().sum()
        n_has_data = total - n_missing
        pct_missing = (n_missing / total) * 100
        
        # Visual bar
        bar_length = int((100 - pct_missing) / 5)  # 20 chars max
        bar = "█" * bar_length + "░" * (20 - bar_length)
        
        print(f"{year:<8} {total:<10,} {n_has_data:<10,} {n_missing:<10,} {pct_missing:>5.1f}%  {bar}")

def investigate_hourly_pattern(rain_df, gage_name):
    """
    Show data availability by hour of day.
    """
    logger.info(f"\n{'='*60}")
    logger.info(f"HOURLY PATTERN FOR {gage_name}")
    logger.info(f"{'='*60}")
    
    # Get just this gage's data
    gage_data = rain_df[gage_name]
    
    # Add hour column
    gage_data_with_hour = gage_data.to_frame()
    gage_data_with_hour['hour'] = gage_data_with_hour.index.hour
    
    print(f"\n{'Hour':<6} {'Total':<12} {'Has Data':<12} {'% Has Data':<12}")
    print("-" * 45)
    
    for hour in range(24):
        hour_data = gage_data_with_hour[gage_data_with_hour['hour'] == hour][gage_name]
        
        total = len(hour_data)
        n_has_data = hour_data.notna().sum()
        pct_has_data = (n_has_data / total) * 100 if total > 0 else 0
        
        # Visual bar
        bar_length = int(pct_has_data / 5)
        bar = "█" * bar_length + "░" * (20 - bar_length)
        
        print(f"{hour:02d}:00  {total:<12,} {n_has_data:<12,} {pct_has_data:>5.1f}%  {bar}")

def investigate_minute_pattern(rain_df, gage_name):
    """
    Show data availability by minute of hour.
    """
    logger.info(f"\n{'='*60}")
    logger.info(f"MINUTE PATTERN FOR {gage_name}")
    logger.info(f"{'='*60}")
    
    # Get just this gage's data
    gage_data = rain_df[gage_name]
    
    # Add minute column
    gage_data_with_minute = gage_data.to_frame()
    gage_data_with_minute['minute'] = gage_data_with_minute.index.minute
    
    print(f"\n{'Minute':<8} {'Total':<12} {'Has Data':<12} {'% Has Data':<12}")
    print("-" * 50)
    
    for minute in sorted(gage_data_with_minute['minute'].unique()):
        minute_data = gage_data_with_minute[gage_data_with_minute['minute'] == minute][gage_name]
        
        total = len(minute_data)
        n_has_data = minute_data.notna().sum()
        pct_has_data = (n_has_data / total) * 100 if total > 0 else 0
        
        # Visual bar
        bar_length = int(pct_has_data / 5)
        bar = "█" * bar_length + "░" * (20 - bar_length)
        
        print(f":{minute:02d}     {total:<12,} {n_has_data:<12,} {pct_has_data:>5.1f}%  {bar}")

def investigate_gage_timing(rain_df):
    for gage in rain_df.columns:
        has_data = rain_df[gage].dropna()

        minutes = has_data.index.minute.unique()
        minutes_sorted = sorted(minutes)

        print(f"{gage:<12} records at minutesL {minutes_sorted}")

def main():
    logger.info("Starting gauge analysis...")

    # Find the database
    db_path = find_latest_database(OUTPUT_DIR)
    
    if db_path is None:
        logger.error("Cannot continue without a database file")
        return
    
    # Load into DataFrame
    rain_df = load_database_to_dataframe(db_path)

    # Check true missingness
    investigate_true_missingness(rain_df)
    logger.info("Analysis complete!")

    # Below calls are useful to analyzing data gaps
    """investigate_data_quality(rain_df)
    investigate_yearly_pattern(rain_df, "HYDRA-4")
    investigate_hourly_pattern(rain_df, "HYDRA-4")
    investigate_minute_pattern(rain_df, "HYDRA-4")
    investigate_gage_timing(rain_df)
"""
    """first_gage = rain_df.columns[0]
    logger.info(f"\nTesting analysis on first gage: {first_gage}")

    result = analyze_single_gage(rain_df, first_gage)

    print("\n Result:")
    for key, value in result.items():
        print(f"  {key}: {value}")
    
    logger.info(f"Output directory: {OUTPUT_DIR}")
    
"""
if __name__ == "__main__":
    main()