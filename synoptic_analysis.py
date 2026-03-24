import sqlite3
import pandas as pd
from config import OUTPUT_DIR
import os

def main():
    # Find Database
    db_files = [f for f in os.listdir(OUTPUT_DIR) if f.endswith('.db')]
    db_path = os.path.join(OUTPUT_DIR, db_files[0])
    print(f"Using database: {db_path}")
    return

if __name__ == "__main__":
    main()