#!/usr/bin/env python3

import sqlite3
import csv
import sys
from pathlib import Path
from datetime import datetime

def init_db(db_path):
    """Initialize database and create table if it doesn't exist"""
    conn = sqlite3.connect(db_path)
    conn.execute('''CREATE TABLE IF NOT EXISTS run (
        run_id TEXT PRIMARY KEY,
        transfer_date TEXT,
        transfer_time TEXT,
        source_dir TEXT,
        target_dir TEXT,
        status TEXT,
        log_path TEXT
    )''')
    conn.commit()
    return conn

def main():
    if len(sys.argv) != 3:
        print("Usage: python populate_db.py <csv_file> <db file>")
        sys.exit(1)
    
    csv_file = sys.argv[1]
    db_path = sys.argv[2]
    
    # Read CSV and insert into database
    conn = init_db(db_path)
    
    with open(csv_file, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            now = datetime.now()
            conn.execute("""
                INSERT OR REPLACE INTO run (run_id, transfer_date, transfer_time, source_dir, target_dir, status, log_path)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                row['run_id'],
                now.strftime('%m/%d/%Y'),
                now.strftime('%H:%M:%S'),
                "NA",
                "NA",
                "SUCCESS",
                "NA"
            ))
    
    conn.commit()
    conn.close()
    print(f"Data imported from {csv_file}")

if __name__ == "__main__":
    main()