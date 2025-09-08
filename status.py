#!/usr/bin/env python3

import argparse
import sqlite3
import csv
import sys
from pathlib import Path

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", help="Show results for specific run ID")
    args = parser.parse_args()
    
    script_dir = Path(__file__).resolve().parent
    db_path = script_dir / "db" / 'data_transfer.db'
    
    if not db_path.exists():
        print("No database found.")
        return
    
    conn = sqlite3.connect(str(db_path))
    
    if args.run_id:
        cursor = conn.execute("""
            SELECT run_id, transfer_date, transfer_time, status, source_dir, target_dir 
            FROM run 
            WHERE run_id = ?
        """, (args.run_id,))
    else:
        cursor = conn.execute("""
            SELECT run_id, transfer_date, transfer_time, status, source_dir, target_dir 
            FROM run 
            ORDER BY transfer_date DESC, transfer_time DESC
        """)
    
    rows = cursor.fetchall()
    
    if rows:
        writer = csv.writer(sys.stdout)
        writer.writerow(["run_id", "transfer_date", "transfer_time", "status", "source_dir", "target_dir"])
        
        for row in rows:
            writer.writerow(row)
    else:
        print("No results found!")
    
    conn.close()

if __name__ == "__main__":
    main()