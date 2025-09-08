#!/usr/bin/env python3

import argparse
import sqlite3
import csv
import sys
from pathlib import Path

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", help="Show results for specific run ID")
    parser.add_argument("--status", choices=["SUCCESS", "FAILED", "PROCESSING"], 
                       help="Filter by status")
    parser.add_argument("--print-ids", action="store_true", 
                       help="Print only run IDs without headers")
    args = parser.parse_args()
    
    script_dir = Path(__file__).resolve().parent
    db_path = script_dir / "db" / 'data_transfer.db'
    
    if not db_path.exists():
        print("No database found.")
        return
    
    conn = sqlite3.connect(str(db_path))
    
    where_conditions = []
    params = []
    
    if args.run_id:
        where_conditions.append("run_id = ?")
        params.append(args.run_id)
    
    if args.status:
        where_conditions.append("status = ?")
        params.append(args.status)
    
    if args.print_ids:
        base_query = "SELECT run_id FROM run"
    else:
        base_query = """
            SELECT run_id, transfer_date, transfer_time, status, source_dir, target_dir 
            FROM run
        """
    
    if where_conditions:
        query = base_query + " WHERE " + " AND ".join(where_conditions)
    else:
        query = base_query
    
    query += " ORDER BY transfer_date DESC, transfer_time DESC"
    
    cursor = conn.execute(query, params)
    rows = cursor.fetchall()
    
    if rows:
        if args.print_ids:
            for row in rows:
                print(row[0])
        else:
            writer = csv.writer(sys.stdout)
            writer.writerow(["run_id", "transfer_date", "transfer_time", "status", "source_dir", "target_dir"])
            
            for row in rows:
                writer.writerow(row)
    else:
        if not args.print_ids:
            print("No results found!")
    
    conn.close()

if __name__ == "__main__":
    main()