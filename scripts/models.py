#!/usr/bin/env python3

import sqlite3
from datetime import datetime

def init_database(db_path):
    """Initialize the database with required tables."""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Create run table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS run (
            run_ID TEXT PRIMARY KEY,
            transfer_day TEXT NOT NULL,
            transfer_time TEXT NOT NULL,
            source_dir TEXT NOT NULL,
            target_dir TEXT NOT NULL,
            status TEXT NOT NULL,
            transfer_log_file_path TEXT NOT NULL,
            md5sum_map_path TEXT NOT NULL
        )
    ''')
    
    # Create file_record table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS file_record (
            run_ID TEXT,
            file_name TEXT NOT NULL,
            full_path_target_dir TEXT NOT NULL,
            FOREIGN KEY (run_ID) REFERENCES run (run_ID)
        )
    ''')
    
    conn.commit()
    conn.close()

def insert_run_record(db_path, run_id, source_dir, target_dir, status, log_file_path, md5sum_map_path):
    """Insert a new run record with the target_dir_name as run_ID."""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    now = datetime.now()
    transfer_day = now.strftime("%m/%d/%Y")
    transfer_time = now.strftime("%H:%M:%S")
    
    cursor.execute('''
        INSERT OR REPLACE INTO run (run_ID, transfer_day, transfer_time, source_dir, target_dir, status, transfer_log_file_path, md5sum_map_path)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    ''', (run_id, transfer_day, transfer_time, source_dir, target_dir, status, log_file_path, md5sum_map_path))
    
    conn.commit()
    conn.close()
    return run_id

def check_run_status(db_path, run_id):
    """Check if run_ID exists and return its status."""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    cursor.execute('SELECT status FROM run WHERE run_ID = ?', (run_id,))
    result = cursor.fetchone()
    
    conn.close()
    return result[0] if result else None

def update_run_status(db_path, run_id, status):
    """Update the status of an existing run."""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    cursor.execute('UPDATE run SET status = ? WHERE run_ID = ?', (status, run_id))
    
    conn.commit()
    conn.close()

def update_run_md5sum_path(db_path, run_id, md5sum_map_path):
    """Update the md5sum_map_path for an existing run."""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    cursor.execute('UPDATE run SET md5sum_map_path = ? WHERE run_ID = ?', (md5sum_map_path, run_id))
    
    conn.commit()
    conn.close()
