#!/usr/bin/env python3

import argparse
import sqlite3
import subprocess
from pathlib import Path
import logging
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

def create_directories():
    script_dir = Path(__file__).resolve().parent
    logs_dir = script_dir / "logs"
    db_dir = script_dir / "db"
    logs_dir.mkdir(exist_ok=True)
    db_dir.mkdir(exist_ok=True)
    return logs_dir, db_dir

def init_db(db_dir):
    db_path = db_dir / 'data_transfer.db'
    conn = sqlite3.connect(str(db_path))
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

def get_dir_size(path):
    result = subprocess.run(['du', '-sh', str(path)], capture_output=True, text=True)
    if result.returncode == 0:
        return result.stdout.strip()
    return "Unknown"

def update_status(db_dir, run_id, status, **kwargs):
    """Helper function for database updates with proper connection handling"""
    conn = sqlite3.connect(str(db_dir / 'data_transfer.db'))
    try:
        if status == "PROCESSING":
            conn.execute("""INSERT OR REPLACE INTO run 
                            (run_id, transfer_date, transfer_time, source_dir, target_dir, status, log_path)
                            VALUES (?, ?, ?, ?, ?, ?, ?)""",
                        (run_id, kwargs['date'], kwargs['time'], kwargs['source'], 
                         kwargs['target'], status, kwargs['log_path']))
        else:
            conn.execute("UPDATE run SET status = ? WHERE run_id = ?", (status, run_id))
        conn.commit()
    finally:
        conn.close()

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", required=True)
    parser.add_argument("--target", required=True)
    parser.add_argument("--max-retries", type=int, default=5, help="Maximum number of retry attempts")
    args = parser.parse_args()
    
    logs_dir, db_dir = create_directories()
    
    source_path = Path(args.source).resolve()
    run_id = source_path.name
    target_path = Path(args.target)
    target_run_dir = Path(args.target) / run_id
    
    conn = init_db(db_dir)
    try:
        cursor = conn.execute("SELECT status FROM run WHERE run_id = ?", (run_id,))
        result = cursor.fetchone()
    finally:
        conn.close()
    if result and result[0] in ["SUCCESS", "PROCESSING"]:
        logger.info(f"{run_id} status {result[0]}")
        return
    
    dir_size = get_dir_size(source_path)
    logger.info(f"Directory size: {dir_size}")
    
    now = datetime.now()
    log_filename = f"{run_id}_{now.strftime('%Y%m%d_%H%M%S')}.log"
    log_file_path = logs_dir / log_filename
    
    update_status(db_dir, run_id, "PROCESSING", 
                  date=now.strftime('%m/%d/%Y'), 
                  time=now.strftime('%H:%M:%S'),
                  source=str(source_path), 
                  target=str(target_path), 
                  log_path=str(log_file_path))
    
    logger.info(f"Starting rsync for {run_id}")
    rsync_cmd = ["rsync", "-avP", "--update", str(source_path), str(target_path) + "/"]
    max_retries = args.max_retries
    status = None
    for attempt in range(1, max_retries + 1):
        with open(log_file_path, 'a' if attempt > 1 else 'w') as log:
            if attempt > 1:
                log.write(f"\n{datetime.now().strftime('%m/%d/%Y')} {datetime.now().strftime('%H:%M:%S')} - Retry attempt {attempt} of {max_retries}\n")
            else:
                log.write(f"{now.strftime('%m/%d/%Y')} {now.strftime('%H:%M:%S')} - Transfer started at: {now.strftime('%Y-%m-%d %H:%M:%S')}\n")
                log.write(f"{now.strftime('%m/%d/%Y')} {now.strftime('%H:%M:%S')} - Source: {source_path}, Target: {target_path}\n")
                log.write(f"{now.strftime('%m/%d/%Y')} {now.strftime('%H:%M:%S')} - Source directory size: {dir_size}\n")
                log.write(f"{now.strftime('%m/%d/%Y')} {now.strftime('%H:%M:%S')} - Command: {' '.join(rsync_cmd)}\n")
            
            log.flush()
            result = subprocess.run(rsync_cmd, stdout=log, stderr=subprocess.STDOUT)
            end_time = datetime.now()
            
            if result.returncode == 0:
                log.write(f"{end_time.strftime('%m/%d/%Y')} {end_time.strftime('%H:%M:%S')} - Transfer completed successfully on attempt {attempt}\n")
                status = "SUCCESS"
                break
            else:
                log.write(f"{end_time.strftime('%m/%d/%Y')} {end_time.strftime('%H:%M:%S')} - Transfer failed on attempt {attempt} (exit code: {result.returncode})\n")
                if attempt < max_retries:
                    logger.warning(f"{run_id} transfer failed on attempt {attempt}, retrying...")
    
    if status != "SUCCESS":
        status = "FAILED"
        logger.error(f"{run_id} transfer failed after {max_retries} attempts")  
    
    update_status(db_dir, run_id, status)

    logger.info(f"{run_id} transfer {status}")

    log_transfer_cmd = ["rsync", "-avP", "--update", str(log_file_path), str(target_run_dir) + "/"]
    subprocess.run(log_transfer_cmd, capture_output=True)
if __name__ ==  "__main__":
    main()