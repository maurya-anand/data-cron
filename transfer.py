#!/usr/bin/env python3

import argparse
import subprocess
import hashlib
import os
import shutil
import sqlite3
import logging
from datetime import datetime
from pathlib import Path
from scripts.models import init_database, insert_run_record, check_run_status, update_run_status, update_run_md5sum_path

def setup_logging(log_file_path):
    """Setup logging configuration with both file and console handlers."""
    # Create logger
    logger = logging.getLogger('data_transfer')
    logger.setLevel(logging.INFO)
    
    # Clear any existing handlers
    logger.handlers.clear()
    
    # Create formatters
    file_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    console_formatter = logging.Formatter('%(asctime)s - %(levelname)s: %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    
    # File handler - logs INFO and above to file
    file_handler = logging.FileHandler(log_file_path)
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(file_formatter)
    
    # Console handler - show INFO and above to user
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(console_formatter)
    
    # Add handlers to logger
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return logger

def get_md5_hash(file_path):
    """Calculate MD5 hash of a file."""
    hash_md5 = hashlib.md5()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()

def create_md5_table(directory):
    """Create MD5 hash table for all files in directory."""
    md5_table = {}
    directory_path = Path(directory)
    for file_path in directory_path.rglob('*'):
        if file_path.is_file():
            relative_path = file_path.relative_to(directory_path)
            md5_table[str(relative_path)] = get_md5_hash(str(file_path))
    return md5_table

def run_rsync(source_dir, target_dir, log_file, logger):
    """Run rsync command and capture output."""
    cmd = ["rsync", "-avP", source_dir + "/", target_dir, "--update", "--checksum"]
    
    logger.info(f"Running rsync: {' '.join(cmd)}")
    
    with open(log_file, "a") as log:
        log.write(f"Running: {' '.join(cmd)}\n")
        result = subprocess.run(cmd, stdout=log, stderr=log, text=True)
        log.write(f"Exit code: {result.returncode}\n")
        
    if result.returncode == 0:
        logger.info("Rsync completed successfully")
    else:
        logger.error(f"Rsync failed with exit code: {result.returncode}")
        
    return result.returncode == 0

def copy_log_to_remote(local_log_path, remote_destination, logger):
    """Copy log file to remote destination using scp."""
    try:
        cmd = ["scp", local_log_path, remote_destination]
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode == 0:
            logger.info(f"Successfully copied file: {remote_destination}")
        else:
            logger.error(f"Failed to copy file: Exit code: {result.returncode}")
            logger.error(f"stderr: {result.stderr}")
        return result.returncode == 0
    except Exception as e:
        logger.error(f"Exception during file copy: {e}")
        return False

def check_files_transferred(db_path, run_id, source_dir):
    """Check if all files from source are already recorded in file_record table."""
    # Get list of all files in source directory
    source_files = []
    source_path = Path(source_dir)
    for file_path in source_path.rglob('*'):
        if file_path.is_file():
            source_files.append(file_path.name)
    
    # Check database for existing files
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute('SELECT file_name FROM file_record WHERE run_ID = ?', (run_id,))
    db_files = [row[0] for row in cursor.fetchall()]
    conn.close()
    
    # Check if all source files are in database
    missing_files = set(source_files) - set(db_files)
    return len(missing_files) == 0, missing_files

def populate_file_records(db_path, run_id, target_dir, is_remote, source_dir, logger):
    """Populate file_record table with transferred files (only files that exist in source)."""
    # Get list of source files to compare against
    source_files = set()
    source_path = Path(source_dir)
    for file_path in source_path.rglob('*'):
        if file_path.is_file():
            source_files.add(file_path.name)
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    try:
        if is_remote:
            # For remote destinations, we need to list files using ssh
            try:
                user_host = target_dir.split(":")[0]  # anand@10.2.175.19
                remote_path = target_dir.split(":")[1]   # /home/anand/Documents/data-cron-test/Run-date1
                cmd = ["ssh", user_host, f"find {remote_path} -type f"]
                result = subprocess.run(cmd, capture_output=True, text=True)
                if result.returncode == 0:
                    for line in result.stdout.strip().split('\n'):
                        if line.strip():
                            full_path = line.strip()
                            file_name = Path(full_path).name
                            # Only record files that exist in source directory
                            if file_name in source_files:
                                # Check if already exists before inserting
                                cursor.execute('SELECT COUNT(*) FROM file_record WHERE run_ID = ? AND file_name = ?', (run_id, file_name))
                                if cursor.fetchone()[0] == 0:
                                    cursor.execute('INSERT INTO file_record (run_ID, file_name, full_path_target_dir) VALUES (?, ?, ?)', 
                                                 (run_id, file_name, full_path))
                else:
                    logger.error("Failed to list remote files")
            except Exception as e:
                logger.error(f"Error listing remote files: {e}")
        else:
            # For local destinations
            target_path = Path(target_dir)
            for file_path in target_path.rglob('*'):
                if file_path.is_file():
                    # Only record files that exist in source directory
                    if file_path.name in source_files:
                        # Check if already exists before inserting
                        cursor.execute('SELECT COUNT(*) FROM file_record WHERE run_ID = ? AND file_name = ?', (run_id, file_path.name))
                        if cursor.fetchone()[0] == 0:
                            cursor.execute('INSERT INTO file_record (run_ID, file_name, full_path_target_dir) VALUES (?, ?, ?)', 
                                         (run_id, file_path.name, str(file_path)))
        
        conn.commit()
    finally:
        conn.close()

def create_final_log(run_id, source_dir, target_dir, file_count):
    """Create final transfer log."""
    now = datetime.now()
    log_content = f"""Transfer Summary:
Run ID: {run_id}
Source Directory Path: {source_dir}
Target Directory Path: {target_dir}
Files Transferred: {file_count}
Date: {now.strftime('%d:%m:%Y')}
Time: {now.strftime('%H:%M:%S')}
"""
    return log_content

def create_md5_mapping_file(source_dir, target_dir, run_id, is_remote, local_tmp_dir):
    """Create MD5 mapping TSV file and return the path."""
    source_md5 = create_md5_table(source_dir)
    
    # Create TSV file
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    md5_file = f"{run_id}_md5_mapping_{timestamp}.tsv"
    
    if is_remote:
        local_md5_path = local_tmp_dir / md5_file
    else:
        local_md5_path = Path(target_dir) / md5_file
    
    with open(local_md5_path, 'w') as f:
        f.write("source_md5\tsource_file_path\ttarget_file_path\n")
        
        for relative_path, source_hash in source_md5.items():
            source_full_path = Path(source_dir) / relative_path
            
            if is_remote:
                target_full_path = f"{target_dir}/{relative_path}"
            else:
                target_full_path = Path(target_dir) / relative_path
            
            f.write(f"{source_hash}\t{source_full_path}\t{target_full_path}\n")
    
    return local_md5_path

def valid_directory(path):
    """Custom argparse type for validating directory paths."""
    if not os.path.exists(path):
        raise argparse.ArgumentTypeError(f"Directory does not exist: {path}")
    if not os.path.isdir(path):
        raise argparse.ArgumentTypeError(f"Path is not a directory: {path}")
    if not os.access(path, os.R_OK):
        raise argparse.ArgumentTypeError(f"Directory is not readable: {path}")
    return path

def main():
    # Get script directory once
    script_dir = Path(__file__).resolve().parent
    
    parser = argparse.ArgumentParser(
        description="""
A script that synchronizes files using rsync with checksum verification and MD5 integrity checking.
Maintains transfer logs and database records to track operations and prevent duplicate transfers.
        """,
        epilog="""
Examples:
  %(prog)s --source_dir /path/to/source --destination_dir /path/to/destination
  %(prog)s --source_dir /data/Run-001 --destination_dir user@host:/remote/path
  %(prog)s --source_dir /data/Run-001 --destination_dir /backup --max_retry 5
        """,
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--source_dir", type=valid_directory, required=True, 
                       help="Source directory path (must exist and be readable)")
    parser.add_argument("--destination_dir", required=True, 
                       help="Destination directory path (local: /path/to/dest or remote: user@host:/path/to/dest)")
    parser.add_argument("--max_retry", type=int, default=10,
                       help="Maximum number of retry attempts for rsync transfer (default: 10)")
    
    # Default database path relative to script location
    default_db_path = script_dir / "sync_info.db"
    parser.add_argument("--db_path", default=str(default_db_path),
                       help="Path to SQLite database file for tracking transfers (default: sync_info.db in script directory)")
    
    args = parser.parse_args()
    
    # Initialize database
    init_database(args.db_path)
    
    # Extract run_ID from source directory (last folder name)
    run_id = Path(args.source_dir).name
    
    # Create local tmp directory in script's directory
    local_tmp_dir = script_dir / "event_logs"
    local_tmp_dir.mkdir(exist_ok=True)
    
    # Create log file name and setup logging
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = f"{run_id}_{timestamp}.log"
    local_log_path = local_tmp_dir / log_file
    
    # Setup logging
    logger = setup_logging(local_log_path)
    
    logger.info(f"Starting data transfer process for Run ID: {run_id}")
    logger.info(f"Source directory: {args.source_dir}")
    
    # Check if run_ID already exists and has SUCCESS or PROCESSING status
    existing_status = check_run_status(args.db_path, run_id)
    if existing_status == "SUCCESS":
        logger.info(f"Run '{run_id}' already completed successfully. Transfer skipped.")
        return
    elif existing_status == "PROCESSING":
        logger.warning(f"Run '{run_id}' has status PROCESSING. Skipping to avoid conflicts.")
        return
    
    # Build the actual target directory by appending run_ID to destination
    if "@" in args.destination_dir and ":" in args.destination_dir:
        # Remote destination: user@host:/path -> user@host:/path/run_id
        actual_target_dir = args.destination_dir + "/" + run_id
        is_remote = True
    else:
        # Local destination: /path -> /path/run_id
        actual_target_dir = str(Path(args.destination_dir) / run_id)
        is_remote = False
    
    logger.info(f"Target directory: {actual_target_dir}")
    
    # For local destinations, create the target directory
    if not is_remote:
        Path(actual_target_dir).mkdir(parents=True, exist_ok=True)
    
    # Mark status as PROCESSING before starting transfer
    if existing_status is None:
        # Create temporary md5 map path for initial insert
        temp_md5_path = local_tmp_dir / f"{run_id}_temp.tsv"
        insert_run_record(args.db_path, run_id, args.source_dir, actual_target_dir, "PROCESSING", str(local_log_path), str(temp_md5_path))
    else:
        update_run_status(args.db_path, run_id, "PROCESSING")
    
    # Attempt rsync up to max_retry times
    success = False
    for attempt in range(1, args.max_retry + 1):
        logger.info(f"Transfer attempt {attempt}/{args.max_retry}")
        
        if run_rsync(args.source_dir, actual_target_dir, str(local_log_path), logger):
            # Populate file_record table for transferred files
            populate_file_records(args.db_path, run_id, actual_target_dir, is_remote, args.source_dir, logger)
            
            # Verify all source files are now in database
            all_transferred, missing_files = check_files_transferred(args.db_path, run_id, args.source_dir)
            if all_transferred:
                logger.info("Transfer successful - all files verified!")
                success = True
                break
            else:
                logger.warning(f"{len(missing_files)} files not found in target directory. Retrying...")
        else:
            logger.error(f"Rsync failed on attempt {attempt}")
    
    if not success:
        logger.error("All transfer attempts failed!")
    
    # Create MD5 mapping file before final log
    logger.info("Creating MD5 mapping file...")
    local_md5_path = create_md5_mapping_file(args.source_dir, actual_target_dir, run_id, is_remote, local_tmp_dir)
    
    # Count files in source directory
    source_file_count = sum(1 for file_path in Path(args.source_dir).rglob('*') if file_path.is_file())
    
    # Create and append final log
    final_log = create_final_log(run_id, args.source_dir, actual_target_dir, source_file_count)
    logger.info("All done!")
    with open(str(local_log_path), "a") as log:
        log.write("\n" + "="*50 + "\n")
        log.write(final_log)
        log.write("="*50 + "\n")
    
    # Copy files to destination
    final_log_path = local_log_path
    final_md5_path = local_md5_path
    
    if is_remote:
        # Copy log file to remote destination
        remote_log_path = actual_target_dir + "/" + log_file
        if copy_log_to_remote(str(local_log_path), remote_log_path, logger):
            final_log_path = remote_log_path
        else:
            logger.warning(f"Failed to copy log file to remote, keeping local: {local_log_path}")
        
        # Copy MD5 mapping file to remote destination
        remote_md5_path = actual_target_dir + "/" + local_md5_path.name
        if copy_log_to_remote(str(local_md5_path), remote_md5_path, logger):
            final_md5_path = remote_md5_path
        else:
            logger.warning(f"Failed to copy MD5 file to remote, keeping local: {local_md5_path}")
    else:
        # Move files to local destination
        dest_log_path = Path(actual_target_dir) / log_file
        dest_md5_path = Path(actual_target_dir) / local_md5_path.name
        shutil.move(str(local_log_path), str(dest_log_path))
        shutil.move(str(local_md5_path), str(dest_md5_path))
        final_log_path = dest_log_path
        final_md5_path = dest_md5_path
    
    # Update status and paths in database
    final_status = "SUCCESS" if success else "FAILED"
    update_run_status(args.db_path, run_id, final_status)
    update_run_md5sum_path(args.db_path, run_id, str(final_md5_path))
    logger.info(f"Transfer Status: {final_status}")
    
    # Update log file path in database if it changed
    if str(final_log_path) != str(local_log_path):
        conn = sqlite3.connect(args.db_path)
        cursor = conn.cursor()
        cursor.execute('UPDATE run SET transfer_log_file_path = ? WHERE run_ID = ?', (str(final_log_path), run_id))
        conn.commit()
        conn.close()

if __name__ == "__main__":
    main()
