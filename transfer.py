#!/usr/bin/env python3

import argparse
import sqlite3
import subprocess
from pathlib import Path
import logging
from datetime import datetime

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)


class DataTransfer:
    """
    Handles automated data transfer with retry logic and status tracking.

    This class performs rsync-based data transfers with a two-stage approach:
    1. Fast initial transfer using --whole-file and --inplace flags
    2. Verification using --append-verify to catch partial files

    All transfers are logged to SQLite database with status tracking.
    """

    def __init__(self, source, target, max_retries=5):
        """
        Initialize DataTransfer instance.

        Args:
            source (str): Source directory path to transfer from
            target (str): Target directory path to transfer to
            max_retries (int): Maximum number of retry attempts (default: 5)
        """
        self.source_path = Path(source).resolve()
        self.target_path = Path(target)
        self.max_retries = max_retries
        self.run_id = self.target_path.name
        self.target_run_dir = self.target_path / self.run_id

        script_dir = Path(__file__).resolve().parent
        self.logs_dir = script_dir / "logs"
        self.db_dir = script_dir / "db"
        self.logs_dir.mkdir(exist_ok=True)
        self.db_dir.mkdir(exist_ok=True)

        self.db_path = self.db_dir / "data_transfer.db"
        self._init_db()

    def _init_db(self):
        """
        Initialize SQLite database for transfer status tracking.

        Creates the 'run' table if it doesn't exist with columns for
        run_id, transfer_date, transfer_time, source_dir, target_dir,
        status, and log_path.
        """
        conn = sqlite3.connect(str(self.db_path))
        conn.execute("""CREATE TABLE IF NOT EXISTS run (
            run_id TEXT PRIMARY KEY, transfer_date TEXT,
            transfer_time TEXT, source_dir TEXT,
            target_dir TEXT, status TEXT,
            log_path TEXT
        )""")
        conn.commit()
        conn.close()

    def _update_status(self, status, **kwargs):
        """
        Update transfer status in the database.

        Args:
            status (str): Transfer status (PROCESSING, SUCCESS, FAILED)
            **kwargs: Additional fields for database update (date, time, source,
                    target, log_path) - required when status is PROCESSING
        """
        conn = sqlite3.connect(str(self.db_path))
        try:
            if status == "PROCESSING":
                conn.execute(
                    """INSERT OR REPLACE INTO run (
                            run_id, transfer_date,
                            transfer_time, source_dir,
                            target_dir, status,
                            log_path) 
                            VALUES (?, ?, ?, ?, ?, ?, ?)""",
                    (
                        self.run_id,
                        kwargs["date"],
                        kwargs["time"],
                        kwargs["source"],
                        kwargs["target"],
                        status,
                        kwargs["log_path"],
                    ),
                )
            else:
                conn.execute(
                    "UPDATE run SET status = ? WHERE run_id = ?", (status, self.run_id)
                )
            conn.commit()
        finally:
            conn.close()

    def _get_current_status(self):
        """
        Get current transfer status from database.

        Returns:
            str or None: Current status of the run_id, None if not found
        """
        conn = sqlite3.connect(str(self.db_path))
        try:
            cursor = conn.execute(
                "SELECT status FROM run WHERE run_id = ?", (self.run_id,)
            )
            result = cursor.fetchone()
            return result[0] if result else None
        finally:
            conn.close()

    def _get_dir_size(self):
        """
        Get human-readable size of source directory using du command.

        Returns:
            str: Directory size (e.g., "5.5G") or "Unknown" if command fails
        """
        result = subprocess.run(
            ["du", "-sh", str(self.source_path)], stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )
        return result.stdout.strip() if result.returncode == 0 else "Unknown"

    def run(self):
        """
        Execute the data transfer with retry logic and verification.

        The transfer process uses a two-stage approach:
        1. Fast initial rsync with --whole-file, --inplace, --no-compress
        2. Verification rsync with --append-verify to catch partial transfers

        Only the verification result determines success/failure. This handles
        cases where network interruptions cause partial files that the first
        rsync doesn't detect due to --inplace flag.

        Returns:
            str: Final transfer status ("SUCCESS", "FAILED", or existing status)
        """
        current_status = self._get_current_status()
        if current_status and current_status in ["SUCCESS", "PROCESSING"]:
            logger.info(f"Skipping {self.run_id} with status {current_status}")
            return current_status

        dir_size = self._get_dir_size()
        logger.info(f"Directory size: {dir_size}")

        now = datetime.now()
        log_filename = f"{self.run_id}_{now.strftime('%Y%m%d_%H%M%S')}.log"
        log_file_path = self.logs_dir / log_filename

        self._update_status(
            "PROCESSING",
            date=now.strftime("%m/%d/%Y"),
            time=now.strftime("%H:%M:%S"),
            source=str(self.source_path),
            target=str(self.target_path),
            log_path=str(log_file_path),
        )

        logger.info(f"Starting rsync for {self.run_id}")
        rsync_cmd = [
            "rsync",
            "-avP",
            "--update",
            "--whole-file",
            "--no-compress",
            "--inplace",
            "--numeric-ids",
            str(self.source_path),
            str(self.target_path) + "/",
        ]
        verification_cmd = [
            "rsync",
            "-avP",
            "--append-verify",
            "--numeric-ids",
            "--inplace",
            str(self.source_path),
            str(self.target_path) + "/",
        ]
        status = None
        for attempt in range(1, self.max_retries + 1):
            with open(log_file_path, "a" if attempt > 1 else "w") as log:
                if attempt > 1:
                    log.write(
                        f"\n{datetime.now().strftime('%m/%d/%Y')} {datetime.now().strftime('%H:%M:%S')} - Retry attempt {attempt} of {self.max_retries}\n"
                    )
                else:
                    log.write(
                        f"{now.strftime('%m/%d/%Y')} {now.strftime('%H:%M:%S')} - Transfer started at: {now.strftime('%Y-%m-%d %H:%M:%S')}\n"
                    )
                    log.write(
                        f"{now.strftime('%m/%d/%Y')} {now.strftime('%H:%M:%S')} - Source: {self.source_path}, Target: {self.target_path}\n"
                    )
                    log.write(
                        f"{now.strftime('%m/%d/%Y')} {now.strftime('%H:%M:%S')} - Source directory size: {dir_size}\n"
                    )
                    log.write(
                        f"{now.strftime('%m/%d/%Y')} {now.strftime('%H:%M:%S')} - Command: {' '.join(rsync_cmd)}\n"
                    )

                log.flush()
                result = subprocess.run(
                    rsync_cmd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    universal_newlines=True
                )
                combined_output = result.stdout + result.stderr
                log.write(combined_output)
                log.flush()
                result_verification = subprocess.run(
                    verification_cmd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    universal_newlines=True
                )
                verification_output = result_verification.stdout + result_verification.stderr
                log.write(verification_output)
                log.flush()
                end_time = datetime.now()
                all_output = combined_output + verification_output
                has_file_errors = (
                    'failed:' in all_output or
                    'rsync error:' in all_output or
                    'error' in all_output.lower()
                )
                if result.returncode == 0 and result_verification.returncode == 0 and not has_file_errors:
                    log.write(
                        f"{end_time.strftime('%m/%d/%Y')} {end_time.strftime('%H:%M:%S')} - Transfer completed successfully on attempt {attempt}\n"
                    )
                    status = "SUCCESS"
                    break
                else:
                    log.write(
                        f"{end_time.strftime('%m/%d/%Y')} {end_time.strftime('%H:%M:%S')} - Transfer failed on attempt {attempt}\n"
                    )
                    if attempt < self.max_retries:
                        logger.warning(
                            f"{self.run_id} transfer failed on attempt {attempt}, retrying..."
                        )

        if status != "SUCCESS":
            status = "FAILED"
            logger.error(
                f"{self.run_id} transfer failed after {self.max_retries} attempts"
            )

        self._update_status(status)

        log_transfer_cmd = [
            "rsync",
            "-avP",
            "--update",
            str(log_file_path),
            str(self.target_run_dir) + "/",
        ]

        subprocess.run(log_transfer_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        return status


def main():
    """
    Main entry point for command-line execution.

    Parses command-line arguments and executes data transfer.
    Exits with code 1 if transfer fails, 0 on success.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", required=True)
    parser.add_argument("--target", required=True)
    parser.add_argument(
        "--max-retries", type=int, default=5, help="Maximum number of retry attempts"
    )
    args = parser.parse_args()

    transfer = DataTransfer(args.source, args.target, args.max_retries)
    status = transfer.run()

    if status == "FAILED":
        exit(1)


if __name__ == "__main__":
    main()
