import argparse
import sqlite3
from datetime import datetime
from pathlib import Path


class TransferDB:
    """A database interface class for managing transfer run records in SQLite.

    This class provides CRUD operations for a transfer logging system.

    Attributes:
        db_path (Path): Path object representing the SQLite database file location.
    """

    def __init__(self, sqlite_db: str):
        """Initialize the TransferDB instance with database path.

        Args:
            sqlite_db (str): Path to the SQLite database file.
        """
        self.db_path = Path(sqlite_db)

    def _connect_db(self):
        """Create and return a connection to the SQLite database.

        Returns:
            sqlite3.Connection: Database connection object.
        """
        conn = sqlite3.connect(self.db_path)
        return conn

    def query(
        self,
        columns: list = None,
        limit: int = None,
        where_clause: str = None,
        order_by: str = "transfer_date DESC, transfer_time DESC",
    ):
        """Query the run table with optional filtering, ordering, and limiting.

        Args:
            columns (list, optional): List of column names to select. Defaults to all standard columns.
            limit (int, optional): Maximum number of rows to return. Defaults to None (no limit).
            where_clause (str, optional): SQL WHERE clause for filtering. Defaults to None.
            order_by (str, optional): SQL ORDER BY clause. Defaults to "transfer_date DESC, transfer_time DESC".

        Returns:
            TransferDB: Self reference for method chaining.

        Example:
            db.query(columns=['run_id', 'status'], limit=10, where_clause="status='SUCCESS'")
        """
        conn = self._connect_db()
        show_columns = [
            "run_id",
            "transfer_date",
            "transfer_time",
            "status",
            "source_dir",
            "target_dir",
            "log_path",
        ]
        if columns:
            show_columns = columns
        select_cols = ", ".join(show_columns)
        q = f"SELECT {select_cols} FROM run"
        if where_clause:
            q += f" WHERE {where_clause}"
        if order_by:
            q += f" ORDER BY {order_by}"
        if limit:
            q += f" LIMIT {limit}"
        cursor = conn.execute(q)
        self.results = cursor.fetchall()
        self.column_names = [description[0]
                             for description in cursor.description]
        conn.close()
        return self

    def show(self, header: bool = False):
        """Display query results in CSV format.

        Args:
            header (bool, optional): Whether to print column headers. Defaults to False.

        Note:
            This method prints directly to stdout. This chained after the query() call.
        """
        if header:
            header = ",".join(col for col in self.column_names)
            print(header)
        for row in self.results:
            row_str = ",".join(val for val in row)
            print(row_str)

    def update(self, run_id: str, **kwargs):
        """Update an existing run record with new values.

        Only updates the fields that are explicitly provided by the user.

        Args:
            run_id (str): Unique identifier for the run record.
            **kwargs: Keyword arguments for fields to update.
                Valid keys: source_dir, target_dir, status, log_path,
                transfer_date, transfer_time.

        Returns:
            TransferDB: Self reference for method chaining.

        Note:
            Prints confirmation message or error if run_id doesn't exist.
            Only provided fields will be updated, others remain unchanged.
            If you want to update timestamps, pass them explicitly.

        Example:
            db.update("run123", status="SUCCESS")  # Only updates status
            db.update("run123", source_dir="/new/path", status="FAILED")
            db.update("run123", status="SUCCESS", transfer_date="09/25/2025")  # Update status and date
        """
        conn = self._connect_db()
        cursor = conn.execute("SELECT * FROM run WHERE run_id = ?", (run_id,))
        existing_record = cursor.fetchone()

        if existing_record:
            set_clauses = []
            params = []
            valid_fields = {
                "source_dir",
                "target_dir",
                "status",
                "log_path",
                "transfer_date",
                "transfer_time",
            }
            for field, value in kwargs.items():
                if field in valid_fields:
                    set_clauses.append(f"{field} = ?")
                    params.append(value)
            if set_clauses:
                params.append(run_id)
                update_query = (
                    f"UPDATE run SET {', '.join(set_clauses)} WHERE run_id = ?"
                )
                conn.execute(update_query, params)
                conn.commit()
                print(f"Updated record for run_id: {run_id}")
            else:
                print(
                    f"No valid fields provided for update of run_id: {run_id}")
        else:
            print(f"{run_id} does not exist.")
        conn.close()
        return self

    def insert(self, run_id: str, **kwargs):
        """Insert a new run record into the database.

        Sets transfer_date and transfer_time to current datetime if not provided.

        Args:
            run_id (str): Unique identifier for the new run record.
            **kwargs: Keyword arguments for fields to insert.
                Valid keys: source_dir, target_dir, status, log_path,
                transfer_date, transfer_time.
                Fields default to "NA" except timestamps which default to current datetime.

        Returns:
            TransferDB: Self reference for method chaining.

        Note:
            Prints confirmation message or error if run_id already exists.
            Use update() for existing records.

        Example:
            db.insert("run123")  # Uses current datetime and "NA" for other fields
            db.insert("run123", status="SUCCESS", source_dir="/path/to/source")
            db.insert("run123", transfer_date="09/25/2025", transfer_time="10:30:00")
        """
        conn = self._connect_db()
        cursor = conn.execute("SELECT * FROM run WHERE run_id = ?", (run_id,))
        existing_record = cursor.fetchone()

        if not existing_record:
            defaults = {
                "transfer_date": datetime.now().strftime("%m/%d/%Y"),
                "transfer_time": datetime.now().strftime("%H:%M:%S"),
                "status": "NA",
                "source_dir": "NA",
                "target_dir": "NA",
                "log_path": "NA",
            }
            for key, value in kwargs.items():
                if key in defaults:
                    defaults[key] = value
            insert_query = """
                INSERT INTO run (run_id, transfer_date, transfer_time, status, 
                                source_dir, target_dir, log_path)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """
            conn.execute(
                insert_query,
                (
                    run_id,
                    defaults["transfer_date"],
                    defaults["transfer_time"],
                    defaults["status"],
                    defaults["source_dir"],
                    defaults["target_dir"],
                    defaults["log_path"],
                ),
            )
            conn.commit()
            print(f"Inserted new record for run_id: {run_id}")
        else:
            print(f"{run_id} already exists. Use update() instead.")
        conn.close()
        return self

    def delete(self, run_id):
        """Delete a run record from the database.

        Args:
            run_id (str): Unique identifier for the run record to delete.

        Returns:
            TransferDB: Self reference for method chaining.

        Note:
            Prints confirmation message or error if run_id doesn't exist.
        """
        conn = self._connect_db()
        cursor = conn.execute("SELECT * FROM run WHERE run_id = ?", (run_id,))
        existing_record = cursor.fetchone()
        if existing_record:
            cursor.execute("DELETE FROM run WHERE run_id = ?", (run_id,))
            conn.commit()
            conn.close()
            print(f"{run_id} deleted.")
        else:
            print(f"{run_id} does not exist.")
        return self

    def upsert(self, run_id: str, **kwargs):
        """Insert if record doesn't exist, otherwise update existing record.

        This is a convenience method that combines insert and update functionality.

        Args:
            run_id (str): Unique identifier for the run record.
            **kwargs: Keyword arguments passed to insert() or update() methods.
                     Valid keys: source_dir, target_dir, status, log_path.

        Returns:
            TransferDB: Self reference for method chaining.

        Example:
            db.upsert("run123", status="SUCCESS", source_dir="/path/to/source")
        """
        conn = self._connect_db()
        cursor = conn.execute("SELECT * FROM run WHERE run_id = ?", (run_id,))
        existing_record = cursor.fetchone()
        conn.close()
        if existing_record:
            return self.update(run_id, **kwargs)
        else:
            return self.insert(run_id, **kwargs)


def main():
    """Main function to handle command-line interface for CRUD operations."""
    parser = argparse.ArgumentParser(
        description="SQLite database interface for transfer run management"
    )
    parser.add_argument(
        "--db", help="Path to the sqlite db file", required=True)

    subparsers = parser.add_subparsers(
        dest='operation', help='Available operations')

    query_parser = subparsers.add_parser(
        'query', help='Query records from the database')
    query_parser.add_argument(
        '--columns', nargs='+', help='Columns to select (default: all standard columns)')
    query_parser.add_argument(
        '--limit', type=int, help='Maximum number of rows to return')
    query_parser.add_argument(
        '--where', help='Custom WHERE clause for complex filtering')
    query_parser.add_argument('--order-by', default='transfer_date DESC, transfer_time DESC',
                              help='ORDER BY clause (default: transfer_date DESC, transfer_time DESC)')
    query_parser.add_argument('--run-id', help='Filter by specific run_id')
    query_parser.add_argument('--status', help='Filter by status')
    query_parser.add_argument(
        '--source-dir', help='Filter by source directory')
    query_parser.add_argument(
        '--target-dir', help='Filter by target directory')
    query_parser.add_argument('--log-path', help='Filter by log path')
    query_parser.add_argument(
        '--transfer-date', help='Filter by transfer date')
    query_parser.add_argument(
        '--transfer-time', help='Filter by transfer time')
    query_parser.add_argument(
        '--header', action='store_true', help='Show column headers')

    insert_parser = subparsers.add_parser('insert', help='Insert a new record')
    insert_parser.add_argument(
        '--run-id', required=True, help='Unique run identifier')
    insert_parser.add_argument('--status', help='Transfer status')
    insert_parser.add_argument('--source-dir', help='Source directory path')
    insert_parser.add_argument('--target-dir', help='Target directory path')
    insert_parser.add_argument('--log-path', help='Log file path')
    insert_parser.add_argument(
        '--transfer-date', help='Transfer date (MM/DD/YYYY)')
    insert_parser.add_argument(
        '--transfer-time', help='Transfer time (HH:MM:SS)')

    update_parser = subparsers.add_parser(
        'update', help='Update an existing record')
    update_parser.add_argument(
        '--run-id', required=True, help='Run ID to update')
    update_parser.add_argument('--status', help='Transfer status')
    update_parser.add_argument('--source-dir', help='Source directory path')
    update_parser.add_argument('--target-dir', help='Target directory path')
    update_parser.add_argument('--log-path', help='Log file path')
    update_parser.add_argument(
        '--transfer-date', help='Transfer date (MM/DD/YYYY)')
    update_parser.add_argument(
        '--transfer-time', help='Transfer time (HH:MM:SS)')

    delete_parser = subparsers.add_parser('delete', help='Delete a record')
    delete_parser.add_argument(
        '--run-id', required=True, help='Run ID to delete')

    upsert_parser = subparsers.add_parser(
        'upsert', help='Insert or update a record')
    upsert_parser.add_argument(
        '--run-id', required=True, help='Unique run identifier')
    upsert_parser.add_argument('--status', help='Transfer status')
    upsert_parser.add_argument('--source-dir', help='Source directory path')
    upsert_parser.add_argument('--target-dir', help='Target directory path')
    upsert_parser.add_argument('--log-path', help='Log file path')
    upsert_parser.add_argument(
        '--transfer-date', help='Transfer date (MM/DD/YYYY)')
    upsert_parser.add_argument(
        '--transfer-time', help='Transfer time (HH:MM:SS)')

    args = parser.parse_args()

    if not Path(args.db).exists():
        print(f"Error: Database file '{args.db}' does not exist.")
        return

    db = TransferDB(sqlite_db=args.db)

    if args.operation == 'query':
        where_conditions = []
        where_clause = None
        if args.run_id:
            where_conditions.append(f"run_id='{args.run_id}'")
        if args.status:
            where_conditions.append(f"status='{args.status}'")
        if args.source_dir:
            where_conditions.append(f"source_dir='{args.source_dir}'")
        if args.target_dir:
            where_conditions.append(f"target_dir='{args.target_dir}'")
        if args.log_path:
            where_conditions.append(f"log_path='{args.log_path}'")
        if args.transfer_date:
            where_conditions.append(f"transfer_date='{args.transfer_date}'")
        if args.transfer_time:
            where_conditions.append(f"transfer_time='{args.transfer_time}'")
        if where_conditions:
            where_clause = " AND ".join(where_conditions)
        if args.where:
            if where_clause:
                where_clause = f"({where_clause}) AND ({args.where})"
            else:
                where_clause = args.where

        db.query(
            columns=args.columns,
            limit=args.limit,
            where_clause=where_clause,
            order_by=args.order_by
        ).show(header=args.header)

    elif args.operation == 'insert':
        kwargs = {}
        if args.status:
            kwargs['status'] = args.status
        if args.source_dir:
            kwargs['source_dir'] = args.source_dir
        if args.target_dir:
            kwargs['target_dir'] = args.target_dir
        if args.log_path:
            kwargs['log_path'] = args.log_path
        if args.transfer_date:
            kwargs['transfer_date'] = args.transfer_date
        if args.transfer_time:
            kwargs['transfer_time'] = args.transfer_time

        db.insert(args.run_id, **kwargs)

    elif args.operation == 'update':
        kwargs = {}
        if args.status:
            kwargs['status'] = args.status
        if args.source_dir:
            kwargs['source_dir'] = args.source_dir
        if args.target_dir:
            kwargs['target_dir'] = args.target_dir
        if args.log_path:
            kwargs['log_path'] = args.log_path
        if args.transfer_date:
            kwargs['transfer_date'] = args.transfer_date
        if args.transfer_time:
            kwargs['transfer_time'] = args.transfer_time

        db.update(args.run_id, **kwargs)

    elif args.operation == 'delete':
        db.delete(args.run_id)

    elif args.operation == 'upsert':
        kwargs = {}
        if args.status:
            kwargs['status'] = args.status
        if args.source_dir:
            kwargs['source_dir'] = args.source_dir
        if args.target_dir:
            kwargs['target_dir'] = args.target_dir
        if args.log_path:
            kwargs['log_path'] = args.log_path
        if args.transfer_date:
            kwargs['transfer_date'] = args.transfer_date
        if args.transfer_time:
            kwargs['transfer_time'] = args.transfer_time

        db.upsert(args.run_id, **kwargs)

    else:
        parser.print_help()


if __name__ == "__main__":
    main()
