import os
import subprocess
import sqlite3
from dotenv import load_dotenv
from pathlib import Path

# default DB path, can be overridden from .env (DB_PATH)
DB_PATH = "sync_info.db"

def init_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("""
        CREATE TABLE IF NOT EXISTS synced_files (
            filename TEXT PRIMARY KEY,
            mtime REAL
        )
    """)
    conn.commit()
    conn.close()

def get_synced_files():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT filename, mtime FROM synced_files")
    files = dict(c.fetchall())
    conn.close()
    return files

def update_synced_files(files):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    for filename, mtime in files.items():
        c.execute(
            "INSERT OR REPLACE INTO synced_files (filename, mtime) VALUES (?, ?)",
            (filename, mtime)
        )
    conn.commit()
    conn.close()

def get_files_to_sync(source_dir, synced_files):
    files_to_sync = []
    for root, _, files in os.walk(source_dir):
        for file in files:
            full_path = os.path.join(root, file)
            rel_path = os.path.relpath(full_path, source_dir)
            mtime = os.path.getmtime(full_path)
            if rel_path not in synced_files or synced_files[rel_path] < mtime:
                files_to_sync.append((full_path, rel_path, mtime))
    return files_to_sync

def main():
    load_dotenv()
    source_dir = os.getenv("SOURCE_DIR")
    destination_dir = os.getenv("DESTINATION_DIR")
    server_ip = os.getenv("SERVER_IP")
    server_user = os.getenv("SERVER_USER")
    # allow overriding the DB path from .env
    db_path_env = os.getenv("DB_PATH")
    if db_path_env:
        # update the module-level DB_PATH used by DB helper functions
        global DB_PATH
        DB_PATH = str(Path(db_path_env).expanduser())

    # ensure local directory for DB exists
    try:
        Path(DB_PATH).expanduser().resolve().parent.mkdir(parents=True, exist_ok=True)
    except Exception:
        # non-fatal: we'll attempt to open the DB later and let sqlite raise a helpful error
        pass

    if not all([source_dir, destination_dir, server_ip, server_user]):
        print("Missing required environment variables.")
        return

    init_db()
    synced_files = get_synced_files()
    files_to_sync = get_files_to_sync(source_dir, synced_files)

    if not files_to_sync:
        print("No new or updated files to sync.")
        return

    for full_path, rel_path, mtime in files_to_sync:
        remote_path = f"{server_user}@{server_ip}:{os.path.join(destination_dir, rel_path)}"
        # Ensure remote directory exists
        subprocess.run([
            "ssh", f"{server_user}@{server_ip}", f"mkdir -p '{os.path.dirname(os.path.join(destination_dir, rel_path))}'"
        ], check=True)
        # Sync file
        rsync_cmd = [
            "rsync",
            "-avz",
            full_path,
            remote_path
        ]
        try:
            subprocess.run(rsync_cmd, check=True)
            print(f"Synced: {rel_path}")
        except subprocess.CalledProcessError as e:
            print(f"Error syncing {rel_path}: {e}")

    # Update database
    update_synced_files({rel_path: mtime for _, rel_path, mtime in files_to_sync})
    print("Sync completed.")

if __name__ == "__main__":
    main()