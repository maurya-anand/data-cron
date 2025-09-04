# data-cron

## Introduction

A script to performs secure file synchronization with comprehensive verification and logging. This script transfers files from a source directory to a destination (local or remote) using rsync with checksum verification, creates MD5 hash mappings for integrity validation, maintains detailed transfer logs, and tracks all operations in a SQLite database.

The script performs extensive checks:

- **Invalid source directory**: Validates existence and readability.
- **Network issues**: Retry mechanism for failed transfers.
- **Permission errors**: Detailed error logging.
- **Duplicate transfers**: Prevents multiple transfers of the same run_ID.
- **Incomplete transfers**: Verification step ensures all files are transferred.

### Recommendations

1. **SSH Key Setup**: For remote transfers, set up SSH key-based authentication.
1. **Network Stability**: Ensure stable network connection for remote transfers.
1. **Disk Space**: Verify sufficient disk space at destination.
1. **Permissions**: Ensure read access to source and write access to destination.

## Requirements

- **Python**: 3.8 or higher
- **rsync**: Must be installed and available in PATH
- **SSH access**: Required for remote destinations (with key-based authentication recommended)

## Setup

### 1. Clone the Repository

```bash
git clone https://github.com/maurya-anand/data-cron.git
cd data-cron
```

### 2. Verify

```bash
python3 transfer.py --help
```

## Usage

```bash
python3 transfer.py --source_dir SOURCE_DIR --destination_dir DESTINATION_DIR [OPTIONS]
```

### Command Line Arguments

| Argument | Required | Description |
|----------|----------|-------------|
| `--source_dir` | Yes | Source directory path (must exist and be readable) |
| `--destination_dir` | Yes | Destination directory path (local or remote) |
| `--max_retry` | No | Maximum retry attempts for rsync transfer (default: 10) |
| `--db_path` | No | Path to SQLite database file (default: sync_info.db in script directory) |

### Examples

#### Local Transfer

```bash
# Basic transfer
python3 transfer.py --source_dir /data/Run-001 --destination_dir /backup

# Transfer to remote server
python3 transfer.py --source_dir /data/Run-001 --destination_dir user@server:/remote/backup

# With custom database location & retry limit
python3 transfer.py --source_dir /data/Run-001 --destination_dir /backup --db_path /custom/path/transfers.db --max_retry 5
```

### Outputs

1. **Transfer Logs**: Detailed logs with timestamps stored in `event_logs/`.
1. **MD5 Mapping Files**: TSV files containing source MD5, source path, and target path.
1. **Database Records**: SQLite database tracking all transfer operations.
1. **Console Output**: Real-time progress with timestamps.
