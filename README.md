# data-cron

## Introduction

A script to perform data transfer with retry logic and status tracking.

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

### 2. Configuration

Configure these variables in the script:

```bash
DATA_ROOT=""              # Root directory to search for data
RUN_FOLDER_PATTERN=""     # Pattern to match run folders
FILE_LOOKUP=""            # Specific file to look for (triggers transfer)
DESTINATION=""            # Target destination for transfers
SRC_DIR=""                # Script directory containing transfer.py
```

### 3. Cron Integration

Add to crontab for automated execution:

```bash
# Run every 6 hours
0 */6 * * * bash /path/to/data_cron.sh 
```

## Manual Usage

### Transfer Script

```bash
python3 transfer.py --source /path/to/source --target /path/to/dest --max-retries 10
```

#### Command Line Arguments

| Argument | Required | Description |
|----------|----------|-------------|
| `--source` | Yes | Source directory path (must exist and be readable) |
| `--target` | Yes | Destination directory path (local or remote) |
| `--max-retries` | No | Maximum retry attempts for rsync transfer (default: 10) |

### Status Monitoring

```bash
# View all transfers
utils/db_cli.py --db db/data_transfer.db query

# Filter by status
utils/db_cli.py --db db/data_transfer.db query --status SUCCESS

# Filter by specific run ID
utils/db_cli.py --db db/data_transfer.db query --run-id XXXXX

# Filter by specific run ID and show status column
utils/db_cli.py --db db/data_transfer.db query --run-id XXXXX --columns run_id status

# Display only run IDs for successful transfers
utils/db_cli.py --db db/data_transfer.db query --status SUCCESS --columns run_id

# Export to csv (including header)
utils/db_cli.py --db db/data_transfer.db query --status SUCCESS --header > exported_data.csv
```

#### Status Command Arguments

```
usage: db_cli.py query [-h] [--columns COLUMNS [COLUMNS ...]] [--limit LIMIT] [--where WHERE] [--order-by ORDER_BY] [--run-id RUN_ID] [--status STATUS] [--source-dir SOURCE_DIR] [--target-dir TARGET_DIR]
                       [--log-path LOG_PATH] [--transfer-date TRANSFER_DATE] [--transfer-time TRANSFER_TIME] [--header]

options:
  -h, --help            show this help message and exit
  --columns COLUMNS [COLUMNS ...]
                        Columns to select (default: all standard columns)
  --limit LIMIT         Maximum number of rows to return
  --where WHERE         Custom WHERE clause for complex filtering
  --order-by ORDER_BY   ORDER BY clause (default: transfer_date DESC, transfer_time DESC)
  --run-id RUN_ID       Filter by specific run_id
  --status STATUS       Filter by status
  --source-dir SOURCE_DIR
                        Filter by source directory
  --target-dir TARGET_DIR
                        Filter by target directory
  --log-path LOG_PATH   Filter by log path
  --transfer-date TRANSFER_DATE
                        Filter by transfer date
  --transfer-time TRANSFER_TIME
                        Filter by transfer time
  --header              Show column headers
```
