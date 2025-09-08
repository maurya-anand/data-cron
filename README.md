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
# View all transfer runs in CSV format
python3 status.py

# View specific run by ID
python3 status.py --run-id XXXX
```

#### Status Command Arguments

| Argument | Required | Description |
|----------|----------|-------------|
| `--run-id` | No | Show results for specific run ID only |
