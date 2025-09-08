#!/usr/bin/env bash

DATA_ROOT=""
RUN_FOLDER_PATTERN=""
FILE_LOOKUP=""
DESTINATION=""
SRC_DIR=""

mkdir -p ${SRC_DIR}/db ${SRC_DIR}/logs
CRON_LOG="${SRC_DIR}/logs/cron_status.log"

find ${DATA_ROOT}/${RUN_FOLDER_PATTERN} -name ${FILE_LOOKUP} -type f | \
while read file; do
    summary_dir=$(dirname "$file")
    run_dir=$(dirname $(dirname "$summary_dir"))
    python3 ${SRC_DIR}/transfer.py --source "$run_dir" --target ${DESTINATION} >> ${CRON_LOG} 2>&1
done