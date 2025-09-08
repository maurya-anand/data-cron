DATA_ROOT=""
RUN_FOLDER_PATTERN=""
FILE_LOOKUP=""
DESTINATION=""
SRC_DIR=""

find ${DATA_ROOT}/${RUN_FOLDER_PATTERN} -name ${FILE_LOOKUP} -type f | \
while read file; do
    summary_dir=$(dirname "$file")
    run_dir=$(dirname $(dirname "$summary_dir"))
    python3 ${SRC_DIR}/transfer.py --run "$run_dir" --target ${DESTINATION}
done