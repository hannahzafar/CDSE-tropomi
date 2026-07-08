#!/bin/bash

# Create a unique job ID using the script's Process ID ($$)
JOB_ID=$(date +"%Y%m%d_%H%M%S")
LOG_FILE="download_job_${JOB_ID}.log"

# Run the script, passing all arguments ($@), and pipe to the unique log file
nohup ./bin/run_eodag_download.sh "$@" > "$LOG_FILE" 2>&1 &

# $! captures the PID of the background process we just started
BG_PID=$!

echo "Started background job!"
echo "Wrapper Job ID: $JOB_ID | Background PID: $BG_PID"
echo "Output is being saved to: $LOG_FILE"
echo "To watch the output, run: tail -f $LOG_FILE"
