#!/bin/bash

# Create a unique job ID using the script's Process ID ($$)
JOB_ID=$(date +"%Y%m%d_%H%M%S")
LOG_FILE="logs/download_job_${JOB_ID}.log"

# Run the script, passing all arguments ($@), and pipe to the unique log file
nohup ./bin/run_eodag_download.sh "$@" >> "$LOG_FILE" 2>&1 &

# Capture BG_PID and PGID
BG_PID=$!
BG_PGID=$(ps -o pgid= -p $BG_PID | tr -d '[:space:]')
echo "Job PID: $BG_PID | Job PGID: $BG_PGID" >> "$LOG_FILE"
echo "--------------------------------------------------------" >> "$LOG_FILE"


echo "Started background job!"
echo "Wrapper Job ID: $JOB_ID | Background PID: $BG_PID | Process Group (PGID): $BG_PGID"
echo "Output is being saved to: $LOG_FILE"
echo "To watch the output, run: tail -f $LOG_FILE"

