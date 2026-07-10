#!/bin/bash

# Get the directory where this bash script is located (the bin/ folder)
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"

# Define the path to main.py (one directory up from bin/)
PYTHON_SCRIPT="$SCRIPT_DIR/../main.py"

# Set default collection (Aerosols)
COLLECTION=${3:-"S5P_L2_AER_AI"} 

# Start/end date as input args
START_DATE=$1
END_DATE=$2
# START_DATE="2025-01-01"
# END_DATE="2025-05-31"

echo "Running eodag CDSE search and download..."
echo "Collection: $COLLECTION | Dates: $START_DATE to $END_DATE"
echo "--------------------------------------------------------"

# Execute the Python script
uv run python "$PYTHON_SCRIPT" -c "$COLLECTION" -s "$START_DATE" -e "$END_DATE"
