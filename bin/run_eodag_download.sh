#!/bin/bash

# Get the directory where this bash script is located (the bin/ folder)
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"

# Define the path to main.py (one directory up from bin/)
PYTHON_SCRIPT="$SCRIPT_DIR/../main.py"

# Set default values
COLLECTION=${1:-"S5P_L2_AER_AI"} # Aerosols collection
START_DATE="2024-06-01"
END_DATE=${3:-"2024-12-31"}

echo "Running eodag CDSE search and download..."
echo "Collection: $COLLECTION | Dates: $START_DATE to $END_DATE"
echo "--------------------------------------------------------"

# Execute the Python script
uv run python "$PYTHON_SCRIPT" -c "$COLLECTION" -s "$START_DATE" -e "$END_DATE"
