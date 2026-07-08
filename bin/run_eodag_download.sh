#!/bin/bash

# 1. Get the directory where this bash script is located (the bin/ folder)
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"

# 2. Define the path to main.py (one directory up from bin/)
PYTHON_SCRIPT="$SCRIPT_DIR/../main.py"

# 3. Set default values
COLLECTION=${1:-"S5P_L2_AER_AI"} # Aerosols collection
START_DATE=${2:-"2025-01-01"}
END_DATE=${3:-"2025-01-02"}

echo "Running eodag CDSE search and download..."
echo "Collection: $COLLECTION | Dates: $START_DATE to $END_DATE"
echo "--------------------------------------------------------"

# 4. Execute the Python script
uv run python "$PYTHON_SCRIPT" -c "$COLLECTION" -s "$START_DATE" -e "$END_DATE"
