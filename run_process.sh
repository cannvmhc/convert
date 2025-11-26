#!/bin/bash

# Run Flow 2: Process data from database

source venv/bin/activate

# Set PYTHONPATH to project root
export PYTHONPATH=$(pwd):$PYTHONPATH

echo "Starting Flow 2: Process data from database..."
python src/main.py --flow process
