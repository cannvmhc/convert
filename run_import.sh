#!/bin/bash

# Run Flow 1: Import Excel files to database

source venv/bin/activate

# Set PYTHONPATH to project root
export PYTHONPATH=$(pwd):$PYTHONPATH

echo "Starting Flow 1: Import Excel files..."
python src/main.py --flow import
