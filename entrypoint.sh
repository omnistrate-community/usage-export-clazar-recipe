#!/bin/bash
set -e

# Install requirements if requirements.txt exists
if [ -f "requirements.txt" ]; then
    echo "Installing dependencies from requirements.txt..."
    pip install -r requirements.txt
fi

# Run the Python script
echo "Running script: python3 src/metering_processor.py"
exec python3 "src/metering_processor.py"