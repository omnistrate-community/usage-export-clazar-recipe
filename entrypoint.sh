#!/bin/bash
set -e

# Install requirements if requirements.txt exists
if [ -f "requirements.txt" ]; then
    echo "Installing dependencies from requirements.txt..."
    pip install -r requirements.txt
fi

# Create environment file for cron jobs
echo "Creating environment file for cron..."
printenv | grep -E '^(AWS_|S3_|CLAZAR_|SERVICE_|ENVIRONMENT_|PLAN_|STATE_|MAX_|START_|DRY_|DIMENSION)' > /app/cron.env || true

# Start cron service
echo "Starting cron service..."
service cron start

# Run the script once immediately (optional) - don't exit if it fails
echo "Running initial execution: python3 src/metering_processor.py"
python3 "src/metering_processor.py" || echo "Initial execution failed, but cron job will continue to retry every 5 minutes..."

# Keep the container running and tail the cron log
echo "Cron job scheduled to run every 5 minutes. Tailing log file..."
tail -f /var/log/cron.log