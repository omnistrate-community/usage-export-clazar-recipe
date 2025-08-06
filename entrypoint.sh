#!/bin/bash
set -e

# Install requirements if requirements.txt exists
if [ -f "requirements.txt" ]; then
    echo "Installing dependencies from requirements.txt..."
    pip install -r requirements.txt
fi

# Create environment file
echo "Creating environment file..."
printenv | grep -E '^[A-Z_][A-Z0-9_]*=' | sed 's/^\([^=]*\)=\(.*\)$/export \1="\2"/' > /app/env.sh

# Create wrapper script
cat > /app/run_job.sh << 'EOF'
#!/bin/bash
source /app/env.sh
cd /app
flock -n /tmp/metering_processor.lock python3 src/metering_processor.py
EOF

chmod +x /app/run_job.sh

# Install simple crontab
echo "*/5 * * * * /app/run_job.sh >> /var/log/cron.log 2>&1" | crontab -

# Start cron service
echo "Starting cron service..."
service cron start

# Verify setup
echo "Installed crontab:"
crontab -l

# Keep the container running and tail the cron log
echo "Cron job scheduled. Tailing log file..."
tail -f /var/log/cron.log