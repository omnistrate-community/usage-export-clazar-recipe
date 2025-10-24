FROM python:3.9-slim

# Install cron
RUN apt-get update && apt-get install -y cron && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy requirements and install dependencies
COPY requirements.txt /app/
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code
COPY src/ /app/src/

# Copy the entrypoint script
COPY entrypoint.sh /app/
RUN chmod +x /app/entrypoint.sh

# Create log file for cron
RUN touch /var/log/cron.log

# Set the entrypoint
ENTRYPOINT ["/app/entrypoint.sh"]
