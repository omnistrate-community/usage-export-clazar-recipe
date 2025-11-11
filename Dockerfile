FROM python:3.14-slim

WORKDIR /app

# Copy requirements and install dependencies
COPY requirements.txt /app/
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code
COPY src/ /app/src/

# Expose healthcheck port
EXPOSE 8080

# Run the main application with unbuffered output
CMD ["python3", "-u", "src/main.py"]
