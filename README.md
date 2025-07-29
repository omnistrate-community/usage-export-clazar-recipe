# S3 to Clazar Usage Export Script

This script automatically pulls usage metering data from S3 and uploads aggregated data to Clazar on a monthly basis. It maintains state in S3 to ensure no data gaps or duplicates, making it suitable for production deployment. The script tracks processed contracts per month to avoid duplicate submissions during reruns.

## Prerequisites

### System Requirements
- Python 3.7 or higher
- AWS CLI configured or AWS credentials available
- Network access to S3 and Clazar API

### Python Dependencies
```bash
pip install boto3 requests
```

### AWS Permissions
Your AWS credentials need the following S3 permissions:
```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "s3:GetObject",
                "s3:ListBucket",
                "s3:PutObject"
            ],
            "Resource": [
                "arn:aws:s3:::your-bucket-name",
                "arn:aws:s3:::your-bucket-name/*"
            ]
        }
    ]
}
```

Note: `s3:PutObject` permission is required for storing the state file in S3.

## Installation

### 1. Install Dependencies
```bash
# Activate your virtual environment if using one
source venv/bin/activate

# Install from requirements.txt
pip install -r requirements.txt
```

### 2. Configure AWS Credentials

Choose one of the following methods:

**Option A: AWS CLI Configuration**
```bash
aws configure
```

**Option B: Environment Variables**
```bash
export AWS_ACCESS_KEY_ID="your-access-key"
export AWS_SECRET_ACCESS_KEY="your-secret-key"
export AWS_DEFAULT_REGION="us-east-1"
```

## Configuration

### Required Environment Variables

Set these environment variables before running the script:

```bash
# Required Configuration
export S3_BUCKET_NAME="omnistrate-usage-metering-export-demo" # This should match your S3 bucket name
export SERVICE_NAME="Postgres" # This should match the service name in your S3 paths
export ENVIRONMENT_TYPE="PROD" # This should match the environment type in your S3 paths
export PLAN_ID="pt-HJSv20iWX0" # This should match the plan ID in your S3 paths
export CLAZAR_CLIENT_ID="your-clazar-client-id" # Your Clazar client ID
export CLAZAR_CLIENT_SECRET="your-clazar-client-secret" # Your Clazar client secret
export CLAZAR_CLOUD="aws"  # This should be the marketplace cloud (aws, azure, gcp, etc.)

# Optional Configuration (with defaults)
export CLAZAR_API_URL="https://api.clazar.io/metering/"
export STATE_FILE_PATH="metering_state.json" # Stored in S3 bucket
export MAX_MONTHS_PER_RUN="12" # Maximum months to process in one run
export DRY_RUN="false" # Set to true for testing without sending data to Clazar
```

## Key Features

### Monthly Processing
- The script processes usage data on a monthly basis, aggregating all usage for each month
- More efficient than hourly processing for large datasets
- Reduces API calls to Clazar

### Contract-Level Duplicate Prevention
- Tracks processed contracts per month in the state file
- Prevents duplicate submissions for the same contract in case of job reruns
- Allows partial month reprocessing if some contracts failed

### S3 State Storage
- State file is stored in the same S3 bucket as usage data
- Enables stateless execution environments (containers, serverless functions)
- Provides better reliability and accessibility across different execution environments

### Clazar Dimensions
This script assumes you are charging for the following dimensions and have configured them in Clazar:
- `memory_byte_hours`
- `storage_allocated_byte_hours`
- `cpu_core_hours`

If you are using different dimensions, update the script accordingly to aggregate and send the correct data.

Note the `quantity` field in the payload should always be a string of positive integers, as Clazar expects this format. So ensure your dimensions and aggregation logic align with this requirement.

## Running the Script

### Manual Execution

```bash
# Test run (from project directory)
python3 metering_processor.py
```

### Check Logs
The script provides detailed logging. Monitor the output for:
- Successfully processed months
- Contract-level processing status
- Any errors or warnings
- State updates

Example output:
```
2025-07-25 20:15:32,604 - INFO - Processing month 1/12: 2025-06
2025-07-25 20:15:32,604 - INFO - Processing month: 2025-06 for Postgres/PROD/pt-HJSv20iWX0
2025-07-25 20:15:32,662 - INFO - Found 744 subscription files in omnistrate-metering/Postgres/PROD/pt-HJSv20iWX0/2025/06/
2025-07-25 20:15:32,735 - INFO - Aggregated 2232 records into 9 entries
2025-07-25 20:15:32,736 - INFO - Filtered from 9 to 6 unprocessed contract records
2025-07-25 20:15:32,736 - INFO - Sending 6 metering records to Clazar for 2 contracts
2025-07-25 20:15:37,526 - INFO - Successfully sent data to Clazar
2025-07-25 20:15:37,526 - INFO - Response: {'results': [{'id': '4a4fefdc-07a9-4b84-a1ee-60c6bb690b12', 'cloud': 'aws', 'contract_id': 'ae641bd1-edf8-4038-bfed-d2ff556c729e', 'dimension': 'cpu_core_hours', 'quantity': '720', 'status': 'success', 'start_time': '2025-06-01T00:00:00Z', 'end_time': '2025-06-30T23:59:59Z', 'custom_properties': {}}]}
2025-07-25 20:15:33,869 - INFO - Saved state to S3: s3://omnistrate-usage-metering-export-demo/metering_state.json
```

## State File Structure

The state file stored in S3 tracks:
- Last processed month per service configuration
- List of processed contracts per month
- Last update timestamps

Example state file structure:
```json
{
  "Postgres:PROD:pt-HJSv20iWX0": {
    "last_processed_month": "2025-06",
    "last_updated": "2025-07-25T20:15:37Z",
    "processed_contracts": {
      "2025-06": [
        "ae641bd1-edf8-4038-bfed-d2ff556c729e:2025-06",
        "bf752ce2-fee9-5149-cgfe-e3gg667d83af:2025-06"
      ]
    }
  }
}
```

## Deployment Options

### Scheduled Execution
Run the script on a monthly schedule using:

**Cron (Linux/macOS):**
```bash
# Run on the 1st day of each month at 2 AM
0 2 1 * * /path/to/python3 /path/to/metering_processor.py
```

**Docker:**
```dockerfile
FROM python:3.9-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt

COPY metering_processor.py .
CMD ["python", "metering_processor.py"]
```

**AWS Lambda (serverless):**
The script works well in serverless environments since state is stored in S3. Configure the Lambda function with:
- Timeout: 15 minutes (for processing large datasets)
- Memory: 512 MB or higher
- Environment variables as listed above
- CloudWatch Events rule to trigger monthly

### Monitoring and Alerting

Monitor the following metrics:
- Script execution success/failure
- Number of contracts processed per month
- API response times from Clazar
- S3 read/write operations

Set up alerts for:
- Script failures
- Unusual processing times
- Clazar API errors
- Missing usage data

## Troubleshooting

### Common Issues

**1. No usage data found**
- Verify S3 bucket path and structure matches expected format
- Check if data exists for the target month
- Ensure AWS credentials have proper S3 permissions

**2. Clazar API errors**
- Verify access token is valid and not expired
- Check if dimensions are properly registered in Clazar
- Review API rate limits

**3. State file issues**
- Ensure S3 bucket has write permissions
- Check if state file format is valid JSON
- Verify state file path in environment variables

**4. Duplicate submissions**
- The script automatically handles duplicates per contract
- Check state file to see which contracts have been processed
- Use dry run mode to test without actual submissions

### Reset Processing State

To reprocess a specific month for all contracts:
```bash
# Delete or modify the state file in S3 to remove processed contracts for that month
aws s3 rm s3://your-bucket/metering_state.json
# Or edit the state file to remove specific month entries
```

## Migration from Hourly to Monthly Processing

If you're migrating from an hourly version of this script:

1. **Backup existing state file**
2. **Update environment variables** (change MAX_HOURS_PER_RUN to MAX_MONTHS_PER_RUN)
3. **Test with dry run mode** first
4. **Monitor the first few runs** carefully

The new monthly processing will:
- Start from 2 months ago if no previous processing history exists
- Automatically aggregate all hourly data into monthly totals
- Track processed contracts to avoid duplicates during reruns
