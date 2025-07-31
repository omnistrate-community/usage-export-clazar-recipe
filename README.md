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

# AWS Configuration
export AWS_ACCESS_KEY_ID="your-aws-access-key"  # AWS access key
export AWS_SECRET_ACCESS_KEY="your-aws-secret-key"  # AWS secret key
export AWS_REGION="us-east-1"  # AWS region

# Optional Configuration (with defaults)
export CLAZAR_API_URL="https://api.clazar.io/metering/"
export STATE_FILE_PATH="metering_state.json" # Stored in S3 bucket
export MAX_MONTHS_PER_RUN="12" # Maximum months to process in one run
export DRY_RUN="false" # Set to true for testing without sending data to Clazar
```

### Clazar Dimensions
This script assumes you are charging for the following dimensions and have configured them in Clazar:
- `memory_byte_hours`
- `storage_allocated_byte_hours`
- `cpu_core_hours`

If you are using different dimensions, update the script accordingly to aggregate and send the correct data.

Note the `quantity` field in the payload should always be a string of positive integers, as Clazar expects this format. So ensure your dimensions and aggregation logic align with this requirement.

## Job Behavior

### Which Months Are Processed

- On each run, the job determines the "next month to process":
  - If there is no previous processing, it starts from **two months ago** (relative to the current date), to avoid processing the current (possibly incomplete) month.
  - If there is a last processed month, it starts from the **month after the last processed month**.
- The job processes months sequentially, up to a maximum number of months per run (default: 12).
- The job **never processes the current or future months**—it only processes months that are fully in the past.

### How Error Contracts Are Handled

- When a contract fails to process for a given month (for example, due to a Clazar API error), the contract and its error details are recorded in the state file under `error_contracts` for that service/month/contract.
- On subsequent runs, the job checks both `success_contracts` and `error_contracts` for each contract-month. If a contract is present in either, it is skipped and **not retried**.
- This means that error contracts from previous months are **not automatically retried**. Usage for those contracts and months will not be sent to Clazar unless you take manual action.

### How to Re-run Error Contracts

To re-run error contracts for a previous month:
1. Open the state file (e.g., `metering_state.json` in S3).
2. Locate the relevant `error_contracts` entry for the service/month/contract you want to retry.
3. Remove the contract entry from the `error_contracts` list for that month.
4. Save the updated state file.
5. Re-run the metering job. The job will now attempt to process the contract again for that month.

## Running the Script

### Manual Execution

```bash
# Ensure AWS credentials are configured (via environment variables, AWS CLI, or IAM roles)
# Set required environment variables

# Test run (from project directory)
python3 metering_processor.py
```

### AWS Authentication Testing

To verify your AWS credentials are working correctly, you can use the provided test script:

```bash
# Test AWS authentication
python3 test_aws_auth.py
```

This script will:
- Check if AWS credentials are configured
- Test S3 connection by listing buckets
- Verify access to your specified S3 bucket

You can also test manually with:

```bash
# Using AWS CLI (if configured)
aws s3 ls s3://your-bucket-name/

# Or using Python boto3
python3 -c "import boto3; print(boto3.client('s3').list_buckets())"
```

## Tracking State

The state file stored in S3 tracks:
- Last processed month and last updated timestamp per service configuration
- List of successfully processed contracts per month per service configuration
- List of error contracts per month per service configuration

Example state file structure:
```json
{
  "Postgres:PROD:pt-HJSv20iWX0": {
    "last_processed_month": "2025-06",
    "last_updated": "2025-07-25T20:15:37Z",
    "success_contracts": {
      "2025-06": [
        "ae641bd1-edf8-4038-bfed-d2ff556c729e",
        "bf752ce2-fee9-5149-cgfe-e3gg667d83af"
      ]
    },
    "error_contracts": {
      "2025-06": [
        {
          "contract_id": "ce751fd3-ghi9-6159-dhgf-f4hh778e94bg",
          "errors": ["Some error"],
          "code": "ERROR_001",
          "message": "Error occurred"
        }
      ]
    }
  }
}
```

### Checking Logs
The script provides detailed logging. Monitor the output for:
- Any errors or warnings
- AWS authentication method being used
- State updates

Example output:
```
2025-07-25 20:15:32,604 - INFO - Using provided AWS credentials for region: us-east-1
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

## Troubleshooting

### AWS Authentication Issues

**Problem**: `NoCredentialsError` or `Unable to locate credentials`
**Solutions**:
1. Verify environment variables are set: `echo $AWS_ACCESS_KEY_ID`
2. Check AWS CLI configuration: `aws configure list`
3. Test with the authentication script: `python3 test_aws_auth.py`
4. Verify IAM permissions for S3 operations

**Problem**: `Access Denied` errors when accessing S3
**Solutions**:
1. Verify the S3 bucket name is correct
2. Check IAM permissions include `s3:GetObject`, `s3:ListBucket`, and `s3:PutObject`
3. Ensure the bucket exists and is in the correct AWS region

**Problem**: `Invalid security token` or `Token expired`
**Solutions**:
1. Refresh AWS credentials if using temporary tokens
2. Check if using IAM roles and the role is still valid
3. Re-run `aws configure` if using AWS CLI configuration
