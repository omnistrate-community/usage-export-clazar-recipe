# S3 to Clazar Usage Export Script

This script automatically pulls usage metering data from S3 and uploads aggregated data to Clazar on a monthly basis. It maintains state in S3 to ensure no data gaps or duplicates, making it suitable for production deployment. The script tracks processed contracts per month to avoid duplicate submissions during reruns.

## Prerequisites

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

## Configuration

### Customize Clazar Dimensions
This script supports both default and custom dimensions:

**Default dimensions:**
- `memory_byte_hours`
- `storage_allocated_byte_hours`
- `cpu_core_hours`

**Custom dimensions:**
You can define custom dimensions using environment variables with the format `DIMENSION_<name>=<formula>`.

Examples:
```bash
# Define a custom pod_hours dimension calculated from cpu_core_hours
DIMENSION_pod_hours="cpu_core_hours / 2"

# Define memory in GB hours instead of byte hours
DIMENSION_memory_gb_hours="memory_byte_hours / 1073741824"

# Define storage in TB hours
DIMENSION_storage_tb_hours="storage_allocated_byte_hours / 1099511627776"

# Complex calculation combining multiple dimensions
DIMENSION_compute_units="(cpu_core_hours * 2) + (memory_byte_hours / 1073741824)"
```

**Formula syntax:**
- Use basic arithmetic operators: `+`, `-`, `*`, `/`, `(`, `)`
- Reference base dimensions by their exact names (case-sensitive)
- Use built-in functions: `abs()`, `round()`, `min()`, `max()`, `int()`, `float()`
- Formulas are evaluated safely with no access to system functions

Note: The `quantity` field in the payload should always be a string of positive integers, as Clazar expects this format. Custom dimensions are automatically converted to integers before sending.

## Job Behavior

### Cron Job Mode
The script can run as a continuous cron job by setting the environment variable `CRON_MODE=true`. In this mode:
- The script runs continuously and executes automatically on the **first day of every month at 00:10 UTC**
- It waits for the scheduled time and processes all pending months
- After completion, it sleeps until the next scheduled run
- Perfect for production deployment where you want automatic monthly processing

### Processing Logic

- On each run, the job determines the "next month to process":
  - If there is no previous processing, it starts from **two months ago** (relative to the current date), to avoid processing the current (possibly incomplete) month.
  - If there is a last processed month, it starts from the **month after the last processed month**.
- The job processes months sequentially, up to a maximum number of months per run (default: 12).
- The job **never processes the current or future months**—it only processes months that are fully in the past.

### Error Handling and Retry Logic

- **Automatic retries**: When a contract fails to process for a given month (for example, due to a Clazar API error), the script automatically retries up to 5 times with exponential backoff (2^attempt seconds).
- **Error tracking**: Failed contracts and their error details are recorded in the state file under `error_contracts` for that service/month/contract.
- **Payload preservation**: The exact payload that failed is stored in the state file, so you can see the usage values and manually add them in Clazar if needed.
- **Retry on subsequent runs**: Error contracts are automatically retried on subsequent runs until they succeed or reach the maximum retry limit.
- **Per-contract processing**: Each contract is processed individually, so one failing contract doesn't block others from being processed successfully.

### Subscription Cancellation
Please note that the script does not handle subscription cancellations. If a subscription is canceled, you will need to manually upload the usage data for that contract and month to Clazar in time. Every marketplace has a grace period for submitting usage data after a subscription ends, so ensure you are aware of those deadlines.

## Environment Variables

### Required
- `AWS_ACCESS_KEY_ID`: AWS access key ID
- `AWS_SECRET_ACCESS_KEY`: AWS secret access key
- `AWS_REGION`: AWS region (default: uses AWS default)
- `S3_BUCKET_NAME`: S3 bucket name
- `CLAZAR_CLIENT_ID`: Clazar client ID for authentication
- `CLAZAR_CLIENT_SECRET`: Clazar client secret for authentication
- `CLAZAR_CLOUD`: Cloud provider name (e.g., "aws", "gcp", "azure")
- `SERVICE_NAME`: Name of the service (e.g., "Postgres")
- `ENVIRONMENT_TYPE`: Environment type (e.g., "PROD", "DEV")
- `PLAN_ID`: Plan ID (e.g., "pt-HJSv20iWX0")

### Optional
- `STATE_FILE_PATH`: Path to state file in S3 (default: "metering_state.json")
- `START_MONTH`: Start month for processing (format: YYYY-MM, default: "2025-06")
- `DRY_RUN`: Set to "true" to run without sending data to Clazar (default: "false")
- `DIMENSION_<name>`: Custom dimension formulas (see Custom Dimensions section)

## Deploy in Omnistrate
To deploy the Clazar Exporter in Omnistrate, run the following command in your terminal. Make sure you have the Omnistrate CLI installed and configured before running the command.

```bash
omctl build-from-repo --product-name "Clazar Exporter"
```

To run the job, you can create a resource instance in Omnistrate with the necessary parameters.

## Running the Script

### One-time Execution
To run the script once and process all pending months:
```bash
python3 src/metering_processor.py
```

### Cron Job Mode (Recommended for Production)
To run the script as a continuous cron job that automatically executes on the first day of every month at 00:10 UTC:
```bash
CRON_MODE=true python3 src/metering_processor.py
```

### Docker Usage
With environment variables in a file:
```bash
# One-time run
docker run --env-file .env your-image-name

# Cron job mode
docker run --env-file .env -e CRON_MODE=true your-image-name
```

### Example Environment File (.env)
```bash
# AWS Configuration
AWS_ACCESS_KEY_ID=your_access_key
AWS_SECRET_ACCESS_KEY=your_secret_key
AWS_REGION=us-east-1
S3_BUCKET_NAME=your-bucket-name

# Clazar Configuration
CLAZAR_CLIENT_ID=your_client_id
CLAZAR_CLIENT_SECRET=your_client_secret

# Service Configuration
SERVICE_NAME=Postgres
ENVIRONMENT_TYPE=PROD
PLAN_ID=pt-HJSv20iWX0

# Optional Configuration
MAX_RETRIES=5
CRON_MODE=true
DRY_RUN=false

# Custom Dimensions (optional)
DIMENSION_pod_hours=cpu_core_hours / 2
DIMENSION_memory_gb_hours=memory_byte_hours / 1073741824
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
          "message": "Error occurred",
          "retry_count": 3,
          "last_retry_time": "2025-07-25T20:15:37Z",
          "payload": {
            "request": [
              {
                "cloud": "aws",
                "contract_id": "ce751fd3-ghi9-6159-dhgf-f4hh778e94bg",
                "dimension": "cpu_core_hours",
                "start_time": "2025-06-01T00:00:00Z",
                "end_time": "2025-06-30T23:59:59Z",
                "quantity": "720"
              }
            ]
          }
        }
      ]
    }
  }
}
```

### Checking Logs
The script provides detailed logging. Monitor the logs for:
- Any errors or warnings
- AWS authentication method being used
- State updates
- Custom dimension calculations
- Retry attempts and backoff delays
- Cron job scheduling information

Example output:
```
2025-07-25 20:15:32,604 - INFO - Using provided AWS credentials for region: us-east-1
2025-07-25 20:15:32,604 - INFO - Loaded custom dimension: pod_hours = cpu_core_hours / 2
2025-07-25 20:15:32,604 - INFO - Processing month 1/12: 2025-06
2025-07-25 20:15:32,604 - INFO - Retrying 2 error contracts for 2025-06
2025-07-25 20:15:32,604 - INFO - Retrying contract ae641bd1-edf8-4038-bfed-d2ff556c729e (retry 2/5) after 4s delay
2025-07-25 20:15:36,604 - INFO - Successfully retried contract ae641bd1-edf8-4038-bfed-d2ff556c729e on attempt 2
2025-07-25 20:15:32,604 - INFO - Processing month: 2025-06 for Postgres/PROD/pt-HJSv20iWX0
2025-07-25 20:15:32,662 - INFO - Found 744 subscription files in omnistrate-metering/Postgres/PROD/pt-HJSv20iWX0/2025/06/
2025-07-25 20:15:32,735 - INFO - Calculating 1 custom dimensions
2025-07-25 20:15:32,735 - INFO - Aggregated 2232 records into 12 entries
2025-07-25 20:15:32,736 - INFO - Filtered from 12 to 6 unprocessed contract records
2025-07-25 20:15:32,736 - INFO - Sending 3 metering records to Clazar for contract ae641bd1-edf8-4038-bfed-d2ff556c729e
2025-07-25 20:15:37,526 - INFO - Successfully sent data to Clazar for contract ae641bd1-edf8-4038-bfed-d2ff556c729e
2025-07-25 20:15:37,526 - INFO - Response: {'results': [{'id': '4a4fefdc-07a9-4b84-a1ee-60c6bb690b12', 'cloud': 'aws', 'contract_id': 'ae641bd1-edf8-4038-bfed-d2ff556c729e', 'dimension': 'cpu_core_hours', 'quantity': '720', 'status': 'success', 'start_time': '2025-06-01T00:00:00Z', 'end_time': '2025-06-30T23:59:59Z', 'custom_properties': {}}]}
2025-07-25 20:15:33,869 - INFO - Saved state to S3: s3://omnistrate-usage-metering-export-demo/metering_state.json
```
