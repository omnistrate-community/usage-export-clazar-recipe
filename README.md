# S3 to Clazar Usage Exporter

This exporter automatically pulls usage metering data from S3 and uploads aggregated data to Clazar on a monthly basis. It is designed to run as a cron job, processing usage data for the previous month and sending it to Clazar for billing purposes.

## Prerequisites

### Configure Clazar Contract IDs as External Payer IDs in Omnistrate

To obtain the contract IDs of your buyers, you can use the included `clazar_contracts.sh` script. The instructions to use this script are included in the script file itself.

In Omnistrate UI, navigate to *"FinOps Center > Tenant Pricing -> Modify Tenant Pricing"* and set the `External Payer ID` field to the corresponding Clazar contract ID. When set, this value will be included in the exported metering data under the `externalPayerId` field. This allows the exporter to correctly associate usage data with the appropriate Clazar contracts.

### AWS Permissions

Your AWS credentials need the following S3 permissions:

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "s3:ListBucket"
            ],
            "Resource": [
                "arn:aws:s3:::your-bucket-name"
            ]
        }, 
        {
            "Effect": "Allow",
            "Action": [
                "s3:GetObject"
            ],
            "Resource": [
                "arn:aws:s3:::your-bucket-name/*"
            ]
        },
        {
            "Effect": "Allow",
            "Action": [
                "s3:PutObject"
            ],
            "Resource": [
                "arn:aws:s3:::your-bucket-name/clazar/*"
            ]
        }
    ]
}
```

Note: `s3:PutObject` permission is required for storing the state file in S3.

## Deploy in Omnistrate

To deploy the Clazar Exporter in Omnistrate, follow these steps:

1. Fork the repository, clone it, and navigate to the repository directory.

2. Build the exporter into a service using the Omnistrate CLI. Ensure you have the Omnistrate CLI (`omctl`) installed and logged in. Make sure you have docker installed and running.

```sbash
make release
```

or 

```bash
omnistrate-ctl build -f omnistrate-compose.yaml --product-name "Clazar exporter"  --environment DEV --environment-type DEV --release-as-preferred
```

3. To run the job, create a resource instance in Omnistrate with below parameters:

- `AWS_ACCESS_KEY_ID`: AWS access key ID
- `AWS_SECRET_ACCESS_KEY`: AWS secret access key
- `AWS_REGION`: AWS region (default: uses AWS default)
- `AWS_S3_BUCKET_NAME`: S3 bucket name
- `CLAZAR_CLIENT_ID`: Clazar client ID for authentication
- `CLAZAR_CLIENT_SECRET`: Clazar client secret for authentication
- `CLAZAR_CLOUD`: Cloud Provider Marketplace (e.g., "aws", "gcp", "azure")
- `SERVICE_NAME`: Name of the service (e.g., "Postgres")
- `ENVIRONMENT_TYPE`: Environment type (e.g., "PROD", "DEV")
- `PLAN_ID`: Plan ID (e.g., "pt-HJSv20iWX0")
- `START_MONTH`: Start month for processing (format: YYYY-MM, default: "2025-06")
- `DRY_RUN`: Set to "true" to run without sending data to Clazar (default: "false")
- `DIMENSION1_NAME`:  Dimension name. Name of the Dimension configured on Clazar.
- `DIMENSION1_FORMULA`: Formula of the metric to export, for example `replica_hours`. Refer to the [Custom Dimensions](#custom-dimensions) section for details.
Optional: additional dimensions and formulas to export to Clazar
- `DIMENSION2_NAME`, `DIMENSION2_FORMULA` (Optional): Second custom dimension name and formula (left empty if not needed)
- `DIMENSION3_NAME`, `DIMENSION3_FORMULA` (Optional): Third custom dimension name and formula (left empty if not needed)

## Custom Dimensions

**Example Usage:**

- `DIMENSION1_NAME`: "cpu_core_hours"
- `DIMENSION1_FORMULA`: "cpu_core_hours"

This would create a custom dimension called "cpu_core_hours" that simply passes through the value from the source data.

**Available Variables in Formulas:**

- `memory_byte_hours`: Memory usage in byte-hours
- `storage_allocated_byte_hours`: Storage usage in byte-hours
- `cpu_core_hours`: CPU core usage in core-hours
- `replica_hours`: Replica usage in replica-hours
- `memory_byte_hours_price_per_unit`: Price per unit for the memory byte hour dimension
- `storage_allocated_byte_hours_price_per_unit`:  Price per unit for the storage usage in byte-hours hour dimension
- `cpu_core_hours_price_per_unit`:  Price per unit for the CPU core usage hour dimension
- `replica_hours_price_per_unit`:  Price per unit for the replica usage hour dimension

**Formula Rules:**

- Both name and formula must be provided together for each dimension
- Dimension names must be unique across all custom dimensions
- Formulas can use basic arithmetic operations (+, -, *, /, //, %, **)
- Formulas can use functions: abs, min, max, round, int, float
- If any formula fails to evaluate, the entire contract's data for that month will be skipped
- Formulas must evaluate to non-negative integers
- Combined dimensions can be used to create more complex calculations, such as:
  - `DIMENSION1_NAME`: "total_compute_units"
  - `DIMENSION1_FORMULA`: "cpu_core_hours + memory_byte_hours / 1024 ** 3"

**Note:** The dimension names should match your configured Clazar dimensions. Otherwise, Clazar will not recognize them.

**Controlling Price from Omnistrate:**

To make use of the Price Per Unit definition in Omnistrate, a way to configure the formula is to set the dimension price to a fixed value of 0.01 in the Marketplace and then define a formula as follows:

- `DIMENSION1_NAME`: "marketplace_metric"
- `DIMENSION1_FORMULA`: "replica_hours * replica_hours_price_per_unit / 0.01"

## Job Behavior

### Periodic Execution

- The metering processor is configured to run as a cron job every 5 minutes. The first run happens at the 5th minute after the job starts, and subsequent runs occur every 5 minutes thereafter. You can adjust the frequency in the `entrypoint.sh` file if needed. 
- It will start processing from the month specified in the `START_MONTH` parameter.
- It checks if the complete usage data is available for the entire month. If so, it processes the data and sends it to Clazar. If not, it waits until the next run to check again.
- After successful export, it updates the state file in S3 with the last processed month and the contracts that were successfully processed.
- If a previous run is still executing, the new cron job will be skipped to prevent overlapping executions.
- The job **never processes the current or future months**—it only processes months that are fully in the past.

### Error Handling

- **Automatic retries**: When a contract fails to process for a given month (for example, due to a Clazar API error), the script automatically retries up to 5 times with exponential backoff (2^attempt seconds).
- **Error tracking**: Failed contracts and their error details are recorded in the state file under `error_contracts` for that service/month/contract.
- **Payload preservation**: The exact payload that failed is stored in the state file, so you can see the usage values and manually add them in Clazar if needed.
- **Retry on subsequent runs**: Error contracts are automatically retried on subsequent runs until they succeed or reach the maximum retry limit.
- **Per-contract processing**: Each contract is processed individually, so one failing contract doesn't block others from being processed successfully.

### Subscription Cancellation Is Not Handled

Please note that the script does not handle subscription cancellations. If a subscription is canceled, you will need to manually upload the usage data for that contract and month to Clazar in time. Every marketplace has a grace period for submitting usage data after a subscription ends, so ensure you are aware of those deadlines.

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
                "dimension": "pod_hours",
                "start_time": "2025-06-01T00:00:00Z",
                "end_time": "2025-06-30T23:59:59Z",
                "quantity": "360"
              }
            ]
          }
        }
      ]
    }
  }
}
```

### Checking Live Logs

The script provides detailed logging. You can monitor the live logs in the SaaS Portal under the resource instance you created. 
![Live Logs](images/live-logs.png)
