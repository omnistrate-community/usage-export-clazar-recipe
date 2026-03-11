# Reading Data from Omnistrate

This document explains how the exporter reads and processes usage metering data that Omnistrate writes to AWS S3.

## How Omnistrate Writes Metering Data

Omnistrate continuously tracks resource consumption (CPU, memory, storage, replicas) for every active subscription. This data is exported to an S3 bucket at regular intervals, organized by service, environment, plan, and time period.

### S3 Path Convention

```
s3://{bucket}/omnistrate-metering/{service}/{env}/{plan}/{YYYY}/{MM}/
```

For example, a Postgres service in production with plan `pt-HJSv20iWX0` would have its January 2025 data at:

```
s3://{bucket}/omnistrate-metering/Postgres/PROD/pt-HJSv20iWX0/2025/01/
```

Each file in this directory is a JSON file containing an array of usage records for a subscription.

### Usage Record Format

Each record within a subscription file contains:

```json
{
  "externalPayerId": "ae641bd1-edf8-4038-bfed-d2ff556c729e",
  "dimension": "cpu_core_hours",
  "value": 24,
  "pricePerUnit": 0.05,
  "timestamp": "2025-01-15T12:00:00Z"
}
```

| Field | Description |
|---|---|
| `externalPayerId` | The Clazar contract ID, configured in Omnistrate's FinOps Center |
| `dimension` | The type of resource being metered (e.g., `cpu_core_hours`, `memory_byte_hours`, `replica_hours`, `storage_allocated_byte_hours`) |
| `value` | The quantity consumed in this record |
| `pricePerUnit` | The unit price configured in Omnistrate for this dimension |
| `timestamp` | When this usage was recorded |

### Export Completion Signal

Omnistrate writes a state file at a well-known path to indicate which months have complete data:

```
s3://{bucket}/omnistrate-metering/last_success_export.json
```

This file contains per-service-key entries with a `last_processed_to` timestamp:

```json
{
  "Postgres:PROD:pt-HJSv20iWX0": {
    "last_processed_to": "2025-01-31T23:59:59Z"
  }
}
```

The exporter uses this to determine the latest month for which all data has been fully exported. It will **never** process a month that Omnistrate hasn't finished exporting — this guarantees data completeness.

## The Reading Pipeline

The `OmnistrateMeteringReader` class (`src/omnistrate_metering_reader.py`) handles all S3 read operations. Here's the step-by-step flow:

### Step 1: Check Data Availability

```python
reader.load_usage_data_state()
```

Reads `last_success_export.json` from S3. This returns a dictionary keyed by service identifiers (e.g., `Postgres:PROD:pt-HJSv20iWX0`), each containing a `last_processed_to` timestamp.

If the file doesn't exist or the service key isn't found, the reader returns `None`, and the processor skips this cycle.

### Step 2: Determine the Latest Complete Month

```python
reader.get_latest_month_with_complete_usage_data()
```

Parses the `last_processed_to` ISO 8601 timestamp from the state file and extracts the year and month. For example, `"2025-01-31T23:59:59Z"` yields `(2025, 1)`.

This is compared against the exporter's own state (last processed month) to decide whether there's a new month to process.

### Step 3: List Subscription Files

```python
reader.list_monthly_subscription_files(year=2025, month=1)
```

Constructs the S3 prefix for the target month:

```
omnistrate-metering/Postgres/PROD/pt-HJSv20iWX0/2025/01/
```

Uses the S3 paginator to list all `.json` files under this prefix. Returns a list of S3 object keys.

### Step 4: Read Each File

```python
reader.read_s3_json_file(key)
```

For each subscription file, downloads and parses the JSON content. Returns a list of usage records. Errors (network issues, malformed JSON) are logged and the file is skipped — other files continue processing.

## Aggregation

Once all records are collected, the `MeteringProcessor.aggregate_usage_data()` method groups them by `(externalPayerId, dimension)` and computes two running totals:

- **count**: The sum of all `value` fields
- **total_price**: The sum of `value × pricePerUnit` for each record

```python
# Input: individual hourly records
[
  {"externalPayerId": "abc", "dimension": "cpu_core_hours", "value": 4, "pricePerUnit": 0.05},
  {"externalPayerId": "abc", "dimension": "cpu_core_hours", "value": 4, "pricePerUnit": 0.05},
  {"externalPayerId": "abc", "dimension": "replica_hours",  "value": 1, "pricePerUnit": 0.10},
]

# Output: aggregated monthly totals
{
  ("abc", "cpu_core_hours"): (8, 0.40),   # (count, total_price)
  ("abc", "replica_hours"):  (1, 0.10),
}
```

Records missing `externalPayerId` or `dimension` are skipped with a warning.

## Dimension Transformation

After aggregation, custom dimension formulas are applied via `MeteringProcessor.transform_dimensions()`. This step converts raw Omnistrate dimensions into the billing dimensions configured in Clazar.

For each contract, the formula is evaluated with these available variables:

| Variable | Source |
|---|---|
| `cpu_core_hours` | Aggregated count for this dimension |
| `cpu_core_hours_total_price` | Aggregated total price for this dimension |
| `memory_byte_hours` | Aggregated count for this dimension |
| `memory_byte_hours_total_price` | Aggregated total price for this dimension |
| `storage_allocated_byte_hours` | Aggregated count for this dimension |
| `storage_allocated_byte_hours_total_price` | Aggregated total price for this dimension |
| `replica_hours` | Aggregated count for this dimension |
| `replica_hours_total_price` | Aggregated total price for this dimension |

Formulas support arithmetic operators (`+`, `-`, `*`, `/`, `//`, `%`, `**`) and math functions (`abs`, `min`, `max`, `round`, `int`, `float`). They are evaluated in a restricted context with no access to builtins beyond math functions.

### Example Transformations

**Simple pass-through:**
```
DIMENSION1_NAME=replica_hours
DIMENSION1_FORMULA=replica_hours
```

**Price-based billing (using Omnistrate-defined prices):**
```
DIMENSION1_NAME=marketplace_metric
DIMENSION1_FORMULA=replica_hours_total_price / 0.01
```
This divides the total price by the marketplace unit price (e.g., $0.01) to derive the quantity to report.

**Composite dimension:**
```
DIMENSION1_NAME=total_compute_units
DIMENSION1_FORMULA=cpu_core_hours + memory_byte_hours / 1024 ** 3
```

### Safety

If any formula fails to evaluate for a contract (syntax error, division by zero, negative result), the **entire contract's data is skipped** for that month. This prevents sending partial or incorrect billing data to Clazar. The error is logged for investigation.

## Filtering

Before sending data to Clazar, the processor checks the state file to skip contracts that were already successfully exported for the target month. This ensures idempotency — restarting the exporter won't duplicate billing records.

```python
processor.filter_success_contracts(transformed_data, year, month)
```

Contracts are only removed from the "to send" list if they appear in the `success_contracts` section of the state file for that month.

## Error Resilience

The reading pipeline is designed to be fault-tolerant:

- **Missing state file**: Treated as "no data available yet" — the processor waits for the next cycle.
- **Missing subscription files**: An empty list is returned; the processor skips the month gracefully.
- **Malformed JSON**: The individual file is skipped; other files are still processed.
- **S3 access errors**: Logged and the file is skipped; the processor continues with available data.
- **Formula evaluation errors**: The affected contract is skipped entirely; other contracts proceed normally.
