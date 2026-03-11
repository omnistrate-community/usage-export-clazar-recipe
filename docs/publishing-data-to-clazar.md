# Publishing Data to Clazar

This document explains how the exporter authenticates with the Clazar API, sends metering data, handles errors, and tracks state across runs.

## Clazar API Overview

The exporter interacts with the [Clazar API](https://api.clazar.io) to submit usage-based billing records for cloud marketplace contracts. Clazar acts as the metering gateway for AWS, GCP, and Azure marketplaces.

Two endpoints are used:

| Endpoint | Method | Purpose |
|---|---|---|
| `/authenticate/` | POST | Obtain an access token using client credentials |
| `/metering/` | POST | Submit usage metering records |

## Authentication

Before sending any metering data, the exporter authenticates with Clazar using client credentials (`CLAZAR_CLIENT_ID` and `CLAZAR_CLIENT_SECRET`).

```
POST https://api.clazar.io/authenticate/

{
  "client_id": "your-client-id",
  "client_secret": "your-client-secret"
}
```

A successful response returns an access token:

```json
{
  "access_token": "eyJ..."
}
```

This token is stored in memory and used as a `Bearer` token in subsequent API calls. The exporter re-authenticates before each processing cycle to avoid token expiration.

In **dry-run mode**, authentication is skipped entirely and a placeholder token is used.

## Sending Metering Data

The `ClazarClient.send_metering_data()` method (`src/clazar_client.py`) handles the submission of billing records. Each call sends data for a single contract.

### Payload Format

```json
{
  "request": [
    {
      "cloud": "aws",
      "contract_id": "ae641bd1-edf8-4038-bfed-d2ff556c729e",
      "dimension": "replica_hours",
      "start_time": "2025-01-01T00:00:00Z",
      "end_time": "2025-01-31T23:59:59Z",
      "quantity": "720"
    }
  ]
}
```

| Field | Description |
|---|---|
| `cloud` | The marketplace: `aws`, `gcp`, or `azure` |
| `contract_id` | The Clazar contract ID (same as `externalPayerId` in Omnistrate) |
| `dimension` | The billing dimension name (must match a dimension configured in Clazar) |
| `start_time` | Start of the billing period (first day of the month, midnight UTC) |
| `end_time` | End of the billing period (last day of the month, 23:59:59 UTC) |
| `quantity` | The metered quantity as a string of a non-negative integer |

A single contract may have multiple records if multiple custom dimensions are configured. All dimension records for a contract are sent in a single API call.

### Response Handling

A successful response contains a `results` array with one entry per submitted record:

```json
{
  "results": [
    {
      "status": "success"
    }
  ]
}
```

The exporter inspects each result for errors and warnings:

- **Errors** (e.g., invalid contract ID): The contract is marked as failed in the state file. The `errors`, `code`, and `message` fields from the response are preserved.
- **Warnings** (e.g., unrecognized dimension): Logged but not treated as failures. This commonly occurs when the dimension name in the exporter doesn't match the Clazar configuration.

## Per-Contract Processing

The `MeteringProcessor.send_to_clazar()` method sends data contract by contract, not in bulk. This design has several benefits:

1. **Isolation**: A failure for one contract doesn't block others.
2. **Granular tracking**: Each contract's success or failure is recorded independently.
3. **Targeted retries**: Only failed contracts are retried on subsequent runs.

The flow for each contract within a processing cycle:

```
For each contract_id:
  1. Build the metering payload (all dimensions for this contract)
  2. Send to Clazar via POST /metering/
  3. Check the response:
     ├─ No errors → remove from error list (if previously failed)
     │              → add to success list
     │              → mark as processed in state
     └─ Has errors → record in state with:
                     - error details
                     - error code and message
                     - the full original payload
                     - retry count
```

## Retry Logic

The exporter has two layers of retry:

### Layer 1: HTTP-Level Retries (within a single API call)

The `ClazarClient` automatically retries on transient HTTP failures:

| Condition | Retried? | Strategy |
|---|---|---|
| HTTP 5xx (server error) | Yes | Exponential backoff: 2^attempt seconds |
| HTTP 429 (rate limit) | Yes | Exponential backoff: 2^attempt seconds |
| Connection timeout | Yes | Exponential backoff: 2^attempt seconds |
| Connection error | Yes | Exponential backoff: 2^attempt seconds |
| HTTP 4xx (client error, except 429) | No | Fail immediately |

Maximum retries per API call: **5** (configurable via `max_retries` parameter).

### Layer 2: Cross-Cycle Retries (across processing cycles)

When a contract fails to process (after exhausting HTTP retries), it's recorded in the state file under `error_contracts` with the full payload preserved. On the next processing cycle, the exporter will:

1. Load error contracts for the current month from state
2. Filter to those with `retry_count < 5` (max retries)
3. Re-send each one using the preserved payload
4. On success: remove from errors, add to success list
5. On failure: increment retry count, update error details

This means a contract gets up to **5 chances across separate processing cycles** to succeed, in addition to the HTTP-level retries within each attempt.

## State Tracking

The `StateManager` (`src/state_manager.py`) persists all processing state to an S3 file:

```
s3://{bucket}/clazar/{service}-{env}-{plan}-export_state.json
```

### State File Structure

```json
{
  "last_processed_month": "2025-01",
  "last_updated": "2025-02-01T00:15:37Z",
  "success_contracts": {
    "2025-01": [
      "ae641bd1-edf8-4038-bfed-d2ff556c729e",
      "bf752ce2-fee9-5149-cgfe-e3gg667d83af"
    ]
  },
  "error_contracts": {
    "2025-01": [
      {
        "contract_id": "ce751fd3-ghi9-6159-dhgf-f4hh778e94bg",
        "errors": ["Invalid contract ID"],
        "code": "INVALID_CONTRACT",
        "message": "Contract not found",
        "retry_count": 3,
        "last_retry_time": "2025-02-01T00:15:37Z",
        "payload": {
          "request": [
            {
              "cloud": "aws",
              "contract_id": "ce751fd3-ghi9-6159-dhgf-f4hh778e94bg",
              "dimension": "replica_hours",
              "start_time": "2025-01-01T00:00:00Z",
              "end_time": "2025-01-31T23:59:59Z",
              "quantity": "360"
            }
          ]
        }
      }
    ]
  }
}
```

| Field | Description |
|---|---|
| `last_processed_month` | The most recent month that was fully processed (`YYYY-MM`) |
| `last_updated` | ISO 8601 timestamp of the last state update |
| `success_contracts` | Map of month → list of successfully exported contract IDs |
| `error_contracts` | Map of month → list of error entries with full details |

### State Operations

| Operation | When it happens |
|---|---|
| `mark_contract_month_processed` | A contract's data was successfully sent to Clazar |
| `mark_contract_month_error` | A contract's data failed to send (preserves payload for retry) |
| `remove_error_contract` | A previously failed contract succeeds on retry |
| `update_last_processed_month` | All contracts for a month are processed (success or max retries) |
| `is_contract_month_processed` | Checked before sending to avoid duplicate submissions |

## Dry-Run Mode

When `DRY_RUN=true`, the exporter runs the entire pipeline — reading from S3, aggregating, transforming dimensions — but skips actual Clazar API calls. Instead, it:

1. Logs the full payload that would be sent
2. Returns a mock success response
3. Records the contract as "processed" in state

This is useful for:
- Validating configuration and dimension formulas before going live
- Testing the S3 reading pipeline without affecting billing
- Debugging aggregation and transformation logic

## Error Scenarios

| Scenario | Behavior |
|---|---|
| Invalid Clazar credentials | Authentication fails at startup; the service exits with code 1 |
| Invalid contract ID in Clazar | Clazar returns an error; the contract is recorded in error state for retry |
| Unrecognized dimension name | Clazar returns a warning (non-success status); logged but not treated as an error |
| Network failure during send | HTTP-level retries kick in (up to 5 attempts with exponential backoff) |
| Clazar rate limiting (429) | Retried with exponential backoff |
| Duplicate metering record | Clazar may reject it; the error is recorded. On subsequent runs, the contract is skipped if already in `success_contracts` |
| Partial contract failure | Each contract is independent; successful contracts are recorded while failed ones enter the retry queue |
