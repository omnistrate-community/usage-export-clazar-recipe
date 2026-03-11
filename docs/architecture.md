# Architecture Overview

The Clazar Usage Exporter is a long-running Python service that bridges Omnistrate's usage metering system with the Clazar marketplace billing platform. It reads hourly resource consumption data from AWS S3, aggregates it monthly, transforms it through configurable formulas, and submits it to Clazar for marketplace billing.

## High-Level Architecture

```
┌───────────────────────────────────────────────────┐
│                    main.py                         │
│              (process launcher)                    │
│                                                    │
│   ┌─────────────────┐   ┌──────────────────────┐  │
│   │   Process 1      │   │     Process 2        │  │
│   │   Healthcheck    │   │  Metering Processor   │  │
│   │   Server (:8080) │   │   (continuous loop)   │  │
│   └─────────────────┘   └──────────────────────┘  │
└───────────────────────────────────────────────────┘
                               │
              ┌────────────────┼────────────────┐
              ▼                ▼                ▼
     ┌──────────────┐  ┌────────────┐  ┌─────────────┐
     │  Omnistrate   │  │   State    │  │   Clazar    │
     │   Metering    │  │  Manager   │  │   Client    │
     │   Reader      │  │            │  │             │
     │  (S3 → read)  │  │ (S3 r/w)  │  │  (HTTP)     │
     └──────┬───────┘  └─────┬──────┘  └──────┬──────┘
            │                │                 │
            ▼                ▼                 ▼
      AWS S3 Bucket    AWS S3 Bucket      Clazar API
    (metering data)   (state files)   (api.clazar.io)
```

## Process Model

The application starts as a single Python process (`main.py`) that spawns two child processes using the `multiprocessing` module:

1. **Healthcheck Server** — A minimal HTTP server that exposes a `/health/` endpoint on port 8080 (configurable). It responds with `200 OK` for health probes and `404` for everything else. This lets orchestration platforms (Omnistrate, Kubernetes) verify the service is alive.

2. **Metering Processor** — The core business logic. It runs in a continuous loop with a configurable interval (default: 300 seconds). Each cycle it determines the next month to process, reads usage data from S3, transforms it, and sends it to Clazar.

Graceful shutdown is handled via `SIGTERM` and `SIGINT` signals — both child processes are terminated with a 5-second timeout.

## Components

### Config (`src/config.py`)

Reads all configuration from environment variables at startup. Provides validation methods for AWS credentials, required fields, custom dimension formulas, and the start month format. All other components receive a `Config` instance at initialization.

### OmnistrateMeteringReader (`src/omnistrate_metering_reader.py`)

Responsible for all read operations against the Omnistrate metering data stored in S3. It understands the S3 path conventions, reads the export state file to determine data completeness, and provides methods to list and read individual subscription files.

See [Reading Data from Omnistrate](./reading-data-from-omnistrate.md) for a detailed walkthrough.

### MeteringProcessor (`src/metering_processor.py`)

The orchestrator that ties everything together. It determines which month to process next, reads and aggregates the raw usage records, applies custom dimension formulas, filters already-processed contracts, sends data to Clazar, and handles per-contract error tracking and retries.

### StateManager (`src/state_manager.py`)

Persists processing state to S3 so the exporter can resume across restarts. Tracks which contracts have been successfully exported for each month, which ones failed (with full error details and the original payload for retry), and the last processed month.

### ClazarClient (`src/clazar_client.py`)

HTTP client that handles authentication and metering data submission to the Clazar API. Implements retry logic with exponential backoff for transient failures. Supports a dry-run mode that logs payloads without making real API calls.

See [Publishing Data to Clazar](./publishing-data-to-clazar.md) for a detailed walkthrough.

### HealthcheckServer (`src/healthcheck_server.py`)

A lightweight HTTP server built on Python's `http.server`. Only the `/health/` endpoint is served; all other paths return `404`. Used by the deployment platform for liveness probes.

## Processing Loop

Each cycle of the metering processor follows this sequence:

```
1. Determine the next month to process
   ├─ Read last processed month from state
   ├─ Read latest complete month from Omnistrate
   └─ Calculate next month (or stop if caught up)

2. Retry any previously failed contracts for that month

3. Read all subscription files from S3 for the month

4. Aggregate raw records by (contract_id, dimension)
   └─ Sum values and compute total_price per group

5. Apply custom dimension formulas
   └─ Transform raw dimensions into billing dimensions

6. Filter out already-processed contracts

7. Send each contract's data to Clazar individually
   ├─ On success → mark as processed in state
   └─ On failure → record error with payload for retry

8. Update last processed month on success
```

The processor only advances to the next month when all contracts for the current month are either successfully processed or recorded as errors. It never processes the current month — only fully completed past months.

## S3 Layout

The exporter interacts with two areas of the S3 bucket:

```
s3://{bucket}/
├── omnistrate-metering/                          # Read-only (written by Omnistrate)
│   ├── last_success_export.json                  # Export state with completion timestamps
│   └── {service}/{env}/{plan}/{YYYY}/{MM}/       # Monthly usage data
│       ├── subscription-001.json
│       ├── subscription-002.json
│       └── ...
└── clazar/                                       # Read-write (managed by this exporter)
    └── {service}-{env}-{plan}-export_state.json  # Processing state
```

## External Dependencies

| Dependency | Purpose |
|---|---|
| **AWS S3** (via `boto3`) | Source of metering data, storage for processing state |
| **Clazar API** (`requests`) | Destination for transformed billing records |
| **Python 3.14** | Runtime (uses `multiprocessing`, `http.server`, `collections`) |
