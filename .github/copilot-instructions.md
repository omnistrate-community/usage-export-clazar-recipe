# Copilot Instructions for usage-export-clazar-recipe

## Project Overview

This is a Python service that bridges Omnistrate's usage metering system with the Clazar marketplace billing platform. It reads hourly resource consumption data from AWS S3, aggregates it monthly, transforms it via configurable formulas, and submits it to the Clazar API for cloud marketplace billing (AWS, GCP, Azure).

For a full understanding of the solution, read these docs:

- [Architecture Overview](docs/architecture.md) — components, process model, S3 layout, processing loop
- [Reading Data from Omnistrate](docs/reading-data-from-omnistrate.md) — S3 conventions, aggregation, dimension transformation
- [Publishing Data to Clazar](docs/publishing-data-to-clazar.md) — Clazar API, retry logic, state tracking, error handling

## Repository Structure

```
src/
├── main.py                        # Entry point — spawns healthcheck + processor processes
├── config.py                      # Reads/validates all environment variables
├── healthcheck_server.py          # HTTP /health/ endpoint (port 8080)
├── clazar_client.py               # Clazar REST client (auth + metering submission)
├── omnistrate_metering_reader.py  # Reads usage data from S3
├── state_manager.py               # Persists processing state to S3
└── metering_processor.py          # Core orchestrator — aggregation, transformation, sending
tests/                             # Unit tests (mocked S3 and HTTP)
integration_tests/                 # Integration tests (live S3 and Clazar API)
docs/                              # Architecture and flow documentation
Dockerfile                         # Python 3.14-slim container
omnistrate-compose.yaml            # Omnistrate service definition with API parameters
Makefile                           # Build, test, Docker, and release targets
clazar_contracts.sh                # Standalone utility to list Clazar contracts/buyers
```

## Tech Stack

- **Python 3.14** (runtime and CI)
- **boto3** — AWS S3 operations (reading metering data, reading/writing state)
- **requests** — HTTP client for Clazar API
- **unittest** — test framework (with `unittest.mock` for mocking)
- **Docker** — containerization (`python:3.14-slim` base)
- **Omnistrate CLI** (`omnistrate-ctl`) — deployment to Omnistrate platform

## Key Design Patterns

### Component Boundaries

Each module in `src/` has a single responsibility. When making changes:

- **Config changes** (new env vars, new validation): edit `src/config.py`. Also update `omnistrate-compose.yaml` to expose new parameters, and update `.env.template`.
- **S3 reading logic** (path conventions, file parsing): edit `src/omnistrate_metering_reader.py`.
- **Clazar API interactions** (new endpoints, payload changes): edit `src/clazar_client.py`.
- **State persistence** (new tracked fields, state file format): edit `src/state_manager.py`.
- **Processing pipeline** (aggregation logic, dimension formulas, processing order): edit `src/metering_processor.py`.
- **Healthcheck changes**: edit `src/healthcheck_server.py`.

Do not mix concerns — e.g., don't put Clazar API logic in the metering processor, or S3 logic in the state manager.

### Dependency Injection

All components receive a `Config` instance at initialization. The `MeteringProcessor` also receives `OmnistrateMeteringReader`, `StateManager`, and `ClazarClient` instances. This makes testing straightforward — mock the dependencies and inject them.

### Per-Contract Processing

Data is sent to Clazar one contract at a time, not in bulk. This enables granular error tracking, independent retries, and prevents one bad contract from blocking others. Preserve this pattern when modifying the sending logic.

### Two-Layer Retry

There are two retry mechanisms — don't confuse them:
1. **HTTP retries** inside `ClazarClient.send_metering_data()` — retries transient network/server errors with exponential backoff within a single API call.
2. **Cross-cycle retries** in `MeteringProcessor.retry_error_contracts()` — re-sends previously failed contracts on subsequent processing cycles using the payload stored in the state file.

### Formula Evaluation

Custom dimension formulas are evaluated using Python's `eval()` with a restricted context (only math builtins). If a formula fails for a contract, the entire contract is skipped. This is intentional — never send partial billing data to Clazar.

## Making Changes

### Adding a New Environment Variable

1. Add the variable to `Config.__init__()` in `src/config.py` with a sensible default.
2. Add validation logic in the appropriate `validate_*` method or create a new one.
3. If the new variable should be settable from the Omnistrate UI, add it to `omnistrate-compose.yaml` under `x-omnistrate-api-params` following the existing patterns. Use `type: Password` for secrets and set `export: false`.
4. Add to `.env.template` with a descriptive placeholder.
5. Add unit tests in `tests/test_config.py` covering the new variable.

### Adding a New Omnistrate Metering Dimension

The set of available dimensions for formulas is defined in `MeteringProcessor.transform_dimensions()` in `src/metering_processor.py`. To add a new dimension:

1. Add it to the `eval_context` dictionary with both the count and `_total_price` variants.
2. Update the docs in `docs/reading-data-from-omnistrate.md` (variable table and examples).
3. Update the `README.md` "Available Variables in Formulas" section.
4. Add test cases in `tests/test_metering_processor.py` for the new dimension.

### Modifying the Clazar API Integration

- The Clazar API base URL is `https://api.clazar.io` (hardcoded in `ClazarClient`).
- Endpoints: `/authenticate/` (POST), `/metering/` (POST).
- Always check for both `errors` and non-success `status` in API responses.
- Respect dry-run mode: when `self.dry_run` is `True`, log the payload and return a mock response instead of making real API calls.
- Keep the retry logic in `send_metering_data()` — don't move it to the processor.

### Modifying the State File Format

The state file is stored in S3 at `clazar/{service}-{env}-{plan}-export_state.json`. When changing the state schema:

1. Maintain backward compatibility — new fields should have defaults so existing state files still load correctly.
2. State is loaded and saved as full JSON documents (not patched). Every `mark_*` or `update_*` method calls `load_state()` first, mutates, then calls `save_state()`.
3. Update integration tests in `integration_tests/test_state_manager.py` which test against a real S3 bucket.

## Running the Project

```bash
# Install dependencies
make build

# Run locally (reads from .env)
make run

# Run in Docker
make docker-run

# Run unit tests
make unit-tests

# Run integration tests (requires real AWS + Clazar credentials)
make integration-tests

# Deploy to Omnistrate
make release
```

## Testing Guidelines

### Unit Tests (`tests/`)

- All external calls (S3, Clazar API) must be mocked using `unittest.mock.patch`.
- Each source module has a corresponding test file: `test_<module>.py`.
- Tests use `unittest.TestCase`. Follow the existing patterns.
- Mock at the boundary — e.g., mock `boto3.client` for S3 calls, mock `requests.post` for HTTP calls.
- Run with: `make unit-tests` or `pytest tests/ -v`.

### Integration Tests (`integration_tests/`)

- These hit real AWS S3 and the real Clazar API. They require valid credentials in `.env_integration`.
- Always include cleanup logic (see `_cleanup_test_state()` patterns in existing tests).
- Run with: `make integration-tests`.

### When to Add Tests

- **Always** add unit tests for new logic or changed behavior.
- Add integration tests when the change affects S3 or Clazar API interactions.
- If modifying aggregation or formula logic, include test cases with real-world-like data.

## Deployment

### Docker

- Base image: `python:3.14-slim`
- The container runs `python3 -u src/main.py` (unbuffered output).
- Port 8080 is exposed for the healthcheck endpoint.
- CI builds multi-arch images (`linux/amd64`, `linux/arm64`) and publishes to `ghcr.io`.

### Omnistrate

- `omnistrate-compose.yaml` defines the service as an Omnistrate-managed resource.
- All user-configurable parameters are declared under `x-omnistrate-api-params`.
- The `make release` target substitutes the image version and runs `omnistrate-ctl build`.
- Compute defaults: single replica on small instances (AWS `t3a.small`, GCP `e2-small`, Azure `Standard_B1s`).

## CI/CD

- **`build.yml`**: Runs unit tests with pytest on push/PR to `main`. Auto-bumps version tags on merge.
- **`package.yml`**: Builds Docker images on push/PR. On release, pushes multi-arch images to GHCR and signs with cosign.

## Common Pitfalls

- **Never process the current month.** The exporter only processes months that Omnistrate has fully exported (signaled by `last_success_export.json`).
- **Don't send partial contract data.** If any dimension formula fails for a contract, skip the entire contract.
- **Preserve the original payload in error state.** When a contract fails, the exact request payload is saved so it can be retried identically.
- **Quantities must be non-negative integer strings.** Clazar expects `"quantity": "720"`, not `720` or `"720.5"`.
- **Dimension names must match Clazar configuration.** Mismatched names cause warnings in the API response.
- **Subscription cancellation is not handled.** Usage data for cancelled subscriptions must be manually uploaded to Clazar before the marketplace grace period expires.
