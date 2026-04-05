# Containerized MDW Pipeline with R2 Storage

## Goal

Run the daily market data fetch as a portable Docker Compose stack on any machine (Windows, Linux, macOS). IB Gateway and the MDW job run as two containers. Parquet data syncs to Cloudflare R2 as the system of record — no local persistence needed.

One command: `docker compose up -d`.

## Architecture

```
docker-compose.yml
├── ib-gateway          # gnzsnz/ib-gateway-docker
│   ├── Ports: 4001 (API), 5900 (VNC for 2FA)
│   └── Persistent volume for IB settings
│
└── mdw-job             # Custom image: python:3.13-slim + repo code
    ├── Scheduler: runs daily_update.py at configurable time (default 4:05 PM ET)
    ├── Writes Parquet to /tmp/bronze inside container
    ├── Syncs /tmp/bronze → R2 bucket after each run
    └── Connects to ib-gateway:4001 (Docker network)
```

## MDW Job Container

### Image

- **Base**: `python:3.13-slim`
- **Dependencies**: `ib-async`, `pyarrow`, `duckdb`, `rich`, `boto3`, `requests`, `pandas`
- **Code**: Copy repo's `clients/`, `scripts/`, `presets/` into the image
- **Dockerfile**: Lives at `docker/mdw-job/Dockerfile`

### Scheduler

A lightweight Python scheduler (using the `schedule` library or a simple `while True` + `time.sleep` loop) that:

1. Waits until the configured time (default 16:05 US/Eastern)
2. Runs `daily_update.py` with bronze dir pointing to `/tmp/bronze`
3. After the job completes, runs the R2 sync step
4. Logs to stdout (Docker captures it)

No cron daemon needed — a single Python process handles scheduling and execution.

### Entrypoint

```
python docker/mdw-job/entrypoint.py
```

The entrypoint:
- Reads schedule config from env vars
- Runs the scheduler loop
- Also supports `--now` flag to run immediately (useful for testing)

## R2 Sync

### Approach

BronzeClient writes Parquet to `/tmp/bronze` inside the container (unchanged logic). After `daily_update.py` completes, a sync function uploads changed files to R2.

### Implementation

- Uses `boto3` with R2's S3-compatible endpoint
- Walks `/tmp/bronze/` tree, uploads each `data.parquet` file
- Preserves the Hive-partitioned key structure: `bronze/asset_class=equity/symbol=AAPL/data.parquet`
- Only uploads files modified since last sync (compare local mtime vs S3 LastModified, or just upload all — the files are small)
- New script: `scripts/sync_to_r2.py`

### R2 Credentials (env vars)

```
R2_ENDPOINT_URL=https://<account-id>.r2.cloudflarestorage.com
R2_ACCESS_KEY_ID=<key>
R2_SECRET_ACCESS_KEY=<secret>
R2_BUCKET=market-data
```

## Docker Compose

```yaml
services:
  ib-gateway:
    image: ghcr.io/gnzsnz/ib-gateway:stable
    ports:
      - "127.0.0.1:5900:5900"  # VNC (for 2FA only — loopback, API stays internal)
    healthcheck:
      test: ["CMD-SHELL", "nc -z 127.0.0.1 4003 || exit 1"]
      interval: 10s
      timeout: 5s
      start_period: 120s
      retries: 5
    environment:
      TWS_USERID: ${TWS_USERID}
      TWS_PASSWORD_FILE: /run/secrets/ib_password
      TRADING_MODE: ${TRADING_MODE:-paper}
      READ_ONLY_API: ${READ_ONLY_API:-yes}
    secrets:
      - ib_password
    volumes:
      - ib-settings:/home/ibgateway/Jts
    restart: unless-stopped

  mdw-job:
    build: ./docker/mdw-job
    depends_on:
      ib-gateway:
        condition: service_healthy
    environment:
      MDW_IB_HOST: ib-gateway
      MDW_IB_PORT: 4003              # Container-internal port (not the host-mapped port)
      MDW_SCHEDULE_HOUR: ${MDW_SCHEDULE_HOUR:-16}
      MDW_SCHEDULE_MINUTE: ${MDW_SCHEDULE_MINUTE:-5}
      MDW_SCHEDULE_TZ: ${MDW_SCHEDULE_TZ:-US/Eastern}
      MDW_DATA_LAKE: /tmp/data-lake     # Override hardcoded ~/market-warehouse/data-lake
      R2_ENDPOINT_URL: ${R2_ENDPOINT_URL}
      R2_ACCESS_KEY_ID: ${R2_ACCESS_KEY_ID}
      R2_SECRET_ACCESS_KEY: ${R2_SECRET_ACCESS_KEY}
      R2_BUCKET: ${R2_BUCKET:-market-data}
    restart: unless-stopped

secrets:
  ib_password:
    file: ./secrets/ib_password.txt

volumes:
  ib-settings:
```

## Configuration

All via `.env` file in the compose directory:

```bash
# IB Gateway
TWS_USERID=your_ib_username
TRADING_MODE=paper            # or "live"
READ_ONLY_API=yes

# Schedule (defaults: 4:05 PM Eastern)
MDW_SCHEDULE_HOUR=16
MDW_SCHEDULE_MINUTE=5
MDW_SCHEDULE_TZ=US/Eastern

# Cloudflare R2
R2_ENDPOINT_URL=https://<account-id>.r2.cloudflarestorage.com
R2_ACCESS_KEY_ID=<key>
R2_SECRET_ACCESS_KEY=<secret>
R2_BUCKET=market-data
```

IB password goes in `secrets/ib_password.txt` (not in `.env`).

## File Layout (new files)

```
docker/
├── ib-gateway/              # Existing (unchanged)
└── mdw-job/
    ├── Dockerfile           # python:3.13-slim + deps + repo code
    └── entrypoint.py        # Scheduler + job runner + R2 sync
scripts/
└── sync_to_r2.py            # boto3 upload logic (reusable outside Docker)
```

## Configurable Bronze Directory

`daily_update.py` and `fetch_ib_historical.py` currently hardcode `DATA_LAKE = Path.home() / "market-warehouse" / "data-lake"`. Add support for a `MDW_DATA_LAKE` env var override so the container can point to `/tmp/data-lake`. This is a small change at the top of each script.

## Job Flow (per run)

1. **Download from R2**: `sync_to_r2.py --download` pulls current Parquet snapshots from R2 → `/tmp/data-lake/bronze/` (rehydrates local state for ticker discovery)
2. `daily_update.py` discovers tickers from local bronze Parquet (BronzeClient, unchanged logic)
3. Fetches missing bars from IB Gateway via `ib-async`
4. Writes updated Parquet to `/tmp/data-lake/bronze/` (BronzeClient, unchanged)
5. **Upload to R2**: `sync_to_r2.py --upload` pushes changed Parquet files to R2 bucket — **only if the job exits 0** (no unresolved failures)
6. Logs success/failure to stdout

### Partial Failure Handling

`daily_update.py` currently exits 0 even when some tickers fail. The entrypoint must gate the R2 upload on strict success: if `tickers_failed > 0`, skip the upload and log a warning. Alternatively, modify `daily_update.py` to return non-zero when gaps remain unresolved.

### Bootstrap Problem

On first run, `/tmp/bronze` is empty — `daily_update.py` discovers tickers from bronze Parquet. Two options:
- Run `fetch_ib_historical.py` first with a preset to seed bronze
- The entrypoint supports `--seed --preset presets/sp500.json` to do an initial backfill before starting the scheduler

After the first seed, daily updates are incremental.

### First-run Sequence

```bash
# 1. Start IB Gateway, wait for login/2FA
docker compose up -d ib-gateway

# 2. Seed initial data (one-time)
docker compose run mdw-job --seed --preset presets/sp500.json

# 3. Start the scheduler
docker compose up -d mdw-job
```

## Querying from Elsewhere

Consumers read Parquet directly from R2 using any S3-compatible client:
- Python: `pyarrow.parquet.read_table('s3://market-data/bronze/...')` with R2 endpoint configured
- Any S3 SDK with R2 endpoint configured

DuckDB requires an S3 secret for R2 (Cloudflare endpoint, not AWS):
```sql
CREATE SECRET r2 (TYPE S3, KEY_ID '<key>', SECRET '<secret>',
  ENDPOINT '<account-id>.r2.cloudflarestorage.com', REGION 'auto');
SELECT * FROM read_parquet('s3://market-data/bronze/asset_class=equity/symbol=AAPL/data.parquet');
```

## Asset Class Coverage

The entrypoint runs the same sequence as `run_daily_update_job.py`:
1. Equity daily update via IB
2. Futures daily update via IB
3. CBOE volatility sync via public API (no IB needed)

This matches current production behavior — no asset classes are dropped.

## Seed / Backfill Resilience

Cursor files (`cursor_*.json`) must persist across container restarts so interrupted seeds resume. Options:
- Store cursors in a Docker volume (simplest)
- Upload cursor files to R2 alongside Parquet

The entrypoint should use a volume mount for `/tmp/data-lake` during seed operations.

## Out of Scope

- Failure email alerts (can be added later)
- DuckDB rebuild (consumers query Parquet directly from R2)
- Watchdog container
