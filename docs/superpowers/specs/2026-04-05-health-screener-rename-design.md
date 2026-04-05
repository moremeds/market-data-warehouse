# Health Check, Universe Screener, and Parquet Rename

**Date:** 2026-04-05
**Status:** Approved

## Context

The market data warehouse has ~2,500 equity tickers plus futures and volatility indices stored as per-ticker bronze parquet files. Three gaps need addressing:

1. **No interior gap detection** — `daily_update.py` only detects tail gaps (latest date vs today). Missing trading days in the middle of a series go unnoticed.
2. **Manual universe management** — The ticker universe is hand-curated via 157 preset JSON files. No dynamic screening based on market cap, volume, and turnover.
3. **Single-timeframe file naming** — Parquet files are named `data.parquet`, which blocks future multi-timeframe support (e.g., `5m.parquet`, `1h.parquet`).

## Implementation Order

**Feature 3 (Rename) → Feature 1 (Health Check) → Feature 2 (Universe Builder)**

Rename first to avoid writing new code against the old filename. Health check stabilizes data integrity before the screener starts auto-adding tickers.

---

## Feature 3: Parquet File Rename (`data.parquet` → `1d.parquet`)

### Approach

Introduce a module-level constant in `bronze_client.py`:

```python
PARQUET_FILENAME = "1d.parquet"
```

Replace all hardcoded `"data.parquet"` references across the codebase with this constant (in code) or the literal `"1d.parquet"` (in tests/docs).

### Files to Modify

**Core code (import and use `PARQUET_FILENAME`):**
- `clients/bronze_client.py` — `_symbol_path()`, `_escaped_glob()`, `get_existing_symbols()`, `_publish_symbol_rows()` temp file prefix
- `clients/db_client.py` — rebuild and export paths (lines 314, 334, 335, 392, 393)
- `scripts/rebuild_duckdb_from_parquet.py` — glob pattern
- `scripts/sync_to_r2.py` — `rglob()` and `endswith()` checks
- `scripts/fetch_cboe_volatility.py` — bronze path construction

**Tests (replace literal strings):**
- `tests/test_bronze_client.py`
- `tests/test_sync_to_r2.py`
- `tests/test_fetch_cboe_volatility.py`
- `tests/test_db_client.py`
- `tests/test_fetch_ib_historical.py`
- `tests/test_storage_client_compat.py`
- `tests/test_rebuild_duckdb_from_parquet.py`

**Docs:**
- `CLAUDE.md`, `README.md`, `docs/observability_defensive_blueprint.md`

**Scripts (additional):**
- `scripts/fetch_ib_historical.py` — any `data.parquet` references

### New File: `scripts/migrate_parquet_filename.py`

Idempotent migration script:
- Walks `~/market-warehouse/data-lake/bronze/` and `bronze-delisted/` recursively
- Renames every `data.parquet` → `1d.parquet`
- Skips if `1d.parquet` already exists
- Supports `--dry-run`
- Prints summary of renamed files

### Rollout

**Order matters: migrate files first, then deploy code.** Deploying code first would make the system blind to all existing `data.parquet` files until migration completes.

1. Stop any running writers (daily update, backfill) to avoid concurrent write conflicts
2. Run migration with `--dry-run` to preview
3. Run migration to rename files on disk
4. Verify: `find ~/market-warehouse/data-lake -name "data.parquet"` (expect empty)
5. Deploy code changes (now code looks for `1d.parquet` which already exists)
6. Run all tests to confirm
7. Run `rebuild_duckdb_from_parquet.py` to confirm DuckDB rebuild works
8. Restart writers

### Conflict Handling

The migration script must be run with **no concurrent writers**. If both `data.parquet` and `1d.parquet` exist in the same symbol directory, the migration aborts with an error (not silent skip) — this indicates a split-brain state that requires manual investigation.

### Risk

R2 sync (if active) still has old `data.parquet` keys. A full re-upload is needed after migration.

---

## Feature 1: Health Check with Auto-Backfill

### New File: `scripts/health_check.py`

### CLI

```bash
python scripts/health_check.py                          # Normal run: detect + backfill + alert (equity default)
python scripts/health_check.py --dry-run                # Report gaps without fixing
python scripts/health_check.py --force                  # Run on non-trading day or re-run same day
python scripts/health_check.py --asset-class futures    # Health check for futures
python scripts/health_check.py --asset-class volatility # Health check for volatility indices
```

Config via env vars (same pattern as rest of system):
- `MDW_IB_HOST` / `MDW_IB_PORT` — IB Gateway endpoint
- `MDW_WAREHOUSE_DIR` — data warehouse root
- `MDW_ALERT_*` — email alert config (existing)
- `MDW_HEALTH_CHECK_EMAIL_THRESHOLD` — min repairs to trigger email (default: 10)

### Core Functions

**`get_all_trade_dates(bronze) -> dict[str, list[date]]`**
Bulk-read all trade dates from bronze parquet via a single DuckDB query:
```sql
SELECT symbol, trade_date FROM read_parquet('<glob>', hive_partitioning=true) ORDER BY symbol, trade_date
```

**`find_interior_gaps(actual_dates, start, end, asset_class) -> list[date]`**
Generate expected trading dates between `min(actual)` and `max(actual)` using NYSE calendar. Diff against actual dates. For futures, skip calendar validation (CME trades on some NYSE holidays).

**`group_contiguous_dates(dates) -> list[tuple[date, date]]`**
Group sorted missing dates into `(start, end)` ranges to minimize IB API calls. A 5-day gap becomes one fetch.

**`compute_range_duration(start_date, end_date) -> str`**
New helper for interior range fetches. Unlike `compute_ib_duration()` (which is tail-only: latest → target), this computes IB duration strings for arbitrary `(start, end)` ranges. Returns the appropriate IB duration string (e.g., `"5 D"`, `"1 M"`).

**`backfill_gaps(symbol, gap_ranges, bronze, ib_client, fallback_client, asset_class)`**
For each contiguous range:
1. Compute IB duration from range via `compute_range_duration()`
2. Fetch via IB with explicit `endDateTime` set to range end (not "now")
3. Validate via `validate_bars`
4. For equities: attempt fallback for remaining misses via `DailyBarFallbackClient`
5. For volatility: attempt repair via CBOE public API (not IB — CBOE is the authoritative source)
6. Merge into bronze via `bronze.merge_ticker_rows()`

**`main()`**
1. Discover all tickers and their trade dates (bulk query)
2. Find interior gaps per ticker
3. If `--dry-run`: report and exit
4. Backfill gaps from IB (+ equity fallback)
5. Log all repairs to `~/market-warehouse/logs/health_check_YYYY-MM-DD.log`
6. If repairs exceed threshold: send email alert via existing Nodemailer system

### Reused Code

| Module | Functions | Modifications |
|--------|-----------|---------------|
| `clients.bronze_client` | `BronzeClient` | None |
| `clients.ib_client` | `IBClient` | None |
| `clients.daily_bar_fallback` | `DailyBarFallbackClient` | None |
| `scripts.daily_update` | `is_trading_day`, `validate_bars`, `bars_to_rows`, `bars_to_futures_rows`, `load_preset`, `trading_days_between` | None |
| `scripts.fetch_cboe_volatility` | CBOE fetch logic | May need to extract a reusable function for single-symbol date-range fetch |

### New Code Required

- `compute_range_duration(start, end)` — IB duration for arbitrary date ranges (the existing `compute_ib_duration` is tail-only)
- Range-based IB fetch wrapper — existing `fetch_ticker_update` has no `end_date` parameter; need a variant that sets `endDateTime` for interior ranges

### Calendar Behavior

| Asset Class | Calendar | Gap Detection | Repair Source |
|-------------|----------|---------------|---------------|
| equity | NYSE | Strict — every NYSE trading day expected | IB → Nasdaq/Stooq fallback |
| volatility | NYSE | Strict — CBOE follows NYSE | CBOE public API (authoritative source, not IB) |
| futures | Relaxed | Only detect gaps on days where adjacent data exists (no NYSE calendar assumption) | IB only (no fallback) |

---

## Feature 2: IB Scanner-Based Universe Builder

### New File: `scripts/universe_screener.py`

### CLI

```bash
python scripts/universe_screener.py            # Normal daily run
python scripts/universe_screener.py --dry-run  # Scan and compare only, no side effects
python scripts/universe_screener.py --force    # Re-run same day
```

Config via env vars / hardcoded defaults:
- Target size: 1000
- Grace days: 3 (only remove after absent N consecutive days)
- Max removals per run: 50 (safety cap)
- Email threshold: 10 changes
- IB host/port: `MDW_IB_HOST` / `MDW_IB_PORT`

### IB Scanner Strategy

IB's `reqScannerData` returns max 50 results per request. To reach ~1000 tickers, run multiple scans across market cap bands. The primary selection criterion is **market cap** (a fundamental), with volume and turnover as secondary filters to ensure liquid, tradeable names.

| Scan Code | Market Cap Band | Purpose |
|-----------|----------------|---------|
| `MOST_ACTIVE_USD` | >$10B | Large cap by $ volume |
| `MOST_ACTIVE_USD` | $2B–$10B | Mid cap by $ volume |
| `MOST_ACTIVE_USD` | $500M–$2B | Small cap by $ volume |
| `MOST_ACTIVE` | >$500M | Share volume supplement |
| `TOP_TRADE_COUNT` | >$500M | High turnover supplement |

Each scan uses:
```python
ScannerSubscription(
    instrument="STK",
    locationCode="STK.US.MAJOR",
    scanCode=scan_code,
    numberOfRows=50,
    marketCapAbove=cap_floor,
    marketCapBelow=cap_ceiling,
    aboveVolume=min_volume,
)
```

Union + deduplicate all results.

### Core Flow

1. **Scan:** Run IB scanner sweeps → union + deduplicate
2. **Compare:** Diff new universe against current bronze (`BronzeClient.get_existing_symbols()`)
3. **Grace period:** Load `~/market-warehouse/logs/screener_state.json` — only mark a ticker for removal after it's been absent for `grace_days` consecutive scans
4. **Additions:** Trigger full historical backfill from IB (reuses `fetch_ib_historical.py` path)
5. **Removals:** Archive to `data-lake/bronze-delisted/asset_class=equity/` via `shutil.move`
6. **Preset:** Write `presets/screened-universe.json`
7. **Log:** Write `~/market-warehouse/logs/universe_changes_YYYY-MM-DD.log`
8. **Alert:** Email if changes exceed threshold

### State File: `~/market-warehouse/logs/screener_state.json`

```json
{
  "last_run": "2026-04-05",
  "absent_counts": {
    "TICKER_X": 2,
    "TICKER_Y": 1
  }
}
```

Tickers present in the latest scan have their absence count reset to 0 (removed from the map). Tickers absent for >= `grace_days` consecutive runs are archived.

### Safeguards

- **Bootstrap mode:** On first run (no `screener_state.json` exists), the screener writes the scanned universe as the initial preset and state file, but does **not** archive any existing bronze tickers. This avoids the deadlock where current universe (~2,500) exceeds target (~1,000) and the max-removals cap (50) blocks convergence. After bootstrap, the grace period begins naturally — tickers not in scans will accumulate absence counts over subsequent daily runs.
- **Max removals cap:** If removals exceed max (default 50) on a non-bootstrap run, abort removals and alert. Prevents catastrophic archiving from scanner API failures.
- **Grace period:** 3 consecutive absences required before removal.
- **Dry-run:** Scans and compares but takes no action.
- **Idempotent:** Running twice on the same day with `--force` produces the same result.

### Generated Preset Format

```json
{
  "name": "screened-universe",
  "description": "Auto-generated: top ~1000 U.S. equities by market cap, volume, and turnover",
  "generated_at": "2026-04-05T13:05:00",
  "tickers": ["AAPL", "MSFT", "NVDA", ...]
}
```

Consumable by `daily_update.py --preset presets/screened-universe.json`.

---

## Testing Strategy

| Feature | Test File | Approach |
|---------|-----------|----------|
| Rename | Existing tests updated | All `data.parquet` → `1d.parquet`; migration script tested with `--dry-run` |
| Health Check | `tests/test_health_check.py` | Mock IB/fallback; `tmp_bronze` fixture; verify gap detection math, contiguous grouping, dry-run safety |
| Universe Builder | `tests/test_universe_screener.py` | Mock `reqScannerData`; `tmp_path` for log/preset/archive; test grace period state, max-removals cap, set comparison |

100% coverage enforced (`fail_under = 100`) for all new code.

---

## Verification Plan

### After Feature 3 (Rename)
1. Run all tests: `python -m pytest tests/ -v --cov=clients --cov=scripts --cov-report=term-missing`
2. Run migration with `--dry-run`
3. Run migration
4. `find ~/market-warehouse/data-lake -name "data.parquet"` → expect empty
5. `python scripts/rebuild_duckdb_from_parquet.py` → confirm DuckDB rebuild works
6. Spot-check: `duckdb ~/market-warehouse/duckdb/market.duckdb "SELECT count(*) FROM md.equities_daily"`

### After Feature 1 (Health Check)
1. Run all tests including new `test_health_check.py`
2. `python scripts/health_check.py --dry-run` → review gap report
3. `python scripts/health_check.py` → verify backfill + log output
4. Check `~/market-warehouse/logs/health_check_YYYY-MM-DD.log`

### After Feature 2 (Universe Builder)
1. Run all tests including new `test_universe_screener.py`
2. `python scripts/universe_screener.py --dry-run` → review scan results and diff
3. `python scripts/universe_screener.py` → verify preset written, backfill triggered, log written
4. Check `presets/screened-universe.json` and `~/market-warehouse/logs/universe_changes_YYYY-MM-DD.log`
