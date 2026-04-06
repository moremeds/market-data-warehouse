# Multi-Timeframe Bars and Core ETF List

**Date:** 2026-04-06
**Status:** Approved (revised after Codex review)

## Context

Today the warehouse stores only daily (`1d`) bars per ticker. Three use cases need finer granularity:

1. **Backtesting** — `1d` is fine for daily strategies, but swing strategies want hourly entry timing
2. **Intraday signals** — opening range, VWAP fades, gap-and-go need bars within the trading session
3. **Options analytics** — dealer gamma, IV surface, hedging analysis need hourly underlying spot

The recent rename from `data.parquet` → `1d.parquet` was the prerequisite for this. The filename now encodes the timeframe; we add `1h.parquet` and `5m.parquet` alongside it.

A second gap: the screener-driven universe is reactive to market activity, but we want a stable core of ETFs (broad market, sectors, international, vol products) **always** included regardless of scanner output.

## Goals

1. Add `1h` and `5m` bar storage alongside `1d` for the equity universe
2. Add a guaranteed-include core ETF list, unioned into the screened universe
3. Unify timestamp handling on **UTC with timezone awareness** to prevent DST bugs
4. Do not regress the existing daily-bar pipeline. The daily client stays exactly as it is.

## Non-Goals

- Sub-minute bars
- Adjusted close for intraday (IB TRADES data isn't adjusted)
- Continuous (tick) data
- Multi-timeframe for futures / volatility (equity only)
- Realtime / streaming bars (snapshot job is a future thing)

---

## 1. Architecture: Separate Intraday Client

**This was the key change after review.** The existing `BronzeClient` is `trade_date`-shaped end-to-end — paths, reads, aggregates, merge keys, validation, normalization. Forcing `(asset_class, timeframe)` into one class is high-risk and pollutes the daily code paths.

**Decision:** Add a parallel `IntradayBronzeClient` for `1h` and `5m`. The existing `BronzeClient` keeps its current shape and only handles daily.

### Two parallel clients

```
clients/
├── bronze_client.py           # existing — DAILY ONLY (1d, futures, volatility)
└── intraday_bronze_client.py  # NEW — 1h and 5m equity only
```

| Concern | `BronzeClient` (existing) | `IntradayBronzeClient` (new) |
|---------|--------------------------|-----------------------------|
| Time column | `trade_date DATE` | `bar_timestamp TIMESTAMPTZ` (UTC) |
| Merge key | `(symbol, trade_date)` | `(symbol, bar_timestamp)` |
| Path | `symbol={X}/1d.parquet` | `symbol={X}/{1h,5m}.parquet` |
| Asset classes | `equity`, `volatility`, `futures` | `equity` only |
| Constructor | `BronzeClient(asset_class=...)` | `IntradayBronzeClient(timeframe=...)` |
| Schema profiles | `_SCHEMA_PROFILES` (existing) | `_INTRADAY_SCHEMA_PROFILES` (new module-level dict) |

### What's shared

A new module `clients/parquet_io.py` extracts the common atomic-write helper (`_publish`, `_validate_parquet_file`) so both clients use the same on-disk publish semantics. This is the only refactor of existing code.

### What's NOT changing in `BronzeClient`

- `_symbol_path()`, `get_existing_symbols()`, `get_latest_dates()` — all unchanged
- The `PARQUET_FILENAME = "1d.parquet"` constant stays
- `_SCHEMA_PROFILES` stays as-is
- All existing tests pass without modification

---

## 2. Storage Layout

Sibling parquet files per symbol:

```
~/market-warehouse/data-lake/bronze/asset_class=equity/
└── symbol=AAPL/
    ├── 1d.parquet          # written by BronzeClient
    ├── 1h.parquet          # written by IntradayBronzeClient(timeframe="1h")
    └── 5m.parquet          # written by IntradayBronzeClient(timeframe="5m")
```

Three independent files, three independent migration timelines, three independent failure modes.

---

## 3. Timeframe Configuration

Constants live in **`clients/intraday_bronze_client.py`** (NOT `bronze_client.py`):

```python
INTRADAY_TIMEFRAMES = ("1h", "5m")  # iteration order

# IB max history per single request — used by chunking logic
INTRADAY_MAX_REQUEST_DURATION = {
    "1h": "1 M",   # IB allows up to 1 month per 1-hour request
    "5m": "1 W",   # IB allows up to ~1 week per 5-min request
}

# Max realistic backfill depth
INTRADAY_MAX_DEPTH = {
    "1h": "2 Y",   # IB stores ~2 years of 1h data
    "5m": "1 Y",   # IB stores ~1 year of 5m data
}

# IB bar size strings
INTRADAY_IB_BAR_SIZE = {
    "1h": "1 hour",
    "5m": "5 mins",
}

# Parquet filename per timeframe
INTRADAY_PARQUET_FILENAME = {
    "1h": "1h.parquet",
    "5m": "5m.parquet",
}
```

`ALL_TIMEFRAMES = ("1d",) + INTRADAY_TIMEFRAMES` for scripts that iterate over all three.

---

## 4. Schema (intraday only)

```python
_INTRADAY_SCHEMA = pa.schema([
    ("bar_timestamp", pa.timestamp("us", tz="UTC")),
    ("symbol_id",     pa.int64()),
    ("open",          pa.float64()),
    ("high",          pa.float64()),
    ("low",           pa.float64()),
    ("close",         pa.float64()),
    ("volume",        pa.int64()),
])
```

No `adj_close` (TRADES data unadjusted), no `trade_date` (derivable from `bar_timestamp` at query time via `bar_timestamp::DATE AT TIME ZONE 'America/New_York'`).

`bar_timestamp` is the **start of the bar** (matches IB's default), stored as UTC. See § 6 for the full convention.

---

## 5. Empirical Verification Required Before Implementation

**Before writing any code**, the implementer must run a small probe script against the live IB Gateway to verify the following empirical questions. Results go into the implementation plan as documented constants, not assumptions.

### Probe script: `scripts/probe_ib_intraday.py`

Connects to IB, requests 1 day of `5 mins` and `1 hour` bars for AAPL, prints:

1. **`type(bar.date)`** — is it `datetime`, `date`, or `str`?
2. **`bar.date.tzinfo`** — naive or tz-aware? if tz-aware, which zone?
3. **First and last bar timestamps** for one trading day at each timeframe
4. **Behavior with `formatDate=1` vs `formatDate=2`** (string vs Unix epoch UTC)
5. **`useRTH=True` vs `useRTH=False`** — bar count difference for one day
6. **What happens at DST transition** — request bars covering the spring-forward day, verify no duplicate or missing 5m bars at 02:00–03:00 ET

The implementer locks in `formatDate` and `useRTH` based on these results. The spec **does not assume** which gives the cleanest UTC handling — the probe decides.

**Why this matters:** The current `daily_update.py` stringifies `bar.date` and the daily code never deals with tz-aware datetimes. Intraday is different and the IB return type is the foundation everything else depends on.

---

## 6. Timestamp & Timezone Convention

**Universal rule: All intraday bar timestamps stored as UTC `TIMESTAMPTZ`.**

| Layer | Type | Example |
|-------|------|---------|
| Parquet | `pa.timestamp("us", tz="UTC")` | `2026-04-04 13:30:00+00:00` |
| DuckDB | `TIMESTAMPTZ` | TZ-aware natively |
| Python | `datetime` with `tzinfo=ZoneInfo("UTC")` | Never naive |

### Bar timestamp convention

**Start-of-bar** timestamps. The first 5m bar of a regular ET trading day (9:30 ET) is stored as either `13:30:00Z` (winter, EST = UTC-5) or `14:30:00Z` (summer, EDT = UTC-4). The UTC value shifts by 1 hour twice a year — that is *correct*.

### Conversion rules

| Boundary | Rule |
|----------|------|
| **IB → bronze** | Whatever IB returns is normalized to a tz-aware UTC `datetime` at the fetch boundary, before any storage code touches it. The exact normalization depends on the probe results (§ 5). |
| **Bronze → query** | Stored as UTC. Convert to ET in SELECT: `bar_timestamp AT TIME ZONE 'America/New_York'`. |
| **Health check expected timestamps** | Build expected bars in ET (using NYSE calendar), convert each to UTC for comparison with stored bars. |

### Validation

`IntradayBronzeClient` validates incoming rows: any naive datetime raises `ValueError("bar_timestamp must be tz-aware")`. No silent coercion — if the fetcher passes a naive datetime, it's a bug and should fail loudly.

### Daily client unaffected

`BronzeClient` keeps `trade_date DATE` (no time, no timezone). The daily-only path is never modified.

---

## 7. Daily Update Flow (for intraday)

**This was the second key change after review.** The existing daily flow assumes one bar per day. Intraday is fundamentally different — bars stream in throughout the trading session, and "today" is partial until the close.

### New script: `scripts/intraday_update.py`

Separate from `scripts/daily_update.py`. Does not share `main()` or CLI flags. Only iterates over equity symbols, only fetches intraday timeframes.

### Session model

Three states for any (symbol, timeframe) pair on a given trading day:

| State | Definition | Action |
|-------|-----------|--------|
| `complete` | Today's session has closed (16:00 ET passed) AND latest stored bar timestamp = expected last bar of day | skip |
| `in_progress` | Today's session has closed but latest stored bar < expected last bar | fetch to fill |
| `live` | Now is during today's session (9:30–16:00 ET) | fetch all bars from latest_stored to most recent complete bar (skip the partial in-progress bar) |
| `tail_gap` | Today not yet complete and no bars from this trading day exist | fetch from latest stored timestamp forward |
| `historical` | Latest stored bar is more than 1 trading day old | fetch from latest stored forward, possibly multiple days |

### Execution

`intraday_update.py` runs after `daily_update.py` completes (chained in the entrypoint or via `make` target). Same once-a-day cadence by default. The "live mode" only matters if the script is invoked during market hours, and the design is defensive: an in-progress bar is never written to bronze (we wait for it to close).

### Last bar of day rule

The "expected last bar" of a trading day depends on the close time (full day = 16:00 ET, early close = 13:00 ET). For 5m bars on a full day, the last bar is `15:55 ET`. For 1h bars on a full day, the last bar is `15:30 ET` (start of the 15:30–16:30 hour).

The half-day calendar (§ 8) provides the close time per trading day.

### CLI

```bash
python scripts/intraday_update.py                    # all equity symbols, all intraday timeframes
python scripts/intraday_update.py --dry-run          # report gaps, no fetches
python scripts/intraday_update.py --force            # run on non-trading day
python scripts/intraday_update.py --timeframe 1h     # only 1h (manual / debug)
python scripts/intraday_update.py --timeframe 5m     # only 5m
```

---

## 8. NYSE Calendar Extensions

Two new helpers in `scripts/daily_update.py` (alongside existing `is_trading_day`, `get_nyse_holidays`):

```python
def get_early_close_days(year: int) -> dict[date, time]:
    """Return {date: close_time_ET} for half-day trading days.

    Day after Thanksgiving — closes 13:00 ET
    Christmas Eve (if trading day) — closes 13:00 ET
    July 3 (if Independence Day on weekday) — closes 13:00 ET
    """

def session_close_time(d: date) -> time:
    """Return the ET close time for trading day *d*. 16:00 normally, 13:00 on early-close days."""
```

Same Pure-Python pattern as `get_nyse_holidays(year)`, no new dependencies. ~3-4 dates per year. Tested for years 2020–2030.

---

## 9. Health Check

### Daily timeframe

**Unchanged.** Existing `health_check.py` continues to detect interior date gaps in `1d.parquet` and auto-repairs.

### Intraday timeframes

Add a new mode: `python scripts/health_check.py --intraday`

**Critical change from original spec:** Intraday health checks are **report-only by default**. They do NOT auto-repair, because:

1. Trading halts produce missing bars that we can't distinguish from gaps
2. IB historical depth rolls (5m only available ~1 year back), so old gaps are unfixable
3. Auto-refetch could mask real data quality issues

To actually repair intraday gaps, the operator runs:
```bash
python scripts/health_check.py --intraday --repair --timeframe 5m --symbol AAPL --since 2026-03-01
```

Targeted, narrow, explicit.

### Expected bar generation (intraday)

For each trading day in a symbol's date range:
1. Skip non-trading days (existing `is_trading_day`)
2. Determine close time via `session_close_time(d)` (full day = 16:00, early = 13:00)
3. Generate expected bar timestamps from 9:30 ET to (close - bar_size), inclusive
4. Convert each ET timestamp to UTC for comparison with stored bars

### Halt awareness (best effort)

A trading halt produces a "gap" of missing bars. We do NOT try to detect or auto-flag these. The intraday health check report includes a "suspected halt" annotation when a gap is short (<30 min) and surrounded by normal bars — for human review only, no action taken.

---

## 10. Core ETF List

New file: `presets/core-etfs.json`

```json
{
  "name": "core-etfs",
  "description": "Always-include ETFs — added to the screened universe regardless of scanner output. Covers broad market, sectors, international, commodities, bonds, vol, leveraged, and crypto for backtesting, intraday signals, and options analytics.",
  "groups": {
    "broad_market":    {"description": "U.S. broad market index ETFs",                  "tickers": ["SPY", "QQQ", "IWM", "DIA", "VTI"]},
    "sectors_spdr":    {"description": "All 11 SPDR sector ETFs",                       "tickers": ["XLF", "XLE", "XLK", "XLV", "XLI", "XLP", "XLY", "XLU", "XLB", "XLRE", "XLC"]},
    "industry":        {"description": "Key industry ETFs not in SPDR sectors",         "tickers": ["SMH"]},
    "international":   {"description": "Country and regional ETFs",                     "tickers": ["EFA", "EEM", "EWJ", "EWY", "EWZ", "FXI", "KWEB"]},
    "metals":          {"description": "Precious metals — gold and silver",             "tickers": ["GLD", "SLV"]},
    "commodities":     {"description": "Energy and ag commodities — macro exposure",    "tickers": ["USO"]},
    "bonds":           {"description": "Treasury, IG, HY, aggregate — rates and credit","tickers": ["TLT", "IEF", "HYG", "LQD", "AGG"]},
    "volatility":      {"description": "VIX-tracking ETFs — options analytics",         "tickers": ["VXX", "UVXY"]},
    "leveraged":       {"description": "Leveraged broad market — intraday signals",     "tickers": ["TQQQ", "SQQQ"]},
    "crypto":          {"description": "Spot crypto ETFs",                              "tickers": ["IBIT"]}
  },
  "tickers": [
    "SPY", "QQQ", "IWM", "DIA", "VTI",
    "XLF", "XLE", "XLK", "XLV", "XLI", "XLP", "XLY", "XLU", "XLB", "XLRE", "XLC",
    "SMH",
    "EFA", "EEM", "EWJ", "EWY", "EWZ", "FXI", "KWEB",
    "GLD", "SLV",
    "USO",
    "TLT", "IEF", "HYG", "LQD", "AGG",
    "VXX", "UVXY",
    "TQQQ", "SQQQ",
    "IBIT"
  ]
}
```

The flat `tickers` array matches the existing preset format so `load_preset()` works unchanged.

### Screener integration (corrected)

**This was the third key change after review.** Core ETFs must bypass the absent-counts logic entirely, not just be unioned at the end.

Current screener flow (broken for core ETFs):
```
scanned = run_scanner_sweeps()
current = bronze.get_existing_symbols()
additions, removals = compare_universes(current, scanned)
absent_counts = update_absent_counts(absent_counts, removals, scanned)  # core ETFs ticked up here!
```

Corrected flow:
```python
core_etf_tickers = load_core_etfs()                       # frozen set
scanned = run_scanner_sweeps()
universe_after_core = scanned | core_etf_tickers          # union BEFORE the comparison
current = bronze.get_existing_symbols()

# Now exclude core ETFs from BOTH sides of the comparison so they're never marked absent
current_excl_core = current - core_etf_tickers
universe_excl_core = universe_after_core - core_etf_tickers

additions = universe_after_core - current                 # may include core ETFs not yet in bronze
removals_candidates = current_excl_core - universe_excl_core  # core ETFs CANNOT appear here
absent_counts = update_absent_counts(absent_counts, removals_candidates, universe_excl_core)
```

**Invariant:** A core ETF is never in `removals_candidates`, never in `absent_counts`, and never archived to `bronze-delisted/`.

### Backfill on add

When `additions` contains a new ticker (whether from scanner or from a core ETF not yet in bronze), the screener triggers backfill for **all 3 timeframes** (`1d`, `1h`, `5m`).

---

## 11. Backfill (Intraday)

`scripts/fetch_ib_historical.py` is extended with intraday support, but the existing daily code path is unchanged.

### Per-timeframe cursor

**Critical change after review:** the existing cursor only tracks `(preset, symbol)` — completed or not. With multi-timeframe, that's insufficient: a ticker can be done for `1d` but not `1h`.

New cursor schema:
```json
{
  "preset": "screened-universe",
  "completed": {
    "AAPL": ["1d", "1h", "5m"],
    "MSFT": ["1d", "1h"],
    "NVDA": ["1d"]
  }
}
```

A ticker is "fully done" only when all configured timeframes are present in its completed list. Resume picks up the missing `(symbol, timeframe)` combinations.

### Per-timeframe chunking

IB caps each historical request by timeframe:

| Timeframe | Max per request | 1-year backfill = how many requests |
|-----------|----------------|-------------------------------------|
| `1d` | 1 year | 1 |
| `1h` | 1 month | ~12 |
| `5m` | 1 week | ~52 |

The fetcher chunks by stepping `endDateTime` backward in `INTRADAY_MAX_REQUEST_DURATION[timeframe]` increments until reaching the depth limit. Each chunk is fetched, validated, and merged into the same `1h.parquet` or `5m.parquet` file.

### Pacing

IB rate limits historical data: ~60 requests per 10 minutes for small bar sizes. The existing `--max-concurrent` flag (default 6) is too aggressive for 5m chunks. New default for intraday fetches: `--max-concurrent 2` (configurable).

### Error handling

If a chunk fetch fails:
1. Retry the same chunk up to 3 times with exponential backoff
2. If still failing, log and continue to next chunk (don't abort the symbol)
3. The symbol's cursor entry is only updated when ALL chunks for that timeframe succeed
4. Partial timeframe success leaves the cursor entry incomplete → next run resumes from the missing chunks

### Realistic backfill estimate (1,166 tickers, 5 years)

| Timeframe | Requests/ticker | Total requests | At 60/10min | Wall time |
|-----------|----------------|---------------|-------------|-----------|
| `1d` | 1 | 1,166 | — | ~10 min (current) |
| `1h` | ~24 (2 yr depth) | ~28,000 | 60/10min = 6/min | ~78 hours |
| `5m` | ~52 (1 yr depth) | ~60,500 | 60/10min = 6/min | ~168 hours |

**Total: ~10 days of wall time for a full intraday backfill.** This is real and the spec acknowledges it. The implementation plan must include a "resume from cursor" smoke test before kicking off the full backfill.

(If the user runs the backfill in batches across multiple days, the per-timeframe cursor handles resumption cleanly.)

---

## 12. DuckDB Schema

Two new tables alongside `md.equities_daily`:

```sql
CREATE TABLE md.equities_1h (
    bar_timestamp TIMESTAMPTZ,
    symbol_id BIGINT,
    open DOUBLE, high DOUBLE, low DOUBLE, close DOUBLE,
    volume BIGINT,
    UNIQUE (bar_timestamp, symbol_id)
);

CREATE TABLE md.equities_5m (
    bar_timestamp TIMESTAMPTZ,
    symbol_id BIGINT,
    open DOUBLE, high DOUBLE, low DOUBLE, close DOUBLE,
    volume BIGINT,
    UNIQUE (bar_timestamp, symbol_id)
);
```

### Rebuild script

`scripts/rebuild_duckdb_from_parquet.py` is extended to read all three parquet types. New `--timeframe` flag (default = all):

```bash
python scripts/rebuild_duckdb_from_parquet.py                  # rebuilds daily + 1h + 5m
python scripts/rebuild_duckdb_from_parquet.py --timeframe 1h   # only 1h table
```

### `clients/db_client.py`

New methods alongside existing `replace_equities_from_parquet`:

```python
def replace_equities_intraday_from_parquet(
    self,
    bronze_dir: Path,
    timeframe: str,           # "1h" or "5m"
    venue: str = "SMART",
) -> dict[str, int]:
    """Rebuild md.equities_{timeframe} from intraday bronze parquet."""
```

The existing `replace_equities_from_parquet` method is unchanged.

---

## 13. R2 Sync

**Critical dependency from review.** `scripts/sync_to_r2.py` currently scans for `PARQUET_FILENAME` only. Extend it to iterate over all three filenames:

```python
PARQUET_FILES_TO_SYNC = ("1d.parquet", "1h.parquet", "5m.parquet")

# in upload():
for parquet_filename in PARQUET_FILES_TO_SYNC:
    for parquet_file in bronze_dir.rglob(parquet_filename):
        ...
```

Each timeframe gets its own R2 key:
```
s3://market-data/bronze/asset_class=equity/symbol=AAPL/1d.parquet
s3://market-data/bronze/asset_class=equity/symbol=AAPL/1h.parquet
s3://market-data/bronze/asset_class=equity/symbol=AAPL/5m.parquet
```

Old `data.parquet` keys (orphans from the rename) remain in R2 — clean manually if needed.

---

## 14. Container Entrypoint

`docker/ibroker-mkt-data/entrypoint.py` adds intraday support to the job cycle:

```python
def run_intraday_update(force: bool = False) -> int:
    """Run intraday update (1h + 5m bars for equity universe)."""
    ...

def run_job_cycle(force: bool = False) -> int:
    rc = sync_download()
    rc = run_daily_update(force=force)
    if rc == 0:
        rc = run_intraday_update(force=force)  # NEW — only after daily succeeds
    if rc == 0:
        sync_upload()
    return rc
```

`--rebuild` mode also fetches intraday timeframes after the daily seed.

---

## 15. Migration Strategy

The new files are **additive** — existing `1d.parquet` files stay where they are. No on-disk migration needed.

### Sequenced rollout

1. **Code changes** (in order, each commit independently testable):
   - Add `clients/parquet_io.py` (extracted helpers, no behavior change)
   - Add `clients/intraday_bronze_client.py` + tests
   - Add `presets/core-etfs.json`
   - Patch `scripts/universe_screener.py` for core ETF exclusion + intraday backfill trigger
   - Add intraday support to `scripts/fetch_ib_historical.py` (per-tf cursor, per-tf chunking)
   - Add new script `scripts/intraday_update.py`
   - Patch `scripts/health_check.py` for `--intraday` mode (report-only)
   - Patch `clients/db_client.py` with `replace_equities_intraday_from_parquet`
   - Patch `scripts/rebuild_duckdb_from_parquet.py` for `--timeframe`
   - Patch `scripts/sync_to_r2.py` to scan all three filenames
   - Patch `docker/ibroker-mkt-data/entrypoint.py` to chain intraday after daily

2. **Empirical probe** — run `scripts/probe_ib_intraday.py` against live IB Gateway, lock in `formatDate`/`useRTH`/timezone normalization. Document results in commit message.

3. **Smoke test** — fetch 1 day of `1h` for AAPL only, verify bronze parquet has 7 rows with correct UTC timestamps. Roundtrip via DuckDB rebuild.

4. **Smoke test 2** — fetch 1 week of `5m` for AAPL only. Verify cursor resume by interrupting and restarting.

5. **Full intraday backfill** — kick off the screened universe backfill. Expect ~10 days wall time. Monitor cursor progress.

6. **Rebuild + sync** — `make rebuild` once intraday backfill is done.

---

## 16. Testing Strategy

### Unit tests

| Component | Test focus |
|-----------|-----------|
| `parquet_io.py` | Atomic write, validate roundtrip, cleanup on failure |
| `intraday_bronze_client.py` | Schema enforcement, rejection of naive datetimes, merge by `bar_timestamp` (not `trade_date`), per-timeframe path resolution |
| `core-etfs.json` integration | (1) Core ETFs in `additions` when missing from bronze; (2) Core ETFs NEVER in `removals_candidates`; (3) Core ETFs NEVER in `absent_counts` after a run |
| `fetch_ib_historical.py` per-tf cursor | Resume after partial-timeframe failure leaves correct state; full re-run is idempotent |
| `intraday_update.py` session model | Test all 5 states (`complete`, `in_progress`, `live`, `tail_gap`, `historical`) with mocked clock |
| `health_check.py` intraday | Report-only mode never writes; targeted `--repair` mode does write; halt-suspect annotation |
| `get_early_close_days(year)` | Day-after-Thanksgiving + Christmas Eve + July 3 for 2020–2030 |
| `session_close_time(d)` | Returns 13:00 for early-close, 16:00 for full day |
| `db_client.replace_equities_intraday_from_parquet` | Round-trip with TIMESTAMPTZ column |
| `sync_to_r2.py` | Uploads all 3 timeframes, separate keys |
| Timezone normalization | DST transition day (spring forward) — verify no duplicate or missing 5m bars |

### Coverage gate

100% on all new code (per `pyproject.toml`). Existing 100% on unchanged code preserved.

### Probe verification

The empirical probe results (§ 5) are captured as constants in `intraday_bronze_client.py` with a comment linking to the commit that ran the probe. Tests assert these constants haven't drifted.

---

## 17. Verification Plan

After implementation:

1. `python -m pytest tests/ -v --cov=clients --cov=scripts --cov-report=term-missing` — all green, 100% coverage
2. `python scripts/probe_ib_intraday.py` — print empirical IB return type, verify matches constants
3. `python scripts/intraday_update.py --dry-run` — review report
4. Targeted smoke test: 1 ticker × 1 day × `5m`, verify file appears
5. `python scripts/rebuild_duckdb_from_parquet.py --timeframe 5m`
6. Spot-check timezone correctness in DuckDB:
   ```sql
   SELECT bar_timestamp,
          bar_timestamp AT TIME ZONE 'America/New_York' AS et_time
   FROM md.equities_5m
   WHERE symbol_id = (SELECT symbol_id FROM md.symbols WHERE symbol = 'AAPL')
   ORDER BY bar_timestamp
   LIMIT 5;
   -- First row should show et_time = '09:30:00'
   ```
7. `python scripts/health_check.py --intraday --dry-run` — verify no false positives on early-close days, holidays, or DST transitions
8. `python scripts/sync_to_r2.py` — verify all 3 timeframes uploaded
9. Run screener with `core-etfs.json` and verify SPY/QQQ/etc are in additions (if not yet in bronze) but never in removals

---

## 18. Known Limitations

- Trading halts produce missing intraday bars; the health check reports them but does not auto-distinguish from data loss
- IB rate limits make full intraday backfill multi-day; the per-timeframe cursor handles resumption
- Adjusted close is not available for intraday TRADES bars
- DST transitions are tested empirically via the probe; implementation must verify behavior
- Screener-driven additions trigger 3-timeframe backfill which can take >24 hours per ticker on slow days

---

## 19. Out of Scope (future work)

- Continuous snapshot job (live bar streaming)
- Sub-minute bars
- Multi-timeframe for futures and volatility indices
- Automatic intraday gap repair (currently report-only)
- Bar timestamp normalization tooling for non-UTC consumers
