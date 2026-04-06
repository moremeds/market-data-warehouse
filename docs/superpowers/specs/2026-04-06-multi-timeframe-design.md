# Multi-Timeframe Bars and Core ETF List

**Date:** 2026-04-06
**Status:** Approved

## Context

Today the warehouse stores only daily (`1d`) bars per ticker. Three use cases need finer granularity:

1. **Backtesting** — works with `1d` (already covered) but swing strategies want hourly entry timing
2. **Intraday signals** — opening range, VWAP fades, gap-and-go need minute-level data
3. **Options analytics** — dealer gamma, IV surface, hedging analysis need hourly underlying spot

The recent rename from `data.parquet` → `1d.parquet` was the prerequisite for this. The filename now encodes the timeframe; we just need to add `1h.parquet` and `5m.parquet` alongside it.

A second gap: the screener-driven universe is reactive to market activity, but we want a stable core of ETFs (broad market, sectors, international, vol products) **always** included regardless of scanner output.

## Goals

1. Add `1h` and `5m` bar storage alongside `1d` for the equity universe
2. Add a guaranteed-include core ETF list, unioned into the screened universe
3. Unify timestamp handling on **UTC with timezone awareness** to prevent DST bugs

## Non-Goals

- Sub-minute bars (`1s`, `1m` is also out — see depth analysis)
- Adjusted close for intraday (IB TRADES data isn't adjusted)
- Continuous (tick) data
- Multi-timeframe for futures / volatility (equity only for now)

---

## 1. Storage Layout

Sibling parquet files per symbol — same `symbol={TICKER}` directory:

```
~/market-warehouse/data-lake/bronze/asset_class=equity/
└── symbol=AAPL/
    ├── 1d.parquet
    ├── 1h.parquet
    └── 5m.parquet
```

Existing `PARQUET_FILENAME = "1d.parquet"` constant is generalized to a per-timeframe lookup.

---

## 2. Timeframe Configuration

A single source of truth in `clients/bronze_client.py`:

```python
TIMEFRAMES = ("1d", "1h", "5m")  # Order matters for iteration in daily updates

# IB depth limits — used for backfill duration computation
TIMEFRAME_MAX_DEPTH = {
    "1d": "30 Y",   # Inception (decades)
    "1h": "2 Y",
    "5m": "1 Y",
}

# IB bar size strings (the format reqHistoricalData expects)
TIMEFRAME_IB_BAR_SIZE = {
    "1d": "1 day",
    "1h": "1 hour",
    "5m": "5 mins",
}
```

All scripts iterate over `TIMEFRAMES`. **No `--timeframe` flag** on daily update / screener / health check (config-driven). Manual rebuild scripts get an optional `--timeframe` for targeted work.

---

## 3. Schema Profiles

Extend `_SCHEMA_PROFILES` in `bronze_client.py`. The key becomes `(asset_class, timeframe)`:

| Profile key | Time column | Other columns |
|-------------|-------------|---------------|
| `("equity", "1d")` (existing, renamed) | `trade_date DATE` | `symbol_id`, OHLCV, `adj_close` |
| `("equity", "1h")` (new) | `bar_timestamp TIMESTAMPTZ` | `symbol_id`, OHLCV |
| `("equity", "5m")` (new) | `bar_timestamp TIMESTAMPTZ` | `symbol_id`, OHLCV |
| `("volatility", "1d")` | `trade_date DATE` | `symbol_id`, OHLCV, `adj_close` |
| `("futures", "1d")` | `trade_date DATE` | `contract_id`, `root_symbol`, ... |

`BronzeClient(asset_class="equity", timeframe="1h")` selects the right profile and resolves the right parquet filename.

### Intraday Schema (`1h` and `5m`)

```python
pa.schema([
    ("bar_timestamp", pa.timestamp("us", tz="UTC")),
    ("symbol_id",     pa.int64()),
    ("open",          pa.float64()),
    ("high",          pa.float64()),
    ("low",           pa.float64()),
    ("close",         pa.float64()),
    ("volume",        pa.int64()),
])
```

No `adj_close` (TRADES data unadjusted), no `trade_date` (derivable from `bar_timestamp` at query time).

---

## 4. Timestamp & Timezone Convention

**Universal rule: All bar timestamps stored as UTC with timezone awareness.**

### Why
DST transitions happen twice a year. A naive timestamp `09:30:00` on a DST boundary day is ambiguous — it could mean two different UTC instants. UTC-with-TZ has zero ambiguity.

### Storage

| Layer | Type | Example |
|-------|------|---------|
| Parquet | `pa.timestamp("us", tz="UTC")` | `2026-04-04 13:30:00+00:00` |
| DuckDB | `TIMESTAMPTZ` | DuckDB stores TZ-aware natively |
| Python | `datetime` with `tzinfo=ZoneInfo("UTC")` | Never naive |

### Bar Timestamp Convention

**Start-of-bar timestamps** (matches IB's default — no conversion needed at fetch time).

| Bar | UTC timestamp | Covers (UTC) | Covers (ET, winter) |
|-----|--------------|--------------|---------------------|
| First 5m of day | `14:30:00Z` | 14:30–14:35 | 09:30–09:35 |
| Last 5m of day | `20:55:00Z` | 20:55–21:00 | 15:55–16:00 |
| First 1h of day | `14:30:00Z` | 14:30–15:30 | 09:30–10:30 |
| Last 1h of day | `20:30:00Z` | 20:30–21:00 | 15:30–16:00 |

In summer the UTC values shift by -1 hour (ET = UTC-4 instead of UTC-5). The UTC values change across DST — that is *correct*.

### Conversion Boundaries

| Boundary | Rule |
|----------|------|
| **IB → bronze** | IB returns naive datetimes in exchange local time. Wrap with `ZoneInfo("America/New_York")`, then `.astimezone(ZoneInfo("UTC"))`. Done at the fetch site, before any storage code touches the data. |
| **Bronze → query** | Stored as UTC. Convert to ET in the SELECT: `bar_timestamp AT TIME ZONE 'America/New_York'`. |
| **Health check expected timestamps** | Build in ET, convert to UTC. See § 7. |

---

## 5. Core ETF List

New file: `presets/core-etfs.json`

```json
{
  "name": "core-etfs",
  "description": "Always-include ETFs — added to the screened universe regardless of scanner output. Covers broad market, sectors, international, commodities, bonds, vol, leveraged, and crypto for backtesting, intraday signals, and options analytics.",
  "groups": {
    "broad_market": {
      "description": "U.S. broad market index ETFs — core benchmarks",
      "tickers": ["SPY", "QQQ", "IWM", "DIA", "VTI"]
    },
    "sectors_spdr": {
      "description": "All 11 SPDR sector ETFs — sector rotation, factor models",
      "tickers": ["XLF", "XLE", "XLK", "XLV", "XLI", "XLP", "XLY", "XLU", "XLB", "XLRE", "XLC"]
    },
    "industry": {
      "description": "Key industry ETFs not covered by SPDR sectors",
      "tickers": ["SMH"]
    },
    "international": {
      "description": "Country and regional ETFs",
      "tickers": ["EFA", "EEM", "EWJ", "EWY", "EWZ", "FXI", "KWEB"]
    },
    "metals": {
      "description": "Precious metals — gold and silver spot exposure",
      "tickers": ["GLD", "SLV"]
    },
    "commodities": {
      "description": "Energy and ag commodities — macro/correlation analysis",
      "tickers": ["USO"]
    },
    "bonds": {
      "description": "Treasury, IG credit, HY credit, aggregate — rates and credit",
      "tickers": ["TLT", "IEF", "HYG", "LQD", "AGG"]
    },
    "volatility": {
      "description": "VIX-tracking ETFs — options analytics, dealer gamma",
      "tickers": ["VXX", "UVXY"]
    },
    "leveraged": {
      "description": "Leveraged broad market — heavily traded for intraday signals",
      "tickers": ["TQQQ", "SQQQ"]
    },
    "crypto": {
      "description": "Spot crypto ETFs",
      "tickers": ["IBIT"]
    }
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

The flat `tickers` array at the bottom matches the existing preset format so `load_preset()` works unchanged. The `groups` field is metadata for documentation.

### Screener integration

`scripts/universe_screener.py` loads `presets/core-etfs.json` after running scanner sweeps and unions the tickers in. Core ETFs are **never archived** — they bypass the absent-counts / grace-period logic entirely.

---

## 6. Backfill Behavior

When the screener detects a new ticker (added either by scanner or by being in the core ETF list and not yet in bronze), it backfills **all three timeframes** immediately.

`scripts/fetch_ib_historical.py` iterates over `TIMEFRAMES`:

```python
for timeframe in TIMEFRAMES:
    bronze = BronzeClient(asset_class="equity", timeframe=timeframe)
    duration = TIMEFRAME_MAX_DEPTH[timeframe]
    bar_size = TIMEFRAME_IB_BAR_SIZE[timeframe]
    bars = await ib.get_historical_data_async(contract, duration=duration, bar_size=bar_size, what_to_show="TRADES")
    rows = bars_to_rows(bars, symbol_id, timeframe=timeframe)
    bronze.replace_ticker_rows(symbol, rows)
```

Each timeframe writes to its own parquet file in the symbol directory. New ticker takes ~3× current backfill time (acceptable; new additions are infrequent).

---

## 7. Daily Update

`scripts/daily_update.py` iterates over `TIMEFRAMES` per ticker:

```python
for timeframe in TIMEFRAMES:
    # Detect tail gap, fetch missing bars at this timeframe, merge into bronze
```

No new CLI flags. The `--asset-class` flag continues to work (currently equity only; futures/volatility stay daily-only).

Daily run time grows from ~10 min to ~30 min for the screened universe. Acceptable.

---

## 8. Health Check

`scripts/health_check.py` iterates over `TIMEFRAMES`. For intraday timeframes:

### Expected bar generation

For each trading day in the symbol's date range:
1. Skip non-trading days (uses existing `is_trading_day(d)` from `daily_update.py`)
2. For early-close days (computed by new `get_early_close_days(year)`), generate bars from 9:30 ET to 13:00 ET
3. For full days, generate bars from 9:30 ET to 16:00 ET
4. Convert each ET timestamp to UTC for comparison with stored bars

### Early close days

New helper in `scripts/daily_update.py`:

```python
def get_early_close_days(year: int) -> dict[date, time]:
    """Return {trading_date: close_time_ET} for early-close days.

    Typical: day after Thanksgiving (1pm), Christmas Eve (1pm), July 3 if Independence Day on weekday.
    """
```

Same pattern as `get_nyse_holidays()`. ~3-4 dates per year.

### Pre/post market

Not generated as expected bars. We use `useRTH=true` in IB requests, so only RTH bars exist.

### Trading halts

Mid-session halts produce missing bars. We do **not** auto-flag these (no reliable source). Documented as known limitation.

---

## 9. DuckDB Schema

Three new tables (one per timeframe):

```sql
CREATE TABLE md.equities_daily (
    trade_date DATE,
    symbol_id BIGINT,
    open DOUBLE, high DOUBLE, low DOUBLE, close DOUBLE,
    adj_close DOUBLE,
    volume BIGINT,
    UNIQUE (trade_date, symbol_id)
);  -- existing

CREATE TABLE md.equities_1h (
    bar_timestamp TIMESTAMPTZ,
    symbol_id BIGINT,
    open DOUBLE, high DOUBLE, low DOUBLE, close DOUBLE,
    volume BIGINT,
    UNIQUE (bar_timestamp, symbol_id)
);  -- new

CREATE TABLE md.equities_5m (
    bar_timestamp TIMESTAMPTZ,
    symbol_id BIGINT,
    open DOUBLE, high DOUBLE, low DOUBLE, close DOUBLE,
    volume BIGINT,
    UNIQUE (bar_timestamp, symbol_id)
);  -- new
```

`scripts/rebuild_duckdb_from_parquet.py` rebuilds **all three** by default. Optional `--timeframe 1h` flag for targeted rebuilds.

---

## 10. R2 Sync

`scripts/sync_to_r2.py` replaces the single `PARQUET_FILENAME` lookup with iteration over `TIMEFRAMES`. Each timeframe uploads to its own R2 key:

```
s3://market-data/bronze/asset_class=equity/symbol=AAPL/1d.parquet
s3://market-data/bronze/asset_class=equity/symbol=AAPL/1h.parquet
s3://market-data/bronze/asset_class=equity/symbol=AAPL/5m.parquet
```

Old `data.parquet` keys remain orphaned in R2 from the rename — clean them manually if needed.

---

## 11. Migration Strategy

The existing `1d.parquet` files stay where they are. The new timeframes are **additive**:

1. Deploy code changes (extends schema profiles, adds `TIMEFRAMES` constant)
2. Run tests
3. Run `scripts/fetch_ib_historical.py --preset presets/screened-universe.json` (now iterates timeframes — fills the new `1h.parquet` and `5m.parquet` for every ticker)
4. Rebuild DuckDB
5. R2 sync uploads the new files

No migration script needed. New files just appear.

---

## 12. Testing Strategy

| Component | Test approach |
|-----------|--------------|
| `bronze_client.py` schema profiles | Parametrized tests for `equity_1d`, `equity_1h`, `equity_5m` schemas, write/read roundtrips |
| Timestamp UTC enforcement | Tests that pass naive datetimes raise errors; tests that pass tz-aware datetimes succeed |
| `core-etfs.json` integration | Screener test verifying core ETFs are unioned and never archived |
| Daily update iteration | Tests that all 3 timeframes get fetched and merged per ticker |
| Health check intraday | Mock IB response with missing bars, verify gaps detected at correct UTC timestamps |
| Early close days | Unit tests for `get_early_close_days(2025)`, `(2026)` |
| DuckDB rebuild | Tests that all 3 tables populated with correct row counts and TIMESTAMPTZ column type |

100% coverage enforced as before.

---

## 13. Verification Plan

1. Run full test suite — all green, 100% coverage
2. Run `make build && make rebuild` — full reseed with all 3 timeframes
3. Spot-check parquet files exist:
   ```
   ls ~/market-warehouse/data-lake/bronze/asset_class=equity/symbol=AAPL/
   # Expected: 1d.parquet, 1h.parquet, 5m.parquet
   ```
4. Spot-check DuckDB row counts:
   ```sql
   SELECT 'daily' AS tf, COUNT(*) FROM md.equities_daily
   UNION ALL SELECT '1h', COUNT(*) FROM md.equities_1h
   UNION ALL SELECT '5m', COUNT(*) FROM md.equities_5m;
   ```
5. Spot-check timezone correctness:
   ```sql
   SELECT bar_timestamp,
          bar_timestamp AT TIME ZONE 'America/New_York' AS et_time
   FROM md.equities_5m
   WHERE symbol_id = (SELECT symbol_id FROM md.symbols WHERE symbol = 'AAPL')
     AND bar_timestamp::DATE = '2026-04-04'
   ORDER BY bar_timestamp
   LIMIT 5;
   -- Expected: first row bar_timestamp ends in '14:30:00+00' (winter) or '13:30:00+00' (summer),
   -- et_time shows '09:30:00'
   ```
6. Run `scripts/health_check.py --force` — verify no false-positive gaps for early-close days, holidays, or DST transitions
7. Run `scripts/sync_to_r2.py` — verify all 3 timeframes uploaded
