# Market Data Warehouse

A local-first financial data warehouse for universe-scale market data.

The project is designed to store and analyze historical **OHLCV data across equities, options, and futures** with a path from **daily bars today to intraday data later**. It uses a **partitioned Parquet data lake** as the canonical storage layer, **DuckDB** as the fast local analytical engine for research and backtesting, and **ClickHouse** as the production-oriented warehouse for large-scale aggregation, serving, and concurrency.

The goal is to give you:

* a **high-performance local quant research environment** on Apple Silicon
* a clean **multi-asset schema** that can handle very large datasets
* a **polyglot workflow** across Python, Rust, and Node.js
* a straightforward **path to cloud production** without rebuilding the data model from scratch

In practice, this project is meant to be the foundation for:

* historical market data storage
* factor research
* backtesting
* rolling analytics like VWAP, moving averages, and cross-sectional signals
* future expansion into intraday and production-grade analytical serving

In one sentence:

**It’s a local-first, production-ready market data warehouse for serious quantitative research and analytics.**


## Project Setup
This project bootstraps a **local-first financial data warehouse** on Apple Silicon macOS.

It sets up:

* **DuckDB** for local analytics and research
* **ClickHouse** for production-style local benchmarking
* a **partitioned Parquet data lake** for canonical storage
* a Python environment with:

  * `duckdb`
  * `polars`
  * `pandas`
  * `pyarrow`
  * `clickhouse-connect`
  * `numpy`
  * `scipy`
  * `jupyterlab`

## Architecture

The intended workflow is:

* **Raw vendor data** → `data-lake/raw/`
* **Normalized canonical Parquet** → `data-lake/bronze/`
* **Cleaned / adjusted / deduped datasets** → `data-lake/silver/`
* **Derived analytics / factor tables / marts** → `data-lake/gold/`

### Local stack

* **Parquet** is the system of record
* **DuckDB** is the default local query engine
* **ClickHouse** is optional and used for warehouse-style benchmarking and production-like schema testing

## Directory layout

```text
~/market-warehouse/
├── .venv/
├── clickhouse/
├── data-lake/
│   ├── raw/
│   │   ├── asset_class=equity/
│   │   ├── asset_class=option/
│   │   └── asset_class=future/
│   ├── bronze/
│   │   ├── asset_class=equity/
│   │   ├── asset_class=option/
│   │   └── asset_class=future/
│   ├── silver/
│   │   ├── asset_class=equity/
│   │   ├── asset_class=option/
│   │   └── asset_class=future/
│   └── gold/
│       ├── asset_class=equity/
│       ├── asset_class=option/
│       └── asset_class=future/
├── duckdb/
│   └── market.duckdb
├── logs/
├── scripts/
│   ├── activate_env.sh
│   ├── bootstrap_clickhouse.sql
│   ├── bootstrap_duckdb.sql
│   ├── init_clickhouse.sh
│   ├── query_parquet_duckdb.sql
│   ├── start_clickhouse.sh
│   ├── stop_clickhouse.sh
│   └── write_sample_parquet.py
└── tmp_duckdb/
```

## Requirements

* macOS
* Apple Silicon recommended
* Homebrew
* internet access for package installation

## Setup

Save the setup script as:

```bash
setup_market_warehouse.sh
```

Make it executable:

```bash
chmod +x setup_market_warehouse.sh
```

## Flags

The setup script supports these flags:

* `--start-clickhouse`
  Starts ClickHouse after setup.

* `--init-clickhouse`
  Initializes the ClickHouse schema after setup. This also implies `--start-clickhouse`.

* `--with-sample-data`
  Generates sample Parquet data under the bronze layer.

* `--smoke-test`
  Runs basic validation checks after setup.

* `--help`
  Prints usage information.

## Common commands

### Minimal install

```bash
./setup_market_warehouse.sh
```

### Install + sample data + validation

```bash
./setup_market_warehouse.sh --with-sample-data --smoke-test
```

### Install + ClickHouse startup + ClickHouse schema

```bash
./setup_market_warehouse.sh --start-clickhouse --init-clickhouse
```

### Full bootstrap

```bash
./setup_market_warehouse.sh --start-clickhouse --init-clickhouse --with-sample-data --smoke-test
```

## What the script does

The script:

1. verifies macOS / Apple Silicon assumptions
2. installs required Homebrew packages
3. installs optional Rust and Node.js tooling if missing
4. creates the warehouse directory structure
5. creates a Python virtual environment
6. installs Python dependencies
7. creates the DuckDB schema
8. writes ClickHouse schema/bootstrap files
9. writes helper scripts for ClickHouse lifecycle management
10. optionally creates sample Parquet data
11. optionally starts and initializes ClickHouse
12. optionally runs smoke tests

## DuckDB schema

The DuckDB bootstrap creates schema `md` with:

* `md.symbols`
* `md.equities_daily`
* `md.futures_daily`
* `md.options_daily`

Notable change:

* the options column is named **`option_right`** instead of `right` to avoid reserved keyword issues

## ClickHouse schema

The ClickHouse bootstrap creates database `md` with:

* `md.equities_daily`
* `md.futures_daily`
* `md.options_daily`

All tables use **MergeTree** and are partitioned by `toYYYYMM(trade_date)`.

## Activating Python later

The setup script creates the virtual environment, but it cannot keep your terminal activated after the script exits.

To activate it in a new shell:

```bash
source ~/market-warehouse/.venv/bin/activate
```

Or:

```bash
~/market-warehouse/scripts/activate_env.sh
```

## Helper scripts

### Start ClickHouse

```bash
~/market-warehouse/scripts/start_clickhouse.sh
```

### Initialize ClickHouse schema

```bash
~/market-warehouse/scripts/init_clickhouse.sh
```

### Stop ClickHouse

```bash
~/market-warehouse/scripts/stop_clickhouse.sh
```

## Sample data

To generate sample Parquet data manually:

```bash
python ~/market-warehouse/scripts/write_sample_parquet.py
```

This writes a small equities dataset into:

```text
~/market-warehouse/bronze/asset_class=equity/year=2025/month=01/
```

## Querying sample Parquet with DuckDB

Run:

```bash
duckdb ~/market-warehouse/duckdb/market.duckdb < ~/market-warehouse/scripts/query_parquet_duckdb.sql
```

The sample query calculates:

* average close
* total volume

grouped by `symbol_id`.

## Smoke tests

When `--smoke-test` is enabled, the script:

* checks `duckdb --version`
* checks `python --version`
* verifies Python imports for core packages
* runs the sample DuckDB query if sample data exists
* checks ClickHouse connectivity if ClickHouse was started

## Troubleshooting

### DuckDB bootstrap appears to stop after “Bootstrapping DuckDB...”

That was previously caused by loading the `httpfs` extension during bootstrap. The current script no longer does that.

### DuckDB parser error near `primary_key`

DuckDB expects:

```sql
symbol_id BIGINT PRIMARY KEY
```

not:

```sql
primary_key (symbol_id)
```

### DuckDB parser error near `right`

`right` is a reserved keyword. The schema now uses:

```sql
option_right
```

### ClickHouse does not start

Try running the helper directly:

```bash
~/market-warehouse/scripts/start_clickhouse.sh
```

Then verify:

```bash
clickhouse-client --query "SELECT version()"
```

## Recommended workflow

For everyday local work:

1. keep canonical data in partitioned Parquet
2. use DuckDB for research, backtests, and local analytics
3. use ClickHouse only when you need:

   * larger-scale benchmarking
   * concurrency
   * production-like testing
   * intraday warehouse experiments

## Recommended command

```bash
./setup_market_warehouse.sh --start-clickhouse --init-clickhouse --with-sample-data --smoke-test
```
