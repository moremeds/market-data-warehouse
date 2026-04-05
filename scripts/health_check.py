"""Core gap detection, backfill engine, and CLI entry point for warehouse health check."""

from __future__ import annotations

import argparse
import logging
import os
import subprocess
from datetime import date, timedelta
from pathlib import Path

from rich.console import Console

from clients import BronzeClient
from scripts.daily_update import (
    _make_contract,
    bars_to_futures_rows,
    bars_to_rows,
    is_trading_day,
    validate_bars,
)

log = logging.getLogger(__name__)

console = Console()
_WAREHOUSE_DIR = Path(os.getenv("MDW_WAREHOUSE_DIR", str(Path.home() / "market-warehouse")))
_DATA_LAKE = _WAREHOUSE_DIR / "data-lake"

_SCRIPT_DIR = Path(__file__).resolve().parent


def _resolve_bronze_dir(asset_class: str) -> Path:
    """Return the bronze directory for the given asset class."""
    return _DATA_LAKE / "bronze" / f"asset_class={asset_class}"


def find_interior_gaps(actual_dates: list[date], asset_class: str = "equity") -> list[date]:
    """Return trading/weekday dates missing between the min and max of *actual_dates*.

    For equity/volatility asset classes the NYSE calendar is used to determine
    which dates are expected.  For futures a simple weekday check is used
    (CME trades some NYSE holidays).

    A single date or empty list always returns ``[]``.
    """
    if len(actual_dates) < 2:
        return []

    actual_set = set(actual_dates)
    start = min(actual_dates)
    end = max(actual_dates)

    gaps: list[date] = []
    current = start + timedelta(days=1)
    while current < end:
        if asset_class == "futures":
            expected = current.weekday() < 5  # Mon-Fri
        else:
            expected = is_trading_day(current)

        if expected and current not in actual_set:
            gaps.append(current)

        current += timedelta(days=1)

    return gaps


def group_contiguous_dates(dates: list[date]) -> list[tuple[date, date]]:
    """Group *dates* into contiguous ``(start, end)`` ranges.

    Contiguous means each successive date is exactly 1 calendar day after
    the previous one.  Non-contiguous dates start a new range.

    Returns ``[]`` for an empty input.
    """
    if not dates:
        return []

    groups: list[tuple[date, date]] = []
    start = dates[0]
    prev = dates[0]

    for current in dates[1:]:
        if (current - prev).days == 1:
            prev = current
        else:
            groups.append((start, prev))
            start = current
            prev = current

    groups.append((start, prev))
    return groups


def compute_range_duration(start_date: date, end_date: date) -> str:
    """Return an IB-style duration string for an arbitrary date range.

    Mirrors the logic in ``compute_ib_duration`` from ``daily_update``:
    calendar days between *start_date* and *end_date* plus a 2-day buffer.

    * ``<= 0`` calendar days → ``"1 D"``
    * ``<= 180`` (after buffer) → ``"{N} D"``
    * ``<= 365`` (after buffer) → ``"1 Y"``
    * else → ``"2 Y"``
    """
    cal_days = (end_date - start_date).days
    if cal_days <= 0:
        return "1 D"
    cal_days += 2
    if cal_days <= 180:
        return f"{cal_days} D"
    elif cal_days <= 365:
        return "1 Y"
    else:
        return "2 Y"


def get_all_trade_dates(bronze: BronzeClient) -> dict[str, list[date]]:
    """Return ``{symbol: [date, ...]}`` for every symbol in bronze, sorted ascending.

    Uses a single bulk DuckDB query over the full parquet glob for efficiency.
    Returns ``{}`` when the bronze directory is empty or has no symbols.
    """
    if not bronze.get_existing_symbols():
        return {}

    sql = f"""
        SELECT symbol, trade_date
        FROM read_parquet('{bronze._escaped_glob()}', hive_partitioning=true)
        ORDER BY symbol, trade_date
    """
    rows = bronze._query(sql)

    result: dict[str, list[date]] = {}
    for row in rows:
        symbol: str = row["symbol"]
        raw = row["trade_date"]
        if isinstance(raw, date):
            d = raw
        else:
            d = date.fromisoformat(str(raw))
        result.setdefault(symbol, []).append(d)

    return result


def _send_alert(
    run_date: str,
    asset_class: str,
    total_gaps: int,
    repaired: int,
    log_path: Path,
) -> None:
    """Send an email alert via the Node.js alert script."""
    alert_script = _SCRIPT_DIR / "send_daily_update_failure_email.mjs"
    error_summary = (
        f"health_check ({asset_class}): {total_gaps} interior gaps detected, "
        f"{repaired} repaired."
    )
    cmd = [
        "node",
        str(alert_script),
        "--run-date", run_date,
        "--log-file", str(log_path),
        "--error-summary", error_summary,
        "--repo-root", str(_SCRIPT_DIR.parent),
        "--job-name", "health_check",
    ]
    subprocess.run(cmd, check=False)


def main() -> None:
    """CLI entry point for the warehouse health check."""
    parser = argparse.ArgumentParser(description="Market data warehouse health check")
    parser.add_argument("--dry-run", action="store_true", help="Report gaps without backfilling")
    parser.add_argument("--force", action="store_true", help="Run even if not a trading day")
    parser.add_argument(
        "--asset-class",
        choices=["equity", "volatility", "futures"],
        default="equity",
        help="Asset class to check (default: equity)",
    )
    parser.add_argument(
        "--host",
        type=str,
        default=os.getenv("MDW_IB_HOST", "127.0.0.1"),
        help="IB Gateway host",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=int(os.getenv("MDW_IB_PORT", "4001")),
        help="IB Gateway port",
    )
    parser.add_argument(
        "--alert-threshold",
        type=int,
        default=10,
        help="Number of repaired gaps that triggers an email alert (default: 10)",
    )
    args = parser.parse_args()

    today = date.today()

    # ── Trading day check ─────────────────────────────────────────────
    if not args.force and not is_trading_day(today):
        console.print(
            f"[yellow]{today} is not a trading day. Use --force to override.[/yellow]"
        )
        return

    asset_class = args.asset_class
    bronze_dir = _resolve_bronze_dir(asset_class)

    console.print(
        f"\n[bold]Health Check[/bold]  date={today}  asset_class={asset_class}"
        f"  dry_run={args.dry_run}  force={args.force}"
    )

    # ── Gap detection ──────────────────────────────────────────────────
    with BronzeClient(bronze_dir=bronze_dir, asset_class=asset_class) as bronze:
        all_dates = get_all_trade_dates(bronze)

        if not all_dates:
            console.print(
                "[yellow]No tickers found in bronze parquet. Run fetch_ib_historical.py first.[/yellow]"
            )
            return

        gaps_by_symbol: dict[str, list[date]] = {}
        for symbol, dates in all_dates.items():
            gaps = find_interior_gaps(dates, asset_class=asset_class)
            if gaps:
                gaps_by_symbol[symbol] = gaps

        total_gaps = sum(len(g) for g in gaps_by_symbol.values())
        console.print(f"\n[bold]Gap Report ({len(all_dates)} symbols):[/bold]")
        if gaps_by_symbol:
            console.print(f"  [red]Interior gaps found in {len(gaps_by_symbol)} symbols ({total_gaps} gap-days total)[/red]")
            for sym, gaps in sorted(gaps_by_symbol.items()):
                ranges = group_contiguous_dates(gaps)
                for start, end in ranges:
                    dur = compute_range_duration(start, end)
                    console.print(f"    {sym}: {start} – {end} ({dur})")
        else:
            console.print("  [green]No interior gaps detected[/green]")

        if args.dry_run:
            console.print("\n[bold]Dry-run complete — no backfill performed.[/bold]")
            return

        if not gaps_by_symbol:
            return

        # ── Backfill ───────────────────────────────────────────────────
        if asset_class == "volatility":
            console.print(
                "[yellow]Backfill for volatility not yet implemented. "
                "Use fetch_cboe_volatility.py instead.[/yellow]"
            )
            return

        # Lazy import — only needed when actually connecting to IB
        from clients.ib_client import IBClient  # noqa: PLC0415
        from scripts.daily_update import fetch_fallback_bars  # noqa: PLC0415

        log_dir = _WAREHOUSE_DIR / "logs"
        log_dir.mkdir(parents=True, exist_ok=True)
        log_path = log_dir / f"health_check_{today:%Y-%m-%d}.log"

        repaired = 0
        fallback_repaired = 0

        console.print(f"\n[bold]Backfilling {len(gaps_by_symbol)} symbols via IB...[/bold]")

        with IBClient(host=args.host, port=args.port) as ib:
            ib.ib.run(ib.connect())

            for symbol, gaps in sorted(gaps_by_symbol.items()):
                ranges = group_contiguous_dates(gaps)
                symbol_rows: list[dict] = []

                for start, end in ranges:
                    duration = compute_range_duration(start, end)
                    contract = _make_contract(symbol, asset_class)
                    try:
                        ib.ib.run(ib.ib.qualifyContractsAsync(contract))
                        bars = ib.ib.run(
                            ib.get_historical_data_async(
                                contract,
                                duration=duration,
                                bar_size="1 day",
                                what_to_show="TRADES",
                            )
                        )
                    except Exception as exc:
                        console.print(f"  [red]{symbol}: IB error — {exc}[/red]")
                        bars = []

                    if not bars:
                        continue

                    valid_bars, issues = validate_bars(bars, symbol, asset_class)
                    for issue in issues:
                        log.warning(issue)

                    if asset_class == "futures":
                        root_symbol = symbol.rsplit("_", 1)[0]
                        expiry_code = symbol.rsplit("_", 1)[1]
                        expiry_date = f"{expiry_code[:4]}-{expiry_code[4:6]}-01"
                        contract_id = bronze.get_symbol_id(symbol)
                        rows = bars_to_futures_rows(valid_bars, contract_id, root_symbol, expiry_date)
                    else:
                        symbol_id = bronze.get_symbol_id(symbol)
                        rows = bars_to_rows(valid_bars, symbol_id)

                    symbol_rows.extend(rows)

                if symbol_rows:
                    inserted = bronze.merge_ticker_rows(symbol, symbol_rows)
                    repaired += inserted
                    console.print(f"  [green]{symbol}: +{inserted} bars repaired via IB[/green]")

                # Equity fallback for remaining gaps
                if asset_class == "equity":
                    # Re-check which gaps remain after IB merge
                    updated_dates = [date.fromisoformat(r["trade_date"]) for r in bronze.read_symbol_rows(symbol)]
                    remaining = find_interior_gaps(updated_dates, asset_class=asset_class)
                    remaining_set = set(remaining) & set(gaps)

                    if remaining_set:
                        from clients.daily_bar_fallback import DailyBarFallbackClient  # noqa: PLC0415
                        fallback = DailyBarFallbackClient()
                        fb_bars, _ = fetch_fallback_bars(symbol, sorted(remaining_set), fallback)
                        if fb_bars:
                            symbol_id = bronze.get_symbol_id(symbol)
                            fb_rows = bars_to_rows(fb_bars, symbol_id)
                            fb_inserted = bronze.merge_ticker_rows(symbol, fb_rows)
                            fallback_repaired += fb_inserted
                            console.print(
                                f"  [cyan]{symbol}: +{fb_inserted} bars repaired via fallback[/cyan]"
                            )

        total_repaired = repaired + fallback_repaired
        console.print(
            f"\n[bold]Summary:[/bold] {total_gaps} gap-days detected, "
            f"{total_repaired} repaired ({repaired} IB, {fallback_repaired} fallback)"
        )

        # Log to file
        with log_path.open("a", encoding="utf-8") as fh:
            fh.write(
                f"{today} health_check: asset_class={asset_class} "
                f"total_gaps={total_gaps} repaired={total_repaired}\n"
            )

        # Alert if threshold exceeded
        if total_repaired >= args.alert_threshold:
            _send_alert(str(today), asset_class, total_gaps, total_repaired, log_path)


if __name__ == "__main__":
    main()
