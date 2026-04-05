"""Core gap detection and duration computation for the warehouse health check."""

from __future__ import annotations

import logging
from datetime import date, timedelta

from clients import BronzeClient
from scripts.daily_update import is_trading_day

log = logging.getLogger(__name__)


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
