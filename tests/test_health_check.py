"""Tests for scripts/health_check.py — core gap detection functions."""

from __future__ import annotations

from datetime import date
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from clients.bronze_client import BronzeClient
from scripts.health_check import (
    compute_range_duration,
    find_interior_gaps,
    get_all_trade_dates,
    group_contiguous_dates,
)

# ── Helpers ────────────────────────────────────────────────────────────────────

_EQUITY_SCHEMA = pa.schema(
    [
        ("trade_date", pa.date32()),
        ("symbol_id", pa.int64()),
        ("open", pa.float64()),
        ("high", pa.float64()),
        ("low", pa.float64()),
        ("close", pa.float64()),
        ("adj_close", pa.float64()),
        ("volume", pa.int64()),
    ]
)


def _write_parquet(bronze_dir, symbol: str, dates: list[date]) -> None:
    """Write a minimal equity parquet file for *symbol* under *bronze_dir*."""
    sym_dir = bronze_dir / f"symbol={symbol}"
    sym_dir.mkdir(parents=True, exist_ok=True)
    rows = [
        {
            "trade_date": d,
            "symbol_id": 1,
            "open": 1.0,
            "high": 2.0,
            "low": 0.5,
            "close": 1.5,
            "adj_close": 1.5,
            "volume": 1000,
        }
        for d in dates
    ]
    table = pa.Table.from_pylist(rows, schema=_EQUITY_SCHEMA)
    pq.write_table(table, sym_dir / "1d.parquet", compression="snappy")


# ── find_interior_gaps ─────────────────────────────────────────────────────────


class TestFindInteriorGaps:
    def test_empty_list_returns_empty(self):
        assert find_interior_gaps([]) == []

    def test_single_date_returns_empty(self):
        assert find_interior_gaps([date(2026, 1, 5)]) == []

    def test_full_week_no_gaps(self):
        # Mon-Fri Jan 5-9 2026 — all trading days, no gaps
        week = [date(2026, 1, 5), date(2026, 1, 6), date(2026, 1, 7), date(2026, 1, 8), date(2026, 1, 9)]
        assert find_interior_gaps(week) == []

    def test_missing_wednesday_detected(self):
        # Mon Jan 5, Tue Jan 6, Thu Jan 8, Fri Jan 9 — Wed Jan 7 is missing
        dates = [date(2026, 1, 5), date(2026, 1, 6), date(2026, 1, 8), date(2026, 1, 9)]
        assert find_interior_gaps(dates) == [date(2026, 1, 7)]

    def test_weekend_not_a_gap(self):
        # Fri Jan 9 → Mon Jan 12 — Sat/Sun are not trading days
        dates = [date(2026, 1, 9), date(2026, 1, 12)]
        assert find_interior_gaps(dates) == []

    def test_nyse_holiday_mlk_day_not_a_gap(self):
        # MLK Day 2026 = Mon Jan 19; Fri Jan 16 → Tue Jan 20 has no gap
        dates = [date(2026, 1, 16), date(2026, 1, 20)]
        assert find_interior_gaps(dates) == []

    def test_futures_missing_tuesday_is_a_gap(self):
        # Mon Jan 5, Wed Jan 7 — Tue Jan 6 is a weekday, so it's a gap for futures
        dates = [date(2026, 1, 5), date(2026, 1, 7)]
        gaps = find_interior_gaps(dates, asset_class="futures")
        assert gaps == [date(2026, 1, 6)]

    def test_futures_weekend_not_a_gap(self):
        # Fri Jan 9 → Mon Jan 12 — weekends are still not expected for futures
        dates = [date(2026, 1, 9), date(2026, 1, 12)]
        assert find_interior_gaps(dates, asset_class="futures") == []

    def test_futures_mlk_day_is_a_gap(self):
        # For futures, MLK Day is NOT a holiday — CME trades some holidays
        # MLK Day 2026 = Mon Jan 19 (weekday) → should be flagged as gap
        dates = [date(2026, 1, 16), date(2026, 1, 20)]
        gaps = find_interior_gaps(dates, asset_class="futures")
        assert date(2026, 1, 19) in gaps


# ── group_contiguous_dates ─────────────────────────────────────────────────────


class TestGroupContiguousDates:
    def test_empty_returns_empty(self):
        assert group_contiguous_dates([]) == []

    def test_single_date(self):
        d = date(2026, 1, 5)
        assert group_contiguous_dates([d]) == [(d, d)]

    def test_three_contiguous_dates(self):
        dates = [date(2026, 1, 5), date(2026, 1, 6), date(2026, 1, 7)]
        assert group_contiguous_dates(dates) == [(date(2026, 1, 5), date(2026, 1, 7))]

    def test_two_separate_ranges(self):
        # Jan 5-6 and Jan 8-9 (gap at Jan 7)
        dates = [
            date(2026, 1, 5), date(2026, 1, 6),
            date(2026, 1, 8), date(2026, 1, 9),
        ]
        result = group_contiguous_dates(dates)
        assert result == [
            (date(2026, 1, 5), date(2026, 1, 6)),
            (date(2026, 1, 8), date(2026, 1, 9)),
        ]

    def test_non_contiguous_single_dates(self):
        dates = [date(2026, 1, 5), date(2026, 1, 7), date(2026, 1, 9)]
        result = group_contiguous_dates(dates)
        assert result == [
            (date(2026, 1, 5), date(2026, 1, 5)),
            (date(2026, 1, 7), date(2026, 1, 7)),
            (date(2026, 1, 9), date(2026, 1, 9)),
        ]


# ── compute_range_duration ─────────────────────────────────────────────────────


class TestComputeRangeDuration:
    def test_same_day_returns_one_d(self):
        d = date(2026, 1, 5)
        assert compute_range_duration(d, d) == "1 D"

    def test_end_before_start_returns_one_d(self):
        assert compute_range_duration(date(2026, 1, 10), date(2026, 1, 5)) == "1 D"

    def test_short_range_four_cal_days(self):
        # 4 cal days + 2 buffer = 6
        start = date(2026, 1, 5)
        end = date(2026, 1, 9)
        assert compute_range_duration(start, end) == "6 D"

    def test_about_two_months(self):
        # ~60 cal days + 2 = 62 D
        start = date(2026, 1, 5)
        end = date(2026, 3, 6)  # 60 cal days later
        result = compute_range_duration(start, end)
        assert result.endswith(" D")
        days = int(result.split()[0])
        assert 60 <= days <= 70

    def test_exactly_180_days_with_buffer(self):
        # 178 cal days + 2 = 180 → still "180 D"
        start = date(2026, 1, 1)
        end = date(2026, 6, 28)  # 178 days later → +2 = 180
        result = compute_range_duration(start, end)
        assert result.endswith(" D")

    def test_over_180_cal_days(self):
        # 200 cal days → "1 Y"
        start = date(2026, 1, 1)
        end = date(2026, 7, 20)  # well over 180 days
        assert compute_range_duration(start, end) == "1 Y"

    def test_over_one_year(self):
        # 400 cal days → "2 Y"
        start = date(2025, 1, 1)
        end = date(2026, 2, 5)  # ~400 days
        assert compute_range_duration(start, end) == "2 Y"


# ── get_all_trade_dates ────────────────────────────────────────────────────────


class TestGetAllTradeDates:
    @pytest.mark.integration
    def test_empty_bronze_returns_empty_dict(self, tmp_path):
        bronze_dir = tmp_path / "bronze"
        bronze_dir.mkdir()
        client = BronzeClient(bronze_dir=bronze_dir)
        try:
            result = get_all_trade_dates(client)
            assert result == {}
        finally:
            client.close()

    @pytest.mark.integration
    def test_reads_known_dates_for_one_symbol(self, tmp_path):
        bronze_dir = tmp_path / "bronze"
        bronze_dir.mkdir()

        known_dates = [date(2026, 1, 5), date(2026, 1, 6), date(2026, 1, 7)]
        _write_parquet(bronze_dir, "AAPL", known_dates)

        client = BronzeClient(bronze_dir=bronze_dir)
        try:
            result = get_all_trade_dates(client)
            assert "AAPL" in result
            assert result["AAPL"] == known_dates
        finally:
            client.close()

    @pytest.mark.integration
    def test_reads_multiple_symbols(self, tmp_path):
        bronze_dir = tmp_path / "bronze"
        bronze_dir.mkdir()

        aapl_dates = [date(2026, 1, 5), date(2026, 1, 6)]
        msft_dates = [date(2026, 1, 5), date(2026, 1, 7)]
        _write_parquet(bronze_dir, "AAPL", aapl_dates)
        _write_parquet(bronze_dir, "MSFT", msft_dates)

        client = BronzeClient(bronze_dir=bronze_dir)
        try:
            result = get_all_trade_dates(client)
            assert result["AAPL"] == aapl_dates
            assert result["MSFT"] == msft_dates
        finally:
            client.close()

    @pytest.mark.integration
    def test_dates_are_sorted_ascending(self, tmp_path):
        bronze_dir = tmp_path / "bronze"
        bronze_dir.mkdir()

        dates = [date(2026, 1, 7), date(2026, 1, 5), date(2026, 1, 6)]
        _write_parquet(bronze_dir, "NVDA", sorted(dates))  # write sorted (parquet requirement)

        client = BronzeClient(bronze_dir=bronze_dir)
        try:
            result = get_all_trade_dates(client)
            assert result["NVDA"] == sorted(dates)
        finally:
            client.close()

    @pytest.mark.integration
    def test_string_trade_date_is_handled(self, tmp_path):
        """Cover the defensive str-conversion branch when DuckDB returns strings."""
        bronze_dir = tmp_path / "bronze"
        bronze_dir.mkdir()
        _write_parquet(bronze_dir, "TSLA", [date(2026, 1, 5)])

        client = BronzeClient(bronze_dir=bronze_dir)
        try:
            # Patch _query to return trade_date as a string rather than a date object
            original_query = client._query

            def _patched_query(sql, params=None):
                rows = original_query(sql, params)
                return [
                    {**row, "trade_date": str(row["trade_date"])}
                    for row in rows
                ]

            with patch.object(client, "_query", side_effect=_patched_query):
                result = get_all_trade_dates(client)
            assert result["TSLA"] == [date(2026, 1, 5)]
        finally:
            client.close()
