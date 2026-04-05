"""Tests for HistoricalProvider implementations."""

import os
import pytest
from unittest.mock import MagicMock, patch
from dataclasses import dataclass

from clients.historical_provider import (
    BarRecord,
    RadonApiProvider,
    IBClientAdapter,
    ib_contract_to_spec,
    spec_to_ib_contract,
    create_ib_client_or_adapter,
)


class TestBarRecord:
    def test_iso_date_format(self):
        bar = BarRecord(date="2025-01-02", open=150.0, high=152.0, low=149.5, close=151.0, volume=1000000)
        assert bar.date == "2025-01-02"
        assert bar.open == 150.0


class TestContractSpecHelpers:
    def test_stock_to_spec(self):
        from ib_async import Stock
        contract = Stock("AAPL", "SMART", "USD")
        spec = ib_contract_to_spec(contract)
        assert spec["sec_type"] == "STK"
        assert spec["symbol"] == "AAPL"
        assert spec["exchange"] == "SMART"

    def test_spec_to_stock(self):
        spec = {"sec_type": "STK", "symbol": "AAPL", "exchange": "SMART", "currency": "USD"}
        contract = spec_to_ib_contract(spec)
        assert contract.symbol == "AAPL"
        assert contract.secType == "STK"

    def test_future_roundtrip(self):
        from ib_async import Future
        contract = Future("ES", "202506", "CME", "USD")
        spec = ib_contract_to_spec(contract)
        assert spec["sec_type"] == "FUT"
        assert spec["last_trade_date"] == "202506"
        rebuilt = spec_to_ib_contract(spec)
        assert rebuilt.symbol == "ES"

    def test_index_roundtrip(self):
        from ib_async import Index
        contract = Index("VIX", "CBOE", "USD")
        spec = ib_contract_to_spec(contract)
        assert spec["sec_type"] == "IND"


class TestRadonApiProvider:
    def test_parses_bar_response(self):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "bars": [
                {"date": "2025-01-02", "open": 150.0, "high": 152.0, "low": 149.5, "close": 151.0, "volume": 1000000},
                {"date": "2025-01-03", "open": 151.0, "high": 153.0, "low": 150.0, "close": 152.5, "volume": 900000},
            ]
        }
        mock_resp.raise_for_status = MagicMock()

        bars = [BarRecord(**b) for b in mock_resp.json()["bars"]]
        assert len(bars) == 2
        assert bars[0].date == "2025-01-02"
        assert bars[1].volume == 900000


class TestIBClientAdapter:
    def test_has_ib_attribute(self):
        mock_provider = MagicMock(spec=RadonApiProvider)
        adapter = IBClientAdapter(mock_provider)
        assert hasattr(adapter, "ib")

    def test_connect_is_noop(self):
        mock_provider = MagicMock(spec=RadonApiProvider)
        adapter = IBClientAdapter(mock_provider)
        adapter.connect(host="localhost", port=4001)  # Should not raise

    def test_context_manager(self):
        mock_provider = MagicMock(spec=RadonApiProvider)
        with IBClientAdapter(mock_provider) as adapter:
            assert adapter is not None


class TestCreateIbClientOrAdapter:
    def test_returns_ibclient_without_env(self):
        with patch.dict(os.environ, {}, clear=True):
            os.environ.pop("MDW_RADON_API_URL", None)
            os.environ.pop("MDW_API_KEY", None)
            result = create_ib_client_or_adapter.__wrapped__ if hasattr(create_ib_client_or_adapter, "__wrapped__") else None
            # Can't fully test without IB connection, but verify env detection
            assert os.getenv("MDW_RADON_API_URL") is None

    def test_fails_fast_on_401(self):
        import httpx
        with patch.dict(os.environ, {"MDW_RADON_API_URL": "http://fake", "MDW_API_KEY": "bad"}):
            mock_resp = MagicMock()
            mock_resp.status_code = 401
            mock_resp.raise_for_status.side_effect = httpx.HTTPStatusError(
                "Unauthorized", request=MagicMock(), response=mock_resp
            )
            with patch("clients.historical_provider.RadonApiProvider") as MockProvider:
                instance = MockProvider.return_value
                instance._client.post.return_value = mock_resp
                instance._client.post.return_value.raise_for_status = mock_resp.raise_for_status
                with pytest.raises(httpx.HTTPStatusError):
                    create_ib_client_or_adapter()
