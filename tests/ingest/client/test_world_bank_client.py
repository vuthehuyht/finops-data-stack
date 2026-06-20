"""Unit tests for WorldBankClient."""

from unittest.mock import MagicMock, patch

import httpx
import pytest

from src.ingest.client.world_bank_client import WorldBankClient


def _mock_response(data: object, status_code: int = 200) -> MagicMock:
    mock = MagicMock(spec=httpx.Response)
    mock.status_code = status_code
    mock.json.return_value = data
    if status_code >= 400:
        mock.raise_for_status.side_effect = httpx.HTTPStatusError(
            "error", request=MagicMock(), response=mock
        )
    else:
        mock.raise_for_status.return_value = None
    return mock


@patch("src.ingest.client.world_bank_client.httpx.get")
def test_world_bank_client_get_indicator_returns_records(
    mock_get: MagicMock,
) -> None:
    """Verify get_indicator returns the data array from the World Bank response."""
    records = [
        {"date": "2024", "value": 430000000000.0, "unit": ""},
        {"date": "2023", "value": 410000000000.0, "unit": ""},
    ]
    mock_get.return_value = _mock_response([{}, records])

    client = WorldBankClient(request_delay_seconds=0)
    result = client.get_indicator("NY.GDP.MKTP.CD")

    assert result == records


@patch("src.ingest.client.world_bank_client.httpx.get")
def test_world_bank_client_get_indicator_calls_correct_url(
    mock_get: MagicMock,
) -> None:
    """Verify get_indicator constructs the URL with country code and series ID."""
    mock_get.return_value = _mock_response([{}, []])

    client = WorldBankClient(request_delay_seconds=0)
    client.get_indicator("NY.GDP.MKTP.CD", country_code="VN", mrv=5)

    call_kwargs = mock_get.call_args
    assert "VN/indicator/NY.GDP.MKTP.CD" in call_kwargs.args[0]
    assert call_kwargs.kwargs["params"]["format"] == "json"
    assert call_kwargs.kwargs["params"]["mrv"] == 5


@patch("src.ingest.client.world_bank_client.httpx.get")
def test_world_bank_client_get_indicator_returns_empty_when_no_records(
    mock_get: MagicMock,
) -> None:
    """Verify get_indicator returns empty list when World Bank has no data."""
    mock_get.return_value = _mock_response([{}, []])

    client = WorldBankClient(request_delay_seconds=0)
    result = client.get_indicator("NY.GDP.MKTP.CD")

    assert result == []


@patch("src.ingest.client.world_bank_client.httpx.get")
def test_world_bank_client_get_indicator_returns_empty_when_malformed(
    mock_get: MagicMock,
) -> None:
    """Verify get_indicator returns empty list when response has no data element."""
    mock_get.return_value = _mock_response([{}])

    client = WorldBankClient(request_delay_seconds=0)
    result = client.get_indicator("NY.GDP.MKTP.CD")

    assert result == []


@patch("src.ingest.client.world_bank_client.httpx.get")
def test_world_bank_client_get_indicator_raises_on_http_error(
    mock_get: MagicMock,
) -> None:
    """Verify get_indicator raises on non-2xx HTTP response."""
    mock_get.return_value = _mock_response({}, status_code=503)

    client = WorldBankClient(request_delay_seconds=0)
    with pytest.raises(httpx.HTTPStatusError):
        client.get_indicator("NY.GDP.MKTP.CD")
