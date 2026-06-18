"""Unit tests for FireAntClient — login, pagination, and error handling."""

from unittest.mock import MagicMock, patch

import pytest
import requests

from src.ingest.client.fireant_client import _PAGE_SIZE, FireAntClient


def _make_client(token: str = "test-token") -> FireAntClient:
    """Build a FireAntClient with login mocked out."""
    with patch.object(FireAntClient, "_login", return_value=token):
        return FireAntClient(
            email="user@test.com", password="pass", request_delay_seconds=0.0
        )


# ---------------------------------------------------------------------------
# _login
# ---------------------------------------------------------------------------


@patch("src.ingest.client.fireant_client.requests.post")
def test_login_returns_token_from_token_key(mock_post: MagicMock) -> None:
    """Login response with 'token' key returns that value."""
    mock_post.return_value.json.return_value = {"token": "abc123"}
    mock_post.return_value.raise_for_status = MagicMock()

    client = FireAntClient.__new__(FireAntClient)
    super(FireAntClient, client).__init__(request_delay_seconds=0.0)
    token = client._login("user@test.com", "pass")

    assert token == "abc123"


@patch("src.ingest.client.fireant_client.requests.post")
def test_login_returns_token_from_access_token_key(mock_post: MagicMock) -> None:
    """Login response with 'accessToken' key is used as fallback."""
    mock_post.return_value.json.return_value = {"accessToken": "xyz789"}
    mock_post.return_value.raise_for_status = MagicMock()

    client = FireAntClient.__new__(FireAntClient)
    super(FireAntClient, client).__init__(request_delay_seconds=0.0)
    token = client._login("user@test.com", "pass")

    assert token == "xyz789"


@patch("src.ingest.client.fireant_client.requests.post")
def test_login_raises_value_error_when_no_token_in_response(
    mock_post: MagicMock,
) -> None:
    """Login succeeds HTTP-wise but response has no token field — raises ValueError."""
    mock_post.return_value.json.return_value = {"userId": 42, "email": "user@test.com"}
    mock_post.return_value.raise_for_status = MagicMock()

    client = FireAntClient.__new__(FireAntClient)
    super(FireAntClient, client).__init__(request_delay_seconds=0.0)

    with pytest.raises(ValueError, match="no token found"):
        client._login("user@test.com", "pass")


@patch("src.ingest.client.fireant_client.requests.post")
def test_login_raises_http_error_on_bad_credentials(mock_post: MagicMock) -> None:
    """HTTP 401 from login propagates as HTTPError."""
    mock_post.return_value.raise_for_status.side_effect = requests.HTTPError(
        "401 Unauthorized"
    )

    client = FireAntClient.__new__(FireAntClient)
    super(FireAntClient, client).__init__(request_delay_seconds=0.0)

    with pytest.raises(requests.HTTPError):
        client._login("bad@test.com", "wrong")


# ---------------------------------------------------------------------------
# _fetch_page
# ---------------------------------------------------------------------------


@patch("src.ingest.client.fireant_client.requests.get")
def test_fetch_page_sends_correct_params(mock_get: MagicMock) -> None:
    """_fetch_page calls /reports/search with the expected query params."""
    mock_get.return_value.json.return_value = {"total": 0, "reports": []}
    mock_get.return_value.raise_for_status = MagicMock()

    client = _make_client()
    client._fetch_page("TCB", "2026-06-01", "2026-06-01", offset=0)

    mock_get.assert_called_once()
    _, kwargs = mock_get.call_args
    assert kwargs["params"]["symbol"] == "TCB"
    assert kwargs["params"]["startDate"] == "2026-06-01"
    assert kwargs["params"]["endDate"] == "2026-06-01"
    assert kwargs["params"]["offset"] == 0
    assert kwargs["params"]["limit"] == _PAGE_SIZE


@patch("src.ingest.client.fireant_client.requests.get")
def test_fetch_page_raises_on_http_error(mock_get: MagicMock) -> None:
    """HTTP error from /reports/search propagates as HTTPError."""
    mock_get.return_value.raise_for_status.side_effect = requests.HTTPError("500")

    client = _make_client()

    with pytest.raises(requests.HTTPError):
        client._fetch_page("TCB", "2026-06-01", "2026-06-01", offset=0)


# ---------------------------------------------------------------------------
# get_reports
# ---------------------------------------------------------------------------


@patch("src.ingest.client.fireant_client.requests.get")
def test_get_reports_returns_all_on_single_page(mock_get: MagicMock) -> None:
    """All reports fit on one page — no second request made."""
    reports = [{"reportID": 1, "symbol": "TCB"}, {"reportID": 2, "symbol": "TCB"}]
    mock_get.return_value.json.return_value = {"total": 2, "reports": reports}
    mock_get.return_value.raise_for_status = MagicMock()

    client = _make_client()
    result = client.get_reports("TCB", "2026-06-01", "2026-06-01")

    assert result == reports
    assert mock_get.call_count == 1


@patch("src.ingest.client.fireant_client.requests.get")
def test_get_reports_paginates_until_total_reached(mock_get: MagicMock) -> None:
    """Reports spanning two pages are concatenated correctly."""
    page1 = [{"reportID": i} for i in range(_PAGE_SIZE)]
    page2 = [{"reportID": i} for i in range(_PAGE_SIZE, _PAGE_SIZE + 30)]

    mock_get.return_value.raise_for_status = MagicMock()
    mock_get.return_value.json.side_effect = [
        {"total": _PAGE_SIZE + 30, "reports": page1},
        {"total": _PAGE_SIZE + 30, "reports": page2},
    ]

    client = _make_client()
    result = client.get_reports("TCB", "2026-06-01", "2026-06-01")

    assert len(result) == _PAGE_SIZE + 30
    assert mock_get.call_count == 2
    # Second call must use correct offset
    second_call_params = mock_get.call_args_list[1][1]["params"]
    assert second_call_params["offset"] == _PAGE_SIZE


@patch("src.ingest.client.fireant_client.requests.get")
def test_get_reports_stops_on_empty_page(mock_get: MagicMock) -> None:
    """Stops pagination when an empty page is returned even if total > collected."""
    page1 = [{"reportID": i} for i in range(10)]

    mock_get.return_value.raise_for_status = MagicMock()
    mock_get.return_value.json.side_effect = [
        {"total": 999, "reports": page1},
        {"total": 999, "reports": []},
    ]

    client = _make_client()
    result = client.get_reports("TCB", "2026-06-01", "2026-06-01")

    assert len(result) == 10
    assert mock_get.call_count == 2


@patch("src.ingest.client.fireant_client.requests.get")
def test_get_reports_returns_empty_list_when_no_results(mock_get: MagicMock) -> None:
    """API returns zero reports — result is empty list, one request made."""
    mock_get.return_value.json.return_value = {"total": 0, "reports": []}
    mock_get.return_value.raise_for_status = MagicMock()

    client = _make_client()
    result = client.get_reports("TCB", "2026-06-01", "2026-06-01")

    assert result == []
    assert mock_get.call_count == 1


@patch("src.ingest.client.fireant_client.requests.get")
def test_get_reports_bearer_token_sent_in_header(mock_get: MagicMock) -> None:
    """Requests include Authorization header with the login token."""
    mock_get.return_value.json.return_value = {"total": 0, "reports": []}
    mock_get.return_value.raise_for_status = MagicMock()

    client = _make_client(token="my-secret-token")
    client.get_reports("TCB", "2026-06-01", "2026-06-01")

    _, kwargs = mock_get.call_args
    assert kwargs["headers"]["Authorization"] == "Bearer my-secret-token"
