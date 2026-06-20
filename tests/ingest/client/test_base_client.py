"""Unit tests for the BaseClient retry and rate limiting capabilities."""

import time
from unittest.mock import Mock
import pytest

from src.ingest.client.base_client import BaseClient


def test_base_client_retry_success_eventually() -> None:
    """Verify that BaseClient retries on failure and returns when eventually successful."""
    client = BaseClient(request_delay_seconds=0.0)  # disable delay for fast test
    mock_func = Mock()

    # Fail twice, then succeed on the third attempt
    mock_func.side_effect = [
        ValueError("Temporary Error 1"),
        ValueError("Temporary Error 2"),
        "Success Value",
    ]

    result = client.call_api_with_retry(mock_func)

    assert result == "Success Value"
    assert mock_func.call_count == 3


def test_base_client_retry_exhausted() -> None:
    """Verify that BaseClient propagates the error when retries are exhausted."""
    client = BaseClient(request_delay_seconds=0.0)
    mock_func = Mock()

    # Always fail
    mock_func.side_effect = ValueError("Fatal Error")

    with pytest.raises(ValueError, match="Fatal Error"):
        client.call_api_with_retry(mock_func)

    assert mock_func.call_count == 3


def test_base_client_rate_limiting() -> None:
    """Verify that BaseClient enforces request delay spacing."""
    delay = 0.2
    client = BaseClient(request_delay_seconds=delay)
    mock_func = Mock(return_value="OK")

    start_time = time.time()

    # Call twice. The second call should apply rate limiting and delay the execution.
    client.call_api_with_retry(mock_func)
    client.call_api_with_retry(mock_func)

    duration = time.time() - start_time
    assert duration >= delay
    assert mock_func.call_count == 2
