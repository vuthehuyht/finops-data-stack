import signal
import time
from unittest.mock import MagicMock

from src.dagster import dbt_util


def _make_invocation(process: MagicMock) -> MagicMock:
    invocation = MagicMock()
    invocation.dbt_command = "dbt test"
    invocation.process = process
    invocation.termination_timeout_seconds = 5
    return invocation


def test_dbt_or() -> None:
    """Test dbt_or."""
    assert dbt_util.dbt_or("a", "b") == "a b"


def test_dbt_and() -> None:
    """Test dbt_and."""
    assert dbt_util.dbt_and("a", "b") == "a,b"


def test_terminate_dbt_process() -> None:
    process = MagicMock()
    process.returncode = 0
    invocation = _make_invocation(process)
    logger = MagicMock()

    dbt_util.terminate_dbt_process(invocation, logger)

    process.send_signal.assert_called_once_with(signal.SIGINT)
    process.wait.assert_called_once_with(timeout=invocation.termination_timeout_seconds)
    logger.info.assert_called()


def test_wait_for_dbt_process_success() -> None:
    """Test wait_for_dbt_process when the process finishes successfully."""
    process = MagicMock()
    invocation = _make_invocation(process)
    # Simulate a fast, successful process
    invocation.is_successful.return_value = True
    logger = MagicMock()
    result = dbt_util.wait_for_dbt_process(invocation, timeout_seconds=5, logger=logger)
    # The call should report success
    assert result is True
    invocation.is_successful.assert_called()
    # The timer should have been cancelled; no termination signal should be sent
    process.send_signal.assert_not_called()
    # No timeout warning should be logged on a successful, timely completion
    logger.warning.assert_not_called()


def test_wait_for_dbt_process_timeout() -> None:
    """Test wait_for_dbt_process when the process times out."""
    process = MagicMock()
    invocation = _make_invocation(process)

    # Simulate a slow process that triggers the timeout
    def slow_is_successful() -> bool:
        time.sleep(0.2)  # Sleep longer than the timeout
        return False

    invocation.is_successful.side_effect = slow_is_successful
    logger = MagicMock()

    result = dbt_util.wait_for_dbt_process(
        invocation, timeout_seconds=0.1, logger=logger
    )

    assert result is False
    invocation.is_successful.assert_called_once()
    # The timer should have triggered and sent SIGINT during the wait
    process.send_signal.assert_called_once_with(signal.SIGINT)
    logger.warning.assert_called_once()
    assert "did not finish within" in str(logger.warning.call_args)
