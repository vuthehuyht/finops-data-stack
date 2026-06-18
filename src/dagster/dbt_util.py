"""Utility for dagster_dbt."""

import logging
import signal
import threading
from typing import Any, Literal

import dagster_dbt


def dbt_and(*args: str) -> str:
    """Return "and" selector of all selectors."""
    return ",".join(args)


def dbt_or(*args: str) -> str:
    """Return "or" selector of all selectors."""
    return " ".join(args)


def terminate_dbt_process(
    dbt_cli_invocation: dagster_dbt.DbtCliInvocation,
    logger: logging.Logger,
) -> None:
    """Terminate the dbt process."""
    # This implementation is based on _stream_stdout method of DbtCliInvocation.
    logger.info(
        "Forwarding interrupt signal to dbt command: `%s`.",
        dbt_cli_invocation.dbt_command,
    )
    dbt_cli_invocation.process.send_signal(signal.SIGINT)
    dbt_cli_invocation.process.wait(
        timeout=dbt_cli_invocation.termination_timeout_seconds
    )
    logger.info(
        "dbt process terminated with exit code `%s`.",
        dbt_cli_invocation.process.returncode,
    )


def try_get_artifact(
    dbt_cli_invocation: dagster_dbt.DbtCliInvocation,
    artifact_name: Literal["manifest.json"]
    | Literal["catalog.json"]
    | Literal["run_results.json"]
    | Literal["sources.json"],
) -> dict[str, Any]:
    """Try to get a dbt artifact from a DbtCliInvocation."""
    try:
        return dbt_cli_invocation.get_artifact(artifact_name)
    except FileNotFoundError:
        return {}


def wait_for_dbt_process(
    dbt_cli_invocation: dagster_dbt.DbtCliInvocation,
    timeout_seconds: float,
    logger: logging.Logger,
) -> bool:
    """Wait for the dbt process to finish and return True if successful.

    If the process does not finish within the timeout,
    terminate the process and return its success status.
    """

    def timeout_handler() -> None:
        timer.cancel()
        logger.warning(
            "dbt process did not finish within %s seconds. Terminating the process.",
            timeout_seconds,
        )
        dbt_cli_invocation.process.send_signal(signal.SIGINT)

    timer = threading.Timer(interval=timeout_seconds, function=timeout_handler)
    timer.start()
    try:
        return dbt_cli_invocation.is_successful()
    finally:
        timer.cancel()
