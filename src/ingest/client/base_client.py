"""Base API client logic with retry and rate limiting capabilities."""

import time
from collections.abc import Callable
from typing import ParamSpec, TypeVar

from dagster import get_dagster_logger
from tenacity import retry, stop_after_attempt, wait_exponential

logger = get_dagster_logger()

P = ParamSpec("P")
R = TypeVar("R")


class BaseClient:
    """Base client configuration managing request safety, delay, and retries."""

    def __init__(self, request_delay_seconds: float = 1.0) -> None:
        """Initialize base client with rate limiting configuration.

        Args:
            request_delay_seconds: Minimum time in seconds between requests.
        """
        self.request_delay_seconds = request_delay_seconds
        self._last_request_time = 0.0

    def _apply_rate_limit(self) -> None:
        """Enforce rate limits by checking time elapsed since last call."""
        elapsed = time.time() - self._last_request_time
        if elapsed < self.request_delay_seconds:
            sleep_time = self.request_delay_seconds - elapsed
            logger.debug("Rate limiting: waiting %.2f seconds", sleep_time)
            time.sleep(sleep_time)
        self._last_request_time = time.time()

    @retry(
        stop=stop_after_attempt(6),
        wait=wait_exponential(multiplier=2, min=5, max=60),
        reraise=True,
    )
    def call_api_with_retry(
        self,
        func: Callable[P, R],
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> R:
        """Execute the API call wrapping it with automatic retry and rate limits.

        Args:
            func: Target function or method to invoke.
            *args: Positional arguments for target function.
            **kwargs: Keyword arguments for target function.

        Returns:
            The return value of the target function.
        """
        self._apply_rate_limit()
        try:
            return func(*args, **kwargs)
        except Exception as e:
            logger.warning("API call failed: %s. Retrying...", e)
            raise e
        except SystemExit as e:
            logger.warning(
                "API call triggered sys.exit (possibly rate limit): %s. Retrying...", e
            )
            raise RuntimeError(f"SystemExit raised: {e}") from e
