"""FireAnt REST API client for fetching analyst research reports."""

import os

import requests

from src.ingest.client.base_client import BaseClient

_BASE_URL = "https://api.fireant.vn"
_PAGE_SIZE = 100


class FireAntClient(BaseClient):
    """Client for fetching analyst reports from the FireAnt API.

    Authenticates via email/password on init, then uses the returned Bearer
    token for subsequent requests.
    """

    def __init__(
        self, email: str, password: str, request_delay_seconds: float = 1.0
    ) -> None:
        """Initialize FireAntClient and authenticate immediately.

        Args:
            email: FireAnt account email.
            password: FireAnt account password.
            request_delay_seconds: Spacing delay between requests.
        """
        super().__init__(request_delay_seconds=request_delay_seconds)
        self._session = requests.Session()
        proxy_url = os.environ.get("FIREANT_PROXY_URL")
        if proxy_url:
            self._session.proxies.update({"http": proxy_url, "https": proxy_url})

        token = self._login(email, password)
        self._session.headers.update(
            {
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
            }
        )

    def _login(self, email: str, password: str) -> str:
        """Authenticate with FireAnt and return the access token.

        Args:
            email: FireAnt account email.
            password: FireAnt account password.

        Returns:
            Bearer access token string.

        Raises:
            ValueError: If the login response does not contain a token.
            requests.HTTPError: If the login request fails.
        """
        response = self._session.post(
            f"{_BASE_URL}/authentication/login",
            json={"email": email, "password": password, "rememberMe": True},
            timeout=30,
        )
        response.raise_for_status()
        data = response.json()
        # FireAnt login response returns token under 'token' key
        token = data.get("token") or data.get("accessToken")
        if not token:
            raise ValueError(
                f"FireAnt login succeeded but no token found in response. "
                f"Keys returned: {list(data.keys())}"
            )
        return token

    def _fetch_page(
        self, symbol: str, start_date: str, end_date: str, offset: int
    ) -> dict:
        """Fetch a single page from /reports/search.

        Args:
            symbol: Stock ticker (e.g. TCB).
            start_date: Start date in YYYY-MM-DD format.
            end_date: End date in YYYY-MM-DD format.
            offset: Pagination offset.

        Returns:
            Raw JSON response dict with keys 'total' and 'reports'.
        """
        response = self._session.get(
            f"{_BASE_URL}/reports/search",
            params={
                "symbol": symbol,
                "startDate": start_date,
                "endDate": end_date,
                "offset": offset,
                "limit": _PAGE_SIZE,
            },
            timeout=30,
        )
        response.raise_for_status()
        return response.json()

    def get_reports(self, symbol: str, start_date: str, end_date: str) -> list[dict]:
        """Fetch all analyst reports for a symbol within a date range.

        Paginates automatically until all records are retrieved.

        Args:
            symbol: Stock ticker (e.g. TCB).
            start_date: Start date in YYYY-MM-DD format.
            end_date: End date in YYYY-MM-DD format.

        Returns:
            List of ReportInfo dicts from the FireAnt API.
        """
        all_reports: list[dict] = []
        offset = 0

        while True:
            data = self.call_api_with_retry(
                self._fetch_page, symbol, start_date, end_date, offset
            )
            page = data.get("reports") or []
            all_reports.extend(page)

            if len(all_reports) >= data.get("total", 0) or not page:
                break
            offset += _PAGE_SIZE

        return all_reports

    def _fetch_quotes_page(
        self, symbol: str, start_date: str, end_date: str, offset: int
    ) -> list[dict]:
        """Fetch a single page of historical quotes from FireAnt API."""
        response = self._session.get(
            f"{_BASE_URL}/symbols/{symbol}/historical-quotes",
            params={
                "startDate": start_date,
                "endDate": end_date,
                "offset": offset,
                "limit": _PAGE_SIZE,
            },
            timeout=30,
        )
        if response.status_code == 404:
            return []
        response.raise_for_status()
        return response.json()

    def get_historical_quotes(
        self, symbol: str, start_date: str, end_date: str
    ) -> list[dict]:
        """Fetch historical quotes (contains foreign and proprietary flow)."""
        all_quotes: list[dict] = []
        offset = 0

        while True:
            page = self.call_api_with_retry(
                self._fetch_quotes_page, symbol, start_date, end_date, offset
            )
            if not page:
                break
            all_quotes.extend(page)

            # If the page is smaller than the limit, it's the last page.
            if len(page) < _PAGE_SIZE:
                break
            offset += _PAGE_SIZE

        return all_quotes
