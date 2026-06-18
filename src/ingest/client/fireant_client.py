"""FireAnt REST API client for fetching analyst research reports."""

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
        token = self._login(email, password)
        self._headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }

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
        response = requests.post(
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
        response = requests.get(
            f"{_BASE_URL}/reports/search",
            headers=self._headers,
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
