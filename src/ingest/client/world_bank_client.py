"""World Bank API client for fetching Vietnam macro indicators."""

import httpx

from src.ingest.client.base_client import BaseClient

_BASE_URL = "https://api.worldbank.org/v2"


class WorldBankClient(BaseClient):
    """Client for the World Bank public API (no auth required)."""

    def get_indicator(
        self,
        series_id: str,
        country_code: str = "VN",
        mrv: int = 5,
    ) -> list[dict]:
        """Fetch the most recent values for a World Bank indicator.

        Args:
            series_id: World Bank series code (e.g. 'NY.GDP.MKTP.CD').
            country_code: ISO 2-letter country code.
            mrv: Number of most-recent values to retrieve.

        Returns:
            List of raw data dicts from the World Bank response.
        """
        url = f"{_BASE_URL}/country/{country_code}/indicator/{series_id}"

        def _fetch() -> list[dict]:
            response = httpx.get(
                url,
                params={"format": "json", "mrv": mrv},
                timeout=30,
            )
            response.raise_for_status()
            data = response.json()
            # data[0] = pagination metadata, data[1] = list of records
            if len(data) < 2 or not data[1]:
                return []
            return data[1]

        return self.call_api_with_retry(_fetch)
