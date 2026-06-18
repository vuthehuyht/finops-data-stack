"""Base ingestion pipeline defining abstract lifecycle for S3 storage upload."""

import abc
import datetime
import logging
import os
import tempfile
import time
from typing import TYPE_CHECKING

import pandas as pd

from src.common.s3_util import upload_to_s3

if TYPE_CHECKING:
    from mypy_boto3_s3.client import S3Client

logger = logging.getLogger(__name__)


# Fallback list of VN30 symbols to ensure basic data ingestion runs
# when no symbols are explicitly provided at runtime.
DEFAULT_TICKER_SYMBOLS: list[str] = [
    "ACB", "BCM", "BID", "BVH", "CTG",
    "FPT", "GAS", "GVR", "HDB", "HPG",
    "MBB", "MSN", "MWG", "PLX", "PNJ",
    "POW", "SAB", "SHB", "SSB", "SSI",
    "STB", "TCB", "TPB", "VCB", "VHM",
    "VIC", "VJC", "VNM", "VPB", "VRE",
]


class BaseIngestPipeline(abc.ABC):
    """Abstract Base Class (ABC) mapping the 4-phase ingestion workflow."""

    def __init__(
        self,
        batch_date: str,
        symbols: list[str] | None = None,
        s3_client: "S3Client | None" = None,
        bucket_name: str = "finops-raw-dev",
    ) -> None:
        """Initialize pipeline parameters.

        Args:
            batch_date: Date partition in YYYY-MM-DD format.
            symbols: List of target stock symbols (if applicable).
            s3_client: Boto3 S3 Client instance.
            bucket_name: S3 bucket target name.
        """
        self.batch_date = batch_date
        self.symbols = symbols or []
        self.s3_client = s3_client
        self.bucket_name = bucket_name
        self.logger = logging.getLogger(self.__class__.__name__)

    @property
    @abc.abstractmethod
    def table_name(self) -> str:
        """Destination raw Redshift table name (e.g. RAW_STOCK_PRICE_EOD)."""
        pass

    @property
    @abc.abstractmethod
    def source_uri_prefix(self) -> str:
        """Source URI pointer representing data origin (e.g. api://vnstock/price)."""
        pass

    @property
    @abc.abstractmethod
    def schema_columns(self) -> list[str]:
        """Ordered list of business columns to retain (lowercase, snake_case).

        Columns defined here will be selected and reordered after uppercase
        conversion. Any extra columns returned by the API are dropped.
        Missing columns are filled with None and logged as warnings.
        """
        pass

    @abc.abstractmethod
    def fetch(self) -> pd.DataFrame:
        """Fetch raw records from the API client or crawler.

        Returns:
            A pandas DataFrame containing raw unformatted data.
        """
        pass

    def standardize(self, df: pd.DataFrame) -> pd.DataFrame:
        """Standardize column names, filter to schema, and inject metadata.

        Column selection order:
        1. Uppercase all column names from source.
        2. Retain only columns declared in `schema_columns` (uppercased).
        3. Fill any missing declared columns with None and warn.
        4. Inject _CONATA_* metadata columns at the end.

        Args:
            df: Raw DataFrame retrieved from fetch().

        Returns:
            Standardized DataFrame with schema-filtered columns and metadata.
        """
        self.logger.info("Standardizing DataFrame columns and metadata")

        # Avoid mutating the input DataFrame in place
        result_df = df.copy()
        result_df.columns = result_df.columns.str.upper()

        # Apply schema column filter: keep only declared columns, in order
        expected_cols = [col.upper() for col in self.schema_columns]
        available_cols = set(result_df.columns)

        missing = [col for col in expected_cols if col not in available_cols]
        if missing:
            self.logger.warning(
                "Schema columns missing from API response for %s: %s. "
                "Filling with None.",
                self.table_name,
                missing,
            )
            for col in missing:
                result_df[col] = None

        # Reorder to schema order, dropping any extra columns
        result_df = result_df[expected_cols]

        # Inject metadata columns
        result_df["BATCH_DATE"] = self.batch_date
        result_df["_CONATA_SOURCE"] = self.source_uri_prefix
        result_df["_CONATA_SOURCE_ROW_NUMBER"] = range(1, len(result_df) + 1)
        result_df["_CONATA_PARTITION_KEY"] = self.batch_date
        result_df["_CONATA_LOADED_AT"] = pd.Timestamp.now(tz=datetime.UTC)

        return result_df

    def serialize(self, df: pd.DataFrame) -> str:
        """Write DataFrame into a temporary local Parquet file.

        Args:
            df: Standardized DataFrame to serialize.

        Returns:
            Absolute local file path of the temporary Parquet file.
        """
        # Create a named temp file. Set delete=False so we can close the wrapper,
        # upload it, and clean it up manually under finally.
        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as temp_file:
            temp_path = temp_file.name

        self.logger.info("Serializing DataFrame to temporary path: %s", temp_path)
        df.to_parquet(temp_path, compression="snappy", index=False)
        return temp_path

    def upload(self, temp_file_path: str) -> str:
        """Upload local Parquet file to target S3 path.

        Args:
            temp_file_path: Absolute path to the local file.

        Returns:
            The uploaded S3 URL string.
        """
        unix_timestamp = int(time.time())
        # Folder structure:
        # raw/<table_name>/batch_date=<date>/<timestamp>/<table_name>.parquet
        s3_key = (
            f"raw/{self.table_name}/"
            f"batch_date={self.batch_date}/"
            f"{unix_timestamp}/{self.table_name}.parquet"
        )
        s3_url = f"s3://{self.bucket_name}/{s3_key}"

        self.logger.info("Uploading file to S3 destination: %s", s3_url)
        upload_to_s3(
            file_path=temp_file_path,
            output_s3_url=s3_url,
            s3_client=self.s3_client,
            logger=self.logger,
        )
        return s3_url

    def run(self) -> str:
        """Coordinate e2e ingestion lifecycle: fetch, standardize, serialize, upload.

        Returns:
            The destination S3 URL path if uploaded successfully,
            empty string if skipped.
        """
        self.logger.info(
            "Starting ingestion for table: %s (batch_date: %s)",
            self.table_name,
            self.batch_date,
        )
        temp_path: str | None = None
        try:
            df = self.fetch()
            if df.empty:
                self.logger.warning("Empty records fetched. Skipping subsequent steps.")
                return ""

            df = self.standardize(df)
            temp_path = self.serialize(df)
            s3_url = self.upload(temp_path)
            return s3_url
        finally:
            if temp_path and os.path.exists(temp_path):
                self.logger.debug("Cleaning up temporary local file: %s", temp_path)
                os.unlink(temp_path)
