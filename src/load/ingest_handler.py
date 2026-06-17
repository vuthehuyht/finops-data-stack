"""
Main ingestion entrypoint executing crawlers and uploading data to AWS S3 dynamically.
"""

import importlib
import logging
import os
from datetime import datetime

from src.load.s3_uploader import save_and_upload_df

logger = logging.getLogger(__name__)


def run_ingest(
    source_client: str, api_method: str, s3_key_prefix: str, params: dict
) -> str:
    """Dynamically invoke specified API crawler method and upload
    returned DataFrame to S3.

    Args:
        source_client (str): Client module name (e.g. 'vnstock').
        api_method (str): Method name in module (e.g. 'fetch_eod_prices').
        s3_key_prefix (str): Destination folder prefix.
        params (dict): API arguments.

    Returns:
        str: S3 URI of the uploaded dataset.
    """
    logger.info(
        f"Triggering ingestion. Source client: {source_client}, API: {api_method}"
    )

    # 1. Dynamically import client module
    try:
        module_path = f"src.load.clients.{source_client}"
        client_module = importlib.import_module(module_path)
    except ModuleNotFoundError as e:
        logger.error(
            f"Ingestion client '{source_client}' not found in src.load.clients: {e}"
        )
        raise e

    # 2. Extract method reference
    try:
        method_ref = getattr(client_module, api_method)
    except AttributeError as e:
        logger.error(
            f"API Method '{api_method}' not found inside module '{source_client}': {e}"
        )
        raise e

    # 3. Invoke cào dữ liệu
    try:
        df = method_ref(**params)
    except Exception as e:
        logger.error(
            "Error occurred during execution of crawler "
            f"{source_client}.{api_method}: {e}"
        )
        raise e

    # Ép toàn bộ kiểu dữ liệu nghiệp vụ gốc (raw business columns) sang String
    for col in df.columns:
        df[col] = df[col].astype(str)

    # Tiêm 5 trường metadata quản lý dữ liệu (Bronze Layer)
    batch_date_str = datetime.now().strftime("%Y-%m-%d")
    loaded_at_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    partition_key = datetime.now().strftime("%Y%m%d")

    df["BATCH_DATE"] = batch_date_str
    df["_CONATA_SOURCE"] = source_client.upper()
    df["_CONATA_SOURCE_ROW_NUMBER"] = list(range(1, len(df) + 1))
    df["_CONATA_PARTITION_KEY"] = partition_key
    df["_CONATA_LOADED_AT"] = loaded_at_str

    # 4. Upload to S3 (Bronze Raw)
    s3_bucket = os.getenv("FINOPS_DATA_LAKE_RAW", "finops-dev-data-lake-raw")

    # Generate structured filename based on execution timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    file_name = f"{source_client}_{api_method}_{timestamp}"

    try:
        s3_uri = save_and_upload_df(
            df=df,
            s3_bucket=s3_bucket,
            s3_key_prefix=s3_key_prefix,
            file_name=file_name,
            file_format="parquet",  # Defaulting to parquet format for Bronze raw lake
        )
        return s3_uri
    except Exception as e:
        logger.error(f"Failed to upload crawled dataset to S3 bucket {s3_bucket}: {e}")
        raise e
