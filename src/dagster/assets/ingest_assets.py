"""
Dynamically generated Dagster assets for the Ingestion layer.
External sources -> S3 Bronze.
"""

import csv
import json
import os

from dagster import AssetsDefinition, Output
from src.common.dagster.decorators import finops_asset
from src.load.ingest_handler import run_ingest


def build_ingest_assets() -> list[AssetsDefinition]:
    """Read ingest_job_defs.csv and generate Dagster assets dynamically."""
    assets = []
    config_path = os.path.abspath(
        os.path.join(
            os.path.dirname(__file__),
            "..",
            "configs",
            "ingest_job_defs.csv",
        )
    )

    if not os.path.exists(config_path):
        return []

    with open(config_path, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            asset_name = row["asset_name"]
            source_client = row["source_client"]
            api_method = row["api_method"]
            s3_prefix = row["s3_key_prefix"]

            try:
                params = json.loads(row["default_params"])
            except Exception:
                params = {}

            # Function factory to capture current loop variables
            def make_ingest_fn(client, method, prefix, p):
                def ingest_fn(context):
                    context.log.info(f"Triggering ingestion for {client}.{method}")
                    s3_uri = run_ingest(client, method, prefix, p)
                    context.log.info(
                        f"Ingestion finished. Output saved to S3 path: {s3_uri}"
                    )

                    # Yield standard output with metadata
                    yield Output(
                        value=s3_uri,
                        metadata={
                            "s3_uri": s3_uri,
                            "source_client": client,
                            "api_method": method,
                        },
                    )

                return ingest_fn

            fn = make_ingest_fn(source_client, api_method, s3_prefix, params)

            # Apply custom decorator to register asset properly
            decorated_asset = finops_asset(
                schema="bronze_raw",
                table_name=asset_name,
                s3_prefix=s3_prefix,
                concurrency_limit=1,
            )(fn)

            assets.append(decorated_asset)

    return assets


INGEST_ASSETS = build_ingest_assets()
