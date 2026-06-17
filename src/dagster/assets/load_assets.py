"""
Dynamically generated Dagster assets for the Loading layer (S3 Bronze -> Redshift Raw).
"""

import csv
import os

from dagster import AssetIn, AssetKey, AssetsDefinition, Output
from src.common.dagster.decorators import finops_asset
from src.common.redshift import execute_redshift_copy


def build_load_assets() -> list[AssetsDefinition]:
    """Read load_job_defs.csv and generate Redshift loading assets dynamically."""
    assets = []
    config_path = os.path.abspath(
        os.path.join(
            os.path.dirname(__file__),
            "..",
            "configs",
            "load_job_defs.csv",
        )
    )

    if not os.path.exists(config_path):
        return []

    with open(config_path, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            table_name = row["table_name"]
            schema = row["schema"]
            s3_source_prefix = row["s3_source_prefix"]
            file_format = row["file_format"]
            upstream_asset = row["trigger_parameter"]

            # Declare dependency on the corresponding raw ingestion asset
            # Raw ingestion asset is located inside the 'bronze_raw' keyprefix folder
            ins = {"s3_uri": AssetIn(key=AssetKey(["bronze_raw", upstream_asset]))}

            def make_load_fn(t_name, sch, s3_prefix, fmt):
                def load_fn(context, s3_uri: str):
                    context.log.info(
                        f"Triggering Redshift COPY for {sch}.{t_name} "
                        f"from S3 URI: {s3_uri}"
                    )

                    # Execute database COPY statement
                    rows_loaded = execute_redshift_copy(
                        table_name=t_name,
                        schema=sch,
                        s3_source_uri=s3_uri,
                        file_format=fmt,
                        partition_date=context.partition_key
                        if context.has_partition_key
                        else "default",
                    )

                    context.log.info(f"Redshift load complete for {sch}.{t_name}.")

                    yield Output(
                        value=f"{sch}.{t_name}",
                        metadata={
                            "table": f"{sch}.{t_name}",
                            "s3_source_uri": s3_uri,
                            "file_format": fmt,
                            "rows_loaded_placeholder": rows_loaded,
                        },
                    )

                return load_fn

            fn = make_load_fn(table_name, schema, s3_source_prefix, file_format)

            # Apply custom decorator
            decorated_asset = finops_asset(
                schema=schema,
                table_name=table_name,
                s3_prefix=s3_source_prefix,
                ins=ins,
                concurrency_limit=1,
            )(fn)

            assets.append(decorated_asset)

    return assets


LOAD_ASSETS = build_load_assets()
