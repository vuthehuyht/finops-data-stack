"""
Dynamically generated Dagster assets for the Machine Learning layer.
Redshift Marts -> SageMaker ML.
"""

import csv
import json
import os

from dagster import AssetIn, AssetKey, AssetsDefinition, Output
from src.common.dagster.decorators import finops_asset


def build_ml_assets() -> list[AssetsDefinition]:
    """Read ml_job_defs.csv and generate ML assets dynamically."""
    assets = []
    config_path = os.path.abspath(
        os.path.join(
            os.path.dirname(__file__),
            "..",
            "configs",
            "ml_job_defs.csv",
        )
    )

    if not os.path.exists(config_path):
        return []

    with open(config_path, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            pipeline_name = row["pipeline_name"]
            task_type = row["task_type"]

            try:
                upstream_list = json.loads(row["upstream_assets"])
            except Exception:
                upstream_list = []

            # Declare dependencies dynamically on transform marts assets
            ins = {}
            for index, upstream in enumerate(upstream_list):
                ins[f"upstream_{index}"] = AssetIn(key=AssetKey(["marts", upstream]))

            # Unique asset name combining pipeline name and task type
            asset_name = f"{pipeline_name}_{task_type}"

            def make_ml_fn(p_name, task, ups):
                def ml_fn(context, **kwargs):
                    context.log.info(f"Triggering ML Pipeline '{p_name}' Task: {task}")
                    context.log.info(f"Verified inputs: {ups}")

                    # Mocking ML training / prediction logic for local development
                    # In production, this executes python src/ml/train.py or predict.py
                    # and returns S3 URI model artifact.
                    s3_artifact = f"s3://finops-dev-model-artifacts/models/{p_name}/{task}/model.tar.gz"
                    context.log.info(f"Saved ML artifact metadata to S3: {s3_artifact}")

                    yield Output(
                        value=s3_artifact,
                        metadata={
                            "pipeline": p_name,
                            "task": task,
                            "s3_artifact": s3_artifact,
                            "status": "success",
                        },
                    )

                return ml_fn

            fn = make_ml_fn(pipeline_name, task_type, upstream_list)

            # Apply custom decorator
            decorated_asset = finops_asset(
                schema="ml_pipelines",
                table_name=asset_name,
                ins=ins,
                concurrency_limit=1,
            )(fn)

            assets.append(decorated_asset)

    return assets


ML_ASSETS = build_ml_assets()
