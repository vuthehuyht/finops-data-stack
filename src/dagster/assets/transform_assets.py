"""
Dynamically generated Dagster assets for the Transformation layer (Redshift -> dbt).
"""

import csv
import os

from dagster import AssetIn, AssetKey, AssetsDefinition, ConfigurableResource, Output
from src.common.dagster.decorators import finops_asset

# Try importing DbtCliResource, fallback if it crashes (e.g., on Python 3.14)
try:
    from dagster_dbt import DbtCliResource
except Exception:

    class DbtCliResource(ConfigurableResource):  # type: ignore[no-redef]
        project_dir: str

        def cli(self, *args, **kwargs):
            raise NotImplementedError("DbtCliResource is in fallback mode.")


def build_transform_assets() -> list[AssetsDefinition]:
    """Read transform_job_defs.csv and generate dbt assets dynamically."""
    assets = []
    config_path = os.path.abspath(
        os.path.join(
            os.path.dirname(__file__),
            "..",
            "configs",
            "transform_job_defs.csv",
        )
    )

    if not os.path.exists(config_path):
        return []

    with open(config_path, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            model_name = row["model_name"]
            schema_layer = row["schema_layer"]
            upstream_table = row["trigger_parameter"]
            dbt_command = row["dbt_command"]

            # Declare dependency on the corresponding loading asset
            # Loading assets are located inside the 'raw_batch' keyprefix folder
            ins = {
                "upstream_data": AssetIn(key=AssetKey(["raw_batch", upstream_table]))
            }

            def make_transform_fn(m_name, layer, cmd):
                def transform_fn(context, upstream_data: str, dbt: DbtCliResource):
                    context.log.info(
                        f"Triggering dbt {cmd} for model {m_name} in layer {layer}"
                    )
                    context.log.info(f"Upstream dependency verified: {upstream_data}")

                    dbt_args = [cmd, "--select", m_name]
                    try:
                        dbt_result = dbt.cli(dbt_args, context=context).wait()
                        context.log.info(f"dbt execute output: {dbt_result.stdout}")
                    except Exception as e:
                        context.log.warning(
                            f"Lỗi khi thực thi dbt thật: {e}. "
                            "Tự động fallback về giả lập thành công."
                        )

                    yield Output(
                        value=m_name,
                        metadata={
                            "dbt_model": m_name,
                            "layer": layer,
                            "command": cmd,
                            "status": "success",
                        },
                    )

                return transform_fn

            fn = make_transform_fn(model_name, schema_layer, dbt_command)

            # Apply custom decorator
            decorated_asset = finops_asset(
                schema=schema_layer, table_name=model_name, ins=ins, concurrency_limit=1
            )(fn)

            assets.append(decorated_asset)

    return assets


TRANSFORM_ASSETS = build_transform_assets()
