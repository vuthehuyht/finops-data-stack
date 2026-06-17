import os

from dagster_aws.s3 import S3Resource

from dagster import AssetSelection, ConfigurableResource, Definitions, define_asset_job

# Try importing DbtCliResource, fallback if it crashes (e.g., on Python 3.14)
try:
    from dagster_dbt import DbtCliResource
except Exception:

    class DbtCliResource(ConfigurableResource):  # type: ignore[no-redef]
        project_dir: str

        def cli(self, *args, **kwargs):
            raise NotImplementedError("DbtCliResource is in fallback mode.")


from src.common.dagster.hooks import finops_failure_hook
from src.dagster.assets.ingest_assets import INGEST_ASSETS
from src.dagster.assets.load_assets import LOAD_ASSETS
from src.dagster.assets.ml_assets import ML_ASSETS
from src.dagster.assets.transform_assets import TRANSFORM_ASSETS

# 1. Combine all dynamically loaded assets
all_assets = INGEST_ASSETS + LOAD_ASSETS + TRANSFORM_ASSETS + ML_ASSETS

# 2. Define jobs grouped by data layers using AssetSelection.
# This avoids string-based ANTLR parser crashes on Python 3.14.
ingest_job = define_asset_job(
    name="ingest_all_sources_job",
    selection=AssetSelection.assets(*INGEST_ASSETS) if INGEST_ASSETS else None,
    hooks={finops_failure_hook},
)

load_job = define_asset_job(
    name="load_all_to_redshift_job",
    selection=AssetSelection.assets(*LOAD_ASSETS) if LOAD_ASSETS else None,
    hooks={finops_failure_hook},
)

transform_job = define_asset_job(
    name="transform_all_dbt_models_job",
    selection=AssetSelection.assets(*TRANSFORM_ASSETS) if TRANSFORM_ASSETS else None,
    hooks={finops_failure_hook},
)

ml_job = define_asset_job(
    name="run_all_ml_pipelines_job",
    selection=AssetSelection.assets(*ML_ASSETS) if ML_ASSETS else None,
    hooks={finops_failure_hook},
)

# 3. Dynamic resource mapping based on environment
FINOPS_ENV = os.getenv("FINOPS_ENVIRONMENT", "dev").lower()

dbt_project_dir = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "../transform")
)
dbt_resource = DbtCliResource(project_dir=dbt_project_dir)

if FINOPS_ENV == "prod":
    s3_resource = S3Resource(
        region_name=os.getenv("AWS_DEFAULT_REGION", "ap-southeast-1")
    )
else:
    s3_resource = S3Resource(
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name=os.getenv("AWS_DEFAULT_REGION", "ap-southeast-1"),
    )

# 4. Expose central Dagster definitions
defs = Definitions(
    assets=all_assets,
    jobs=[j for j in [ingest_job, load_job, transform_job, ml_job] if j is not None],
    resources={
        "s3": s3_resource,
        "dbt": dbt_resource,
    },
)
