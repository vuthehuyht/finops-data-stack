"""Dagster resources for FinOps pipeline."""

import contextlib
import os
import pathlib
from collections.abc import Iterator
from typing import Any

import boto3
import dagster
from dagster_aws.s3 import S3Resource
from dagster_dbt import DbtCliResource
from pydantic import Field

from src.common.redshift_util import get_redshift_connection

_PROJECT_ROOT = pathlib.Path(__file__).parent.parent.parent


class RedshiftResource(dagster.ConfigurableResource):
    """Redshift connection resource — delegates to get_redshift_connection()."""

    @contextlib.contextmanager
    def get_connection(self) -> Iterator[Any]:
        """Open a psycopg2 connection to Redshift Serverless."""
        conn = get_redshift_connection()
        try:
            yield conn
        finally:
            conn.close()


class S3BucketResource(dagster.ConfigurableResource):
    """S3 bucket name for raw (Bronze) data."""

    raw_bucket: str = Field(
        default_factory=lambda: os.getenv("FINOPS_RAW_BUCKET", "finops-raw-dev"),
        description="S3 bucket holding Bronze layer Parquet files.",
    )


class DbtConfigResource(dagster.ConfigurableResource):
    """Runtime dbt configuration passed to each transform job."""

    variables: dict[str, str] = Field(default_factory=dict)
    full_refresh: bool = Field(default=False)
    no_data_test: bool = Field(default=False)
    no_materialization: bool = Field(default=False)
    use_prod_upstream: bool = Field(default=False)
    days_offset_for_output_diff: int = Field(default=0)
    empty: bool = Field(default=False)


class LoadJobConfigResource(dagster.ConfigurableResource):
    """IAM configuration for Redshift COPY from S3."""

    iam_role_arn: str = Field(
        default_factory=lambda: os.getenv("REDSHIFT_IAM_ROLE_ARN", ""),
        description="IAM Role ARN authorizing Redshift to read from S3.",
    )


class SageMakerResource(dagster.ConfigurableResource):
    """SageMaker execution role and target bucket for the ML training pipeline."""

    execution_role_arn: str = Field(
        default_factory=lambda: os.getenv("SAGEMAKER_EXECUTION_ROLE_ARN", ""),
        description="IAM Role ARN SageMaker assumes to run training jobs.",
    )
    model_artifacts_bucket: str = Field(
        default_factory=lambda: os.getenv(
            "FINOPS_MODEL_ARTIFACTS_BUCKET", "finops-model-artifacts-dev"
        ),
        description="S3 bucket storing versioned model.tar.gz + metadata.json.",
    )


class SsmParameterResource(dagster.ConfigurableResource):
    """SSM Parameter Store access for ML model version tracking."""

    region_name: str = Field(default="ap-southeast-1")

    def get_parameter(self, name: str) -> str | None:
        """Return the parameter value, or None if it does not exist."""
        client = boto3.client("ssm", region_name=self.region_name)
        try:
            response = client.get_parameter(Name=name)
        except client.exceptions.ParameterNotFound:
            return None
        return response["Parameter"]["Value"]

    def put_parameter(self, name: str, value: str) -> None:
        """Create or overwrite a String SSM parameter."""
        client = boto3.client("ssm", region_name=self.region_name)
        client.put_parameter(Name=name, Value=value, Type="String", Overwrite=True)


dbt = DbtCliResource(
    project_dir=os.fspath(_PROJECT_ROOT / "src" / "transform" / "dbt"),
)
s3 = S3Resource(region_name="ap-southeast-1")
s3bucket = S3BucketResource()
redshift = RedshiftResource()
dbt_config = DbtConfigResource()
load_config = LoadJobConfigResource()
sagemaker_config = SageMakerResource()
ssm = SsmParameterResource()
