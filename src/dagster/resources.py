"""Dagster resources for FinOps pipeline."""

import contextlib
import os
import pathlib
import time
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
    """SageMaker execution role, target bucket, and Batch Transform job management."""

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
    region_name: str = Field(default="ap-southeast-1")

    def create_model_if_not_exists(
        self,
        model_name: str,
        model_data_s3_uri: str,
        inference_image: str,
    ) -> None:
        """Create a SageMaker Model resource if it does not already exist."""
        client = boto3.client("sagemaker", region_name=self.region_name)
        model_exists = False
        try:
            client.describe_model(ModelName=model_name)
            model_exists = True
        except client.exceptions.ClientError as exc:
            is_val = "ValidationException" in str(exc)
            if "Could not find" not in str(exc) and not is_val:
                raise exc

        if not model_exists:
            client.create_model(
                ModelName=model_name,
                PrimaryContainer={
                    "Image": inference_image,
                    "ModelDataUrl": model_data_s3_uri,
                    "Environment": {
                        "SAGEMAKER_PROGRAM": "serve.py",
                        "SAGEMAKER_SUBMIT_DIRECTORY": "/opt/ml/model/code",
                    },
                },
                ExecutionRoleArn=self.execution_role_arn,
            )

    def run_batch_transform_job(
        self,
        job_name: str,
        model_name: str,
        input_s3_uri: str,
        output_s3_uri: str,
        instance_type: str = "ml.m5.large",
    ) -> None:
        """Launch a SageMaker Batch Transform Job and block until it completes."""
        client = boto3.client("sagemaker", region_name=self.region_name)
        client.create_transform_job(
            TransformJobName=job_name,
            ModelName=model_name,
            TransformInput={
                "DataSource": {
                    "S3DataSource": {
                        "S3DataType": "S3Prefix",
                        "S3Uri": input_s3_uri,
                    }
                },
                # SplitType=Line sends each line as a separate request whose body
                # is one JSON object, not the whole JSON Lines file -- keep this
                # as application/json, matching src/ml/serve.py::input_fn.
                "ContentType": "application/json",
                "SplitType": "Line",
            },
            TransformOutput={
                "S3OutputPath": output_s3_uri,
                "AssembleWith": "Line",
            },
            TransformResources={
                "InstanceType": instance_type,
                "InstanceCount": 1,
            },
        )

        while True:
            desc = client.describe_transform_job(TransformJobName=job_name)
            status = desc["TransformJobStatus"]
            if status == "Completed":
                break
            elif status in ("Failed", "Stopped"):
                failure_reason = desc.get("FailureReason", "unknown reason")
                raise RuntimeError(
                    f"SageMaker Batch Transform Job {job_name} "
                    f"ended with status {status}: {failure_reason}"
                )
            time.sleep(10)


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
