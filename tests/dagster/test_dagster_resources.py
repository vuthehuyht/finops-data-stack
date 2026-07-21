"""Tests for Dagster resources."""

import os
import unittest.mock

import dagster


def test_redshift_resource_is_configurable_resource() -> None:
    from src.dagster.resources import RedshiftResource

    assert issubclass(RedshiftResource, dagster.ConfigurableResource)


def test_redshift_resource_has_get_connection() -> None:
    from src.dagster.resources import RedshiftResource

    r = RedshiftResource()
    assert hasattr(r, "get_connection")


def test_redshift_resource_get_connection_lifecycle() -> None:
    from src.dagster.resources import RedshiftResource

    with unittest.mock.patch(
        "src.dagster.resources.get_redshift_connection"
    ) as mock_get:
        mock_conn = unittest.mock.MagicMock()
        mock_get.return_value = mock_conn

        r = RedshiftResource()
        with r.get_connection() as conn:
            assert conn is mock_conn

        mock_conn.close.assert_called_once()


def test_s3_bucket_resource_default() -> None:
    from src.dagster.resources import S3BucketResource

    with unittest.mock.patch.dict(os.environ, {}, clear=False):
        os.environ.pop("FINOPS_RAW_BUCKET", None)
        r = S3BucketResource()
    assert r.raw_bucket == "finops-raw-dev"


def test_s3_bucket_resource_from_env() -> None:
    from src.dagster.resources import S3BucketResource

    with unittest.mock.patch.dict(os.environ, {"FINOPS_RAW_BUCKET": "my-bucket"}):
        r = S3BucketResource()
    assert r.raw_bucket == "my-bucket"


def test_dbt_config_resource_defaults() -> None:
    from src.dagster.resources import DbtConfigResource

    r = DbtConfigResource()
    assert r.full_refresh is False
    assert r.variables == {}


def test_load_job_config_resource_default() -> None:
    from src.dagster.resources import LoadJobConfigResource

    with unittest.mock.patch.dict(os.environ, {}, clear=False):
        os.environ.pop("REDSHIFT_IAM_ROLE_ARN", None)
        r = LoadJobConfigResource()
    assert r.iam_role_arn == ""


def test_sagemaker_resource_defaults() -> None:
    from src.dagster.resources import SageMakerResource

    with unittest.mock.patch.dict(os.environ, {}, clear=False):
        os.environ.pop("SAGEMAKER_EXECUTION_ROLE_ARN", None)
        os.environ.pop("FINOPS_MODEL_ARTIFACTS_BUCKET", None)
        r = SageMakerResource()
    assert r.execution_role_arn == ""
    assert r.model_artifacts_bucket == "finops-model-artifacts-dev"


def test_sagemaker_resource_from_env() -> None:
    from src.dagster.resources import SageMakerResource

    with unittest.mock.patch.dict(
        os.environ,
        {
            "SAGEMAKER_EXECUTION_ROLE_ARN": "arn:aws:iam::123:role/sm",
            "FINOPS_MODEL_ARTIFACTS_BUCKET": "my-artifacts",
        },
    ):
        r = SageMakerResource()
    assert r.execution_role_arn == "arn:aws:iam::123:role/sm"
    assert r.model_artifacts_bucket == "my-artifacts"


def test_ssm_parameter_resource_get_parameter_returns_value() -> None:
    from src.dagster.resources import SsmParameterResource

    mock_client = unittest.mock.MagicMock()
    mock_client.exceptions.ParameterNotFound = Exception
    mock_client.get_parameter.return_value = {"Parameter": {"Value": "v42"}}

    with unittest.mock.patch(
        "src.dagster.resources.boto3.client", return_value=mock_client
    ):
        r = SsmParameterResource()
        value = r.get_parameter("/finops/model/active_version")

    assert value == "v42"
    mock_client.get_parameter.assert_called_once_with(
        Name="/finops/model/active_version"
    )


def test_ssm_parameter_resource_get_parameter_returns_none_when_missing() -> None:
    from src.dagster.resources import SsmParameterResource

    class _ParameterNotFound(Exception):
        pass

    mock_client = unittest.mock.MagicMock()
    mock_client.exceptions.ParameterNotFound = _ParameterNotFound
    mock_client.get_parameter.side_effect = _ParameterNotFound()

    with unittest.mock.patch(
        "src.dagster.resources.boto3.client", return_value=mock_client
    ):
        r = SsmParameterResource()
        value = r.get_parameter("/finops/model/active_version")

    assert value is None


def test_ssm_parameter_resource_put_parameter() -> None:
    from src.dagster.resources import SsmParameterResource

    mock_client = unittest.mock.MagicMock()

    with unittest.mock.patch(
        "src.dagster.resources.boto3.client", return_value=mock_client
    ):
        r = SsmParameterResource()
        r.put_parameter("/finops/model/active_version", "v43")

    mock_client.put_parameter.assert_called_once_with(
        Name="/finops/model/active_version",
        Value="v43",
        Type="String",
        Overwrite=True,
    )


_TEST_INFERENCE_IMAGE = (
    "763104351884.dkr.ecr.ap-southeast-1.amazonaws.com/pytorch-inference:2.2-cpu-py310"
)
_TEST_MODEL_DATA_URI = "s3://bucket/finops-multimodal-regressor-20260703/model.tar.gz"


def test_sagemaker_resource_create_model_if_not_exists() -> None:
    import botocore.exceptions

    from src.dagster.resources import SageMakerResource

    mock_client = unittest.mock.MagicMock()
    mock_client.exceptions.ClientError = botocore.exceptions.ClientError
    err = botocore.exceptions.ClientError(
        error_response={
            "Error": {"Code": "ValidationException", "Message": "Could not find"}
        },
        operation_name="DescribeModel",
    )
    mock_client.describe_model.side_effect = err

    with unittest.mock.patch(
        "src.dagster.resources.boto3.client", return_value=mock_client
    ):
        r = SageMakerResource(execution_role_arn="arn:aws:iam::123:role/sm")
        r.create_model_if_not_exists(
            model_name="my-model",
            model_data_s3_uri=_TEST_MODEL_DATA_URI,
            inference_image=_TEST_INFERENCE_IMAGE,
        )

    mock_client.create_model.assert_called_once_with(
        ModelName="my-model",
        PrimaryContainer={
            "Image": _TEST_INFERENCE_IMAGE,
            "ModelDataUrl": _TEST_MODEL_DATA_URI,
            "Environment": {
                "SAGEMAKER_PROGRAM": "serve.py",
                "SAGEMAKER_SUBMIT_DIRECTORY": "/opt/ml/model/code",
            },
        },
        ExecutionRoleArn="arn:aws:iam::123:role/sm",
    )


def test_sagemaker_resource_run_batch_transform_job() -> None:
    from src.dagster.resources import SageMakerResource

    mock_client = unittest.mock.MagicMock()
    mock_client.describe_transform_job.side_effect = [
        {"TransformJobStatus": "InProgress"},
        {"TransformJobStatus": "Completed"},
    ]

    with (
        unittest.mock.patch(
            "src.dagster.resources.boto3.client", return_value=mock_client
        ),
        unittest.mock.patch("src.dagster.resources.time.sleep") as mock_sleep,
    ):
        r = SageMakerResource()
        r.run_batch_transform_job(
            job_name="my-job",
            model_name="my-model",
            input_s3_uri="s3://in/in.jsonl",
            output_s3_uri="s3://out/",
        )

    mock_client.create_transform_job.assert_called_once_with(
        TransformJobName="my-job",
        ModelName="my-model",
        TransformInput={
            "DataSource": {
                "S3DataSource": {
                    "S3DataType": "S3Prefix",
                    "S3Uri": "s3://in/in.jsonl",
                }
            },
            "ContentType": "application/json",
            "SplitType": "Line",
        },
        TransformOutput={
            "S3OutputPath": "s3://out/",
            "AssembleWith": "Line",
        },
        TransformResources={
            "InstanceType": "ml.m5.large",
            "InstanceCount": 1,
        },
    )
    mock_sleep.assert_called_once_with(10)


def test_module_singletons_importable() -> None:
    from src.dagster import resources

    assert resources.redshift is not None
    assert resources.s3 is not None
    assert resources.s3bucket is not None
    assert resources.dbt is not None
    assert resources.dbt_config is not None
    assert resources.load_config is not None
    assert resources.sagemaker_config is not None
    assert resources.ssm is not None
