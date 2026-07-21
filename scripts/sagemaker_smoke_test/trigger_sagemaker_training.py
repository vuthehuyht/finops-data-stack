"""Standalone smoke-test trigger for training the FinOps multimodal
regressor on a real SageMaker Training Job.

Independent of src/ml/training_job.py and src/dagster/ml_job.py on purpose
— this is a disposable script for validating that src/ml/train.py runs
correctly inside the SageMaker PyTorch container, before relying on the
production Dagster pipeline. Requires the role ARN and bucket name output
by scripts/sagemaker_smoke_test/cdk/ (see that folder's README section in
the design spec for how to obtain them).
"""

import argparse
import datetime
import os

import boto3
from sagemaker.core import image_uris
from sagemaker.core.helper.session_helper import Session
from sagemaker.core.shapes.shapes import OutputDataConfig
from sagemaker.core.training.configs import Compute, InputData, SourceCode
from sagemaker.train.model_trainer import ModelTrainer

from scripts.dataset_builder import FinOpsDatasetBuilder

_FRAMEWORK_VERSION = "2.6.0"
_PY_VERSION = "py312"
_REGION = "ap-southeast-1"


def _parse_args() -> argparse.Namespace:
    today = datetime.date.today()
    parser = argparse.ArgumentParser(
        description=(
            "Smoke-test: upload local training data to S3 and launch a "
            "real SageMaker training job using src/ml/train.py."
        )
    )
    parser.add_argument(
        "--role-arn",
        required=True,
        help="SageMaker execution role ARN (CDK output ExecutionRoleArn).",
    )
    parser.add_argument(
        "--bucket",
        required=True,
        help="S3 bucket for data + model artifacts (CDK output BucketName).",
    )
    parser.add_argument(
        "--raw-data-dir",
        default="data/raw",
        help="Local raw data directory used to rebuild features before training.",
    )
    parser.add_argument(
        "--data-path",
        default="data/processed/features.parquet",
        help=(
            "Local Parquet file re-built from --raw-data-dir and then "
            "uploaded as the training channel."
        ),
    )
    parser.add_argument(
        "--instance-type",
        default="ml.m5.large",
        help="SageMaker training instance type (CPU by default for a smoke test).",
    )
    parser.add_argument(
        "--epochs",
        type=int,
        default=3,
        help="Training epochs (kept low for a smoke test).",
    )
    parser.add_argument(
        "--train-end-date",
        default=(today - datetime.timedelta(days=180)).isoformat(),
        help="Train/validation split cutoff, YYYY-MM-DD.",
    )
    parser.add_argument(
        "--val-end-date",
        default=(today - datetime.timedelta(days=90)).isoformat(),
        help="Validation/test split cutoff, YYYY-MM-DD.",
    )
    return parser.parse_args()


def _build_features(raw_data_dir: str, data_path: str) -> None:
    """Rebuild the Parquet feature set at data_path from raw_data_dir.

    Reuses scripts/dataset_builder.py unmodified — the same feature builder
    already used to produce the data the local training run was validated
    against — so the smoke test trains on the current raw data, not a
    possibly-stale cached Parquet file.
    """
    output_dir = os.path.dirname(data_path) or "."
    FinOpsDatasetBuilder(raw_dir=raw_data_dir, output_dir=output_dir).build_dataset()


def _upload_training_data(data_path: str, bucket: str, timestamp: str) -> str:
    """Upload the local Parquet file to S3 and return the channel's S3 prefix."""
    filename = os.path.basename(data_path)
    key = f"ml/train_data/{timestamp}/{filename}"
    boto3.client("s3").upload_file(data_path, bucket, key)
    return f"s3://{bucket}/ml/train_data/{timestamp}/"


def _launch_training_job(
    role_arn: str,
    bucket: str,
    input_s3_uri: str,
    instance_type: str,
    hyperparameters: dict[str, str],
) -> tuple[str, str]:
    """Launch and block on a SageMaker training job.

    Returns:
        (training_job_name, model_data_s3_uri).
    """
    training_image = image_uris.retrieve(
        framework="pytorch",
        region=_REGION,
        version=_FRAMEWORK_VERSION,
        py_version=_PY_VERSION,
        instance_type=instance_type,
        image_scope="training",
    )
    # Without an explicit default_bucket, ModelTrainer uploads the packaged
    # source_code to SageMaker's auto-created session bucket
    # (sagemaker-{region}-{account}), which the sandbox execution role has
    # no S3 permissions on (it's scoped to only the sandbox bucket) — that
    # mismatch fails CreateTrainingJob with a ListBucket AccessDenied. Point
    # the session at the sandbox bucket so everything the role touches is
    # inside its granted permissions.
    sagemaker_session = Session(default_bucket=bucket)
    trainer = ModelTrainer(
        training_image=training_image,
        source_code=SourceCode(source_dir="src/ml", entry_script="train.py"),
        compute=Compute(instance_type=instance_type, instance_count=1),
        role=role_arn,
        base_job_name="finops-ml-smoke-test",
        hyperparameters=hyperparameters,
        sagemaker_session=sagemaker_session,
        output_data_config=OutputDataConfig(
            s3_output_path=f"s3://{bucket}/ml/model_artifacts"
        ),
    )
    trainer.train(
        input_data_config=[InputData(channel_name="train", data_source=input_s3_uri)],
        wait=True,
    )
    # ModelTrainer.train() only exposes the completed job via this
    # attribute (same access pattern already used in src/ml/training_job.py).
    training_job = trainer._latest_training_job
    return (
        training_job.training_job_name,
        training_job.model_artifacts.s3_model_artifacts,
    )


def main() -> None:
    args = _parse_args()
    timestamp = datetime.datetime.now(datetime.UTC).strftime("%Y%m%d-%H%M%S")

    print(f"Building features from {args.raw_data_dir} into {args.data_path} ...")
    _build_features(args.raw_data_dir, args.data_path)
    print("Features built.")

    print(
        f"Uploading {args.data_path} to "
        f"s3://{args.bucket}/ml/train_data/{timestamp}/ ..."
    )
    input_s3_uri = _upload_training_data(args.data_path, args.bucket, timestamp)
    print(f"Uploaded. Training channel: {input_s3_uri}")

    hyperparameters = {
        "train-end-date": args.train_end_date,
        "val-end-date": args.val_end_date,
        "epochs": str(args.epochs),
    }
    print(f"Launching SageMaker training job on {args.instance_type} ...")
    job_name, model_data_s3_uri = _launch_training_job(
        role_arn=args.role_arn,
        bucket=args.bucket,
        input_s3_uri=input_s3_uri,
        instance_type=args.instance_type,
        hyperparameters=hyperparameters,
    )
    print(f"Training job finished: {job_name}")
    print(f"Model artifact: {model_data_s3_uri}")


if __name__ == "__main__":
    main()
