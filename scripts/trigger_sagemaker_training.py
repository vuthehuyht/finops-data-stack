import argparse
import logging

import sagemaker
from sagemaker.pytorch import PyTorch

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(description="Trigger SageMaker Training Job")
    parser.add_argument(
        "--role-arn", type=str, required=True, help="IAM Role ARN for SageMaker"
    )
    parser.add_argument(
        "--bucket", type=str, required=True, help="S3 Bucket for data and artifacts"
    )
    parser.add_argument(
        "--data-dir",
        type=str,
        default="data/processed",
        help="Local directory containing training data",
    )
    parser.add_argument(
        "--instance-type",
        type=str,
        default="ml.g4dn.xlarge",
        help="SageMaker instance type",
    )
    parser.add_argument(
        "--epochs", type=int, default=5, help="Number of epochs for testing"
    )
    args = parser.parse_args()

    # 1. Initialize SageMaker session
    logger.info("Initializing SageMaker session...")
    sagemaker_session = sagemaker.Session(default_bucket=args.bucket)

    # 2. Upload local processed data to S3
    s3_data_prefix = "ml/train_data"
    logger.info(
        f"Uploading local data from {args.data_dir} to s3://{args.bucket}/{s3_data_prefix}..."
    )

    train_input = sagemaker_session.upload_data(
        path=args.data_dir, bucket=args.bucket, key_prefix=s3_data_prefix
    )
    logger.info(f"Data uploaded to: {train_input}")

    # 3. Define the PyTorch Estimator
    logger.info(f"Configuring PyTorch Estimator on {args.instance_type}...")
    estimator = PyTorch(
        entry_point="train.py",
        source_dir="src/ml",
        role=args.role_arn,
        framework_version="2.0.0",
        py_version="py310",
        instance_count=1,
        instance_type=args.instance_type,
        sagemaker_session=sagemaker_session,
        hyperparameters={
            "epochs": args.epochs,
            "batch-size": 64,
            "learning-rate": 0.001,
            # Dates matching our 5-year data span
            "train-end-date": "2024-12-31",
            "val-end-date": "2025-12-31",
        },
        output_path=f"s3://{args.bucket}/ml/model_artifacts",
        disable_profiler=True,
    )

    # 4. Trigger the Training Job
    logger.info("Starting SageMaker Training Job. Check AWS Console for logs...")
    estimator.fit({"train": train_input})
    logger.info("Training Job completed successfully!")


if __name__ == "__main__":
    main()
