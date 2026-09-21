"""One-off script: download the pretrained sentiment model, bundle it with
the serving code, and upload model.tar.gz to the SageMaker model artifacts
bucket. Run manually, not from Dagster — there is no training step for
Phase A.

Usage:
    uv run --group nlp python -m src.nlp.package_model --bucket finops-model-artifacts
"""

import argparse
import json
import os
import shutil
import tarfile
import tempfile

import boto3
from transformers import AutoModelForSequenceClassification, AutoTokenizer

from src.nlp.config import MODEL_ID, SENTIMENT_SCHEMA_VERSION

_SERVING_FILES = ("serve.py", "config.py", "sentiment_model.py", "requirements.txt")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--bucket", required=True, help="S3 bucket for model.tar.gz")
    parser.add_argument(
        "--key-prefix",
        default=f"nlp-sentiment/{MODEL_ID.replace('/', '_')}/",
        help="S3 key prefix to upload model.tar.gz under.",
    )
    return parser.parse_args()


def package_and_upload(bucket: str, key_prefix: str) -> str:
    """Download MODEL_ID, bundle serving code, upload model.tar.gz.

    Returns:
        The full s3:// URI of the uploaded model.tar.gz.
    """
    source_dir = os.path.dirname(os.path.abspath(__file__))
    with tempfile.TemporaryDirectory() as tmpdir:
        model_dir = os.path.join(tmpdir, "model")
        os.makedirs(model_dir)

        tokenizer = AutoTokenizer.from_pretrained(MODEL_ID)
        model = AutoModelForSequenceClassification.from_pretrained(MODEL_ID)
        tokenizer.save_pretrained(model_dir)
        model.save_pretrained(model_dir)

        with open(os.path.join(model_dir, "metadata.json"), "w", encoding="utf-8") as f:
            json.dump(
                {
                    "model_version": MODEL_ID,
                    "sentiment_schema_version": SENTIMENT_SCHEMA_VERSION,
                },
                f,
            )

        code_dir = os.path.join(model_dir, "code")
        os.makedirs(code_dir)
        for filename in _SERVING_FILES:
            shutil.copy(
                os.path.join(source_dir, filename), os.path.join(code_dir, filename)
            )

        tarball_path = os.path.join(tmpdir, "model.tar.gz")
        with tarfile.open(tarball_path, "w:gz") as tar:
            for entry in os.listdir(model_dir):
                tar.add(os.path.join(model_dir, entry), arcname=entry)

        key = f"{key_prefix.rstrip('/')}/model.tar.gz"
        boto3.client("s3").upload_file(tarball_path, bucket, key)
        return f"s3://{bucket}/{key}"


def main() -> None:
    args = _parse_args()
    s3_uri = package_and_upload(args.bucket, args.key_prefix)
    print(f"Uploaded model artifact to {s3_uri}")


if __name__ == "__main__":
    main()
