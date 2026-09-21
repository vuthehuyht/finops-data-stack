"""SageMaker Training Job launcher for the sentiment fine-tuning pipeline.

Mirrors src/ml/training_job.py (same ModelTrainer v3 API and the same
model_artifacts_bucket scoping rationale — see that module's docstring),
pointed at src/nlp/train.py.
"""

from dataclasses import dataclass

import boto3
from sagemaker.core import image_uris
from sagemaker.core.helper.session_helper import Session
from sagemaker.core.shapes.shapes import OutputDataConfig
from sagemaker.core.training.configs import Compute, InputData, SourceCode
from sagemaker.train.model_trainer import ModelTrainer

_JOB_NAME_PREFIX = "finops-nlp-sentiment-finetune"
_INSTANCE_TYPE = "ml.g4dn.xlarge"
_FRAMEWORK_VERSION = "2.6.0"
_PY_VERSION = "py312"
_REGION = "ap-southeast-1"


@dataclass
class TrainingJobResult:
    """Outcome of a completed SageMaker training job."""

    job_name: str
    model_data_s3_uri: str


def launch_nlp_training_job(
    role_arn: str,
    input_s3_uri: str,
    hyperparameters: dict[str, str],
    model_artifacts_bucket: str,
    sagemaker_session: object | None = None,
) -> TrainingJobResult:
    """Launch the fine-tuning job and block until it finishes.

    Args:
        role_arn: IAM role ARN SageMaker assumes to run the job.
        input_s3_uri: S3 prefix holding `train.csv`, `val.csv` and optionally
            `eval_domain.csv` (the `train` channel).
        hyperparameters: Hyperparameters forwarded to `src/nlp/train.py`.
        model_artifacts_bucket: S3 bucket the execution role is scoped to; used
            as the session default bucket and for the job output location.
        sagemaker_session: Optional injected session for testing.

    Returns:
        TrainingJobResult with the completed job name and model artifact URI.
    """
    if sagemaker_session is None:
        sagemaker_session = Session(
            boto_session=boto3.Session(region_name=_REGION),
            default_bucket=model_artifacts_bucket,
        )

    training_image = image_uris.retrieve(
        framework="pytorch",
        region=_REGION,
        version=_FRAMEWORK_VERSION,
        py_version=_PY_VERSION,
        instance_type=_INSTANCE_TYPE,
        image_scope="training",
    )
    trainer = ModelTrainer(
        training_image=training_image,
        source_code=SourceCode(source_dir="src/nlp", entry_script="train.py"),
        compute=Compute(instance_type=_INSTANCE_TYPE, instance_count=1),
        role=role_arn,
        base_job_name=_JOB_NAME_PREFIX,
        hyperparameters=hyperparameters,
        sagemaker_session=sagemaker_session,
        output_data_config=OutputDataConfig(
            s3_output_path=f"s3://{model_artifacts_bucket}/nlp-training-output"
        ),
    )
    trainer.train(
        input_data_config=[InputData(channel_name="train", data_source=input_s3_uri)],
        wait=True,
    )
    # ModelTrainer.train() only exposes the completed job via this attribute.
    training_job = trainer._latest_training_job
    return TrainingJobResult(
        job_name=training_job.training_job_name,
        model_data_s3_uri=training_job.model_artifacts.s3_model_artifacts,
    )
