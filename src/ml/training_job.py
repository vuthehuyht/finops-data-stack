"""SageMaker Training Job launcher for the quarterly re-training pipeline.

Uses the built-in SageMaker PyTorch framework container in script mode
(`source_code.entry_script=train.py`) via the SageMaker SDK v3 ModelTrainer
API — no custom Docker image.
"""

from dataclasses import dataclass

from sagemaker.core import image_uris
from sagemaker.core.helper.session_helper import Session
from sagemaker.core.shapes.shapes import OutputDataConfig
from sagemaker.core.training.configs import Compute, InputData, SourceCode
from sagemaker.train.model_trainer import ModelTrainer

from src.ml.config import MODEL_NAME

_INSTANCE_TYPE = "ml.g4dn.xlarge"
_FRAMEWORK_VERSION = "2.6.0"
_PY_VERSION = "py312"
_REGION = "ap-southeast-1"


@dataclass
class TrainingJobResult:
    """Outcome of a completed SageMaker training job."""

    job_name: str
    model_data_s3_uri: str


def launch_training_job(
    role_arn: str,
    input_s3_uri: str,
    hyperparameters: dict[str, str],
    model_artifacts_bucket: str,
    sagemaker_session: object | None = None,
) -> TrainingJobResult:
    """Launch a SageMaker PyTorch training job and block until it finishes.

    Args:
        role_arn: IAM role ARN SageMaker assumes to run the job.
        input_s3_uri: S3 URI of the training data (Parquet, `train` channel).
        hyperparameters: Hyperparameters forwarded to `src/ml/train.py`.
        model_artifacts_bucket: S3 bucket the execution role is scoped to
            (Terraform `modules/sagemaker` grants it S3 access only here).
            Used both as the default session bucket (so ModelTrainer's
            packaged source_code lands somewhere the role can read) and as
            the training job's output location — without this, ModelTrainer
            uploads source_code and writes output to SageMaker's
            auto-created default session bucket, which the role has no
            permissions on and CreateTrainingJob rejects with a ListBucket
            AccessDenied.
        sagemaker_session: Optional `sagemaker.Session` (injected for
            testing); when omitted, one is constructed from
            model_artifacts_bucket.

    Returns:
        TrainingJobResult with the completed job name and model artifact URI.
    """
    if sagemaker_session is None:
        sagemaker_session = Session(default_bucket=model_artifacts_bucket)

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
        source_code=SourceCode(source_dir="src/ml", entry_script="train.py"),
        compute=Compute(instance_type=_INSTANCE_TYPE, instance_count=1),
        role=role_arn,
        base_job_name=MODEL_NAME,
        hyperparameters=hyperparameters,
        sagemaker_session=sagemaker_session,
        output_data_config=OutputDataConfig(
            s3_output_path=f"s3://{model_artifacts_bucket}/ml-training-output"
        ),
    )
    trainer.train(
        input_data_config=[InputData(channel_name="train", data_source=input_s3_uri)],
        wait=True,
    )
    # ModelTrainer.train() only exposes the completed job via this
    # attribute (documented in the SDK's own docstring example).
    training_job = trainer._latest_training_job
    return TrainingJobResult(
        job_name=training_job.training_job_name,
        model_data_s3_uri=training_job.model_artifacts.s3_model_artifacts,
    )
