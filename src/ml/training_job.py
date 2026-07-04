"""SageMaker Training Job launcher for the quarterly re-training pipeline.

Uses the built-in SageMaker PyTorch framework container in script mode
(`source_code.entry_script=train.py`) via the SageMaker SDK v3 ModelTrainer
API — no custom Docker image.
"""

from dataclasses import dataclass

from sagemaker.core import image_uris
from sagemaker.core.training.configs import Compute, InputData, SourceCode
from sagemaker.train.model_trainer import ModelTrainer

from src.ml.config import MODEL_NAME

_INSTANCE_TYPE = "ml.g4dn.xlarge"
_FRAMEWORK_VERSION = "2.2"
_PY_VERSION = "py310"
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
    sagemaker_session: object | None = None,
) -> TrainingJobResult:
    """Launch a SageMaker PyTorch training job and block until it finishes.

    Args:
        role_arn: IAM role ARN SageMaker assumes to run the job.
        input_s3_uri: S3 URI of the training data (Parquet, `train` channel).
        hyperparameters: Hyperparameters forwarded to `src/ml/train.py`.
        sagemaker_session: Optional `sagemaker.Session` (injected for testing).

    Returns:
        TrainingJobResult with the completed job name and model artifact URI.
    """
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
