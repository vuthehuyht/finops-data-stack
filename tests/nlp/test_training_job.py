"""Tests for src/nlp/training_job.py (mirrors tests/ml/test_training_job.py)."""

from unittest.mock import MagicMock, patch

from src.nlp.training_job import launch_nlp_training_job


def _mock_trainer(job_name: str, model_uri: str) -> MagicMock:
    training_job = MagicMock()
    training_job.training_job_name = job_name
    training_job.model_artifacts.s3_model_artifacts = model_uri
    trainer = MagicMock()
    trainer._latest_training_job = training_job
    return trainer


def test_launch_nlp_training_job_constructs_model_trainer_with_expected_args() -> None:
    mock_trainer = _mock_trainer(
        "finops-nlp-sentiment-finetune-20260921",
        "s3://sagemaker-bucket/job/output/model.tar.gz",
    )

    with (
        patch(
            "src.nlp.training_job.image_uris.retrieve", return_value="training-image"
        ) as mock_retrieve,
        patch(
            "src.nlp.training_job.ModelTrainer", return_value=mock_trainer
        ) as mock_cls,
    ):
        result = launch_nlp_training_job(
            role_arn="arn:aws:iam::123456789012:role/sagemaker-execution",
            input_s3_uri="s3://bucket/nlp-training-data/",
            hyperparameters={"epochs": "3"},
            model_artifacts_bucket="finops-model-artifacts-dev",
        )

    mock_retrieve.assert_called_once_with(
        framework="pytorch",
        region="ap-southeast-1",
        version="2.6.0",
        py_version="py312",
        instance_type="ml.g4dn.xlarge",
        image_scope="training",
    )
    _, kwargs = mock_cls.call_args
    assert kwargs["training_image"] == "training-image"
    assert kwargs["source_code"].source_dir == "src/nlp"
    assert kwargs["source_code"].entry_script == "train.py"
    assert kwargs["compute"].instance_type == "ml.g4dn.xlarge"
    assert kwargs["role"] == "arn:aws:iam::123456789012:role/sagemaker-execution"
    assert kwargs["hyperparameters"] == {"epochs": "3"}
    assert (
        kwargs["output_data_config"].s3_output_path
        == "s3://finops-model-artifacts-dev/nlp-training-output"
    )

    _, train_kwargs = mock_trainer.train.call_args
    (channel,) = train_kwargs["input_data_config"]
    # train.py reads train.csv, val.csv and eval_domain.csv from this one channel.
    assert channel.channel_name == "train"
    assert channel.data_source == "s3://bucket/nlp-training-data/"
    assert train_kwargs["wait"] is True

    assert result.job_name == "finops-nlp-sentiment-finetune-20260921"
    assert result.model_data_s3_uri == "s3://sagemaker-bucket/job/output/model.tar.gz"


def test_launch_nlp_training_job_forwards_sagemaker_session() -> None:
    session = MagicMock()
    mock_trainer = _mock_trainer("job", "s3://bucket/job/output/model.tar.gz")

    with (
        patch("src.nlp.training_job.image_uris.retrieve", return_value="image"),
        patch(
            "src.nlp.training_job.ModelTrainer", return_value=mock_trainer
        ) as mock_cls,
    ):
        launch_nlp_training_job(
            role_arn="arn:aws:iam::123456789012:role/sagemaker-execution",
            input_s3_uri="s3://bucket/prefix/",
            hyperparameters={},
            model_artifacts_bucket="finops-model-artifacts-dev",
            sagemaker_session=session,
        )

    _, kwargs = mock_cls.call_args
    assert kwargs["sagemaker_session"] is session
