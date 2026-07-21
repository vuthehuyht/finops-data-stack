"""Tests for src/ml/training_job.py."""

from unittest.mock import MagicMock, patch


def test_launch_training_job_constructs_model_trainer_with_expected_args() -> None:
    from src.ml.training_job import launch_training_job

    mock_training_job = MagicMock()
    mock_training_job.training_job_name = "finops-multimodal-regressor-20260703"
    mock_training_job.model_artifacts.s3_model_artifacts = (
        "s3://sagemaker-bucket/finops-multimodal-regressor-20260703/output/model.tar.gz"
    )
    mock_trainer = MagicMock()
    mock_trainer._latest_training_job = mock_training_job

    with (
        patch(
            "src.ml.training_job.image_uris.retrieve",
            return_value="763104351884.dkr.ecr.ap-southeast-1.amazonaws.com/pytorch-training:2.6.0-gpu-py312",
        ) as mock_retrieve,
        patch(
            "src.ml.training_job.ModelTrainer", return_value=mock_trainer
        ) as mock_cls,
    ):
        result = launch_training_job(
            role_arn="arn:aws:iam::123456789012:role/sagemaker-execution",
            input_s3_uri="s3://bucket/ml-training-data/2026-07-03/",
            hyperparameters={"epochs": "20"},
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
    assert kwargs["training_image"] == (
        "763104351884.dkr.ecr.ap-southeast-1.amazonaws.com/pytorch-training:2.6.0-gpu-py312"
    )
    assert kwargs["source_code"].source_dir == "src/ml"
    assert kwargs["source_code"].entry_script == "train.py"
    assert kwargs["compute"].instance_type == "ml.g4dn.xlarge"
    assert kwargs["compute"].instance_count == 1
    assert kwargs["role"] == "arn:aws:iam::123456789012:role/sagemaker-execution"
    assert kwargs["hyperparameters"] == {"epochs": "20"}
    assert kwargs["sagemaker_session"] is not None
    assert (
        kwargs["output_data_config"].s3_output_path
        == "s3://finops-model-artifacts-dev/ml-training-output"
    )

    mock_trainer.train.assert_called_once()
    _, train_kwargs = mock_trainer.train.call_args
    input_data_config = train_kwargs["input_data_config"]
    assert len(input_data_config) == 1
    assert input_data_config[0].channel_name == "train"
    assert (
        input_data_config[0].data_source == "s3://bucket/ml-training-data/2026-07-03/"
    )
    assert train_kwargs["wait"] is True

    assert result.job_name == "finops-multimodal-regressor-20260703"
    assert result.model_data_s3_uri == (
        "s3://sagemaker-bucket/finops-multimodal-regressor-20260703/output/model.tar.gz"
    )


def test_launch_training_job_forwards_sagemaker_session() -> None:
    from src.ml.training_job import launch_training_job

    mock_session = MagicMock()
    mock_training_job = MagicMock()
    mock_training_job.training_job_name = "job-name"
    mock_training_job.model_artifacts.s3_model_artifacts = (
        "s3://bucket/job-name/output/model.tar.gz"
    )
    mock_trainer = MagicMock()
    mock_trainer._latest_training_job = mock_training_job

    with (
        patch("src.ml.training_job.image_uris.retrieve", return_value="dummy-image"),
        patch(
            "src.ml.training_job.ModelTrainer", return_value=mock_trainer
        ) as mock_cls,
    ):
        launch_training_job(
            role_arn="arn:aws:iam::123456789012:role/sagemaker-execution",
            input_s3_uri="s3://bucket/prefix/",
            hyperparameters={},
            model_artifacts_bucket="finops-model-artifacts-dev",
            sagemaker_session=mock_session,
        )

    _, kwargs = mock_cls.call_args
    assert kwargs["sagemaker_session"] is mock_session
