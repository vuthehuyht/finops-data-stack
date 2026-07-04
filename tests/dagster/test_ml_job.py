"""Tests for ml_job.py."""

import datetime
import io
import json
import tarfile
import unittest.mock

import dagster


def _make_tarball(files: dict[str, bytes]) -> bytes:
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode="w:gz") as tar:
        for name, content in files.items():
            info = tarfile.TarInfo(name=name)
            info.size = len(content)
            tar.addfile(info, io.BytesIO(content))
    return buffer.getvalue()


def test_ml_training_dataset_config_run_date_defaults_to_today() -> None:
    from src.dagster.ml_job import MlTrainingDatasetConfig

    config = MlTrainingDatasetConfig()
    assert config.run_date == datetime.date.today().isoformat()


def test_ml_training_job_config_defaults() -> None:
    from src.dagster.ml_job import MlTrainingJobConfig

    config = MlTrainingJobConfig()
    assert config.epochs == 20
    assert config.batch_size == 64
    assert config.learning_rate == 1e-3
    # train_end_date must be chronologically before val_end_date.
    assert config.train_end_date < config.val_end_date


def test_ml_evaluation_config_default_threshold() -> None:
    from src.dagster.ml_job import MlEvaluationConfig

    config = MlEvaluationConfig()
    assert config.evaluation_threshold == 0.0


def test_define_ml_jobs_returns_bundle_with_three_assets() -> None:
    from src.dagster.ml_job import MlJobBundle, define_ml_jobs

    bundle = define_ml_jobs()
    assert isinstance(bundle, MlJobBundle)
    assert len(bundle.assets) == 3
    assert len(bundle.jobs) == 1


def test_define_ml_jobs_asset_keys() -> None:
    from src.dagster.ml_job import define_ml_jobs

    bundle = define_ml_jobs()
    keys = {a.key for a in bundle.assets}
    assert dagster.AssetKey(["ML", "GOLD_ML_TRAINING_DATASET"]) in keys
    assert dagster.AssetKey(["ML", "ML_TRAINING_JOB"]) in keys
    assert dagster.AssetKey(["ML", "ML_MODEL_EVALUATION"]) in keys


def test_ml_training_job_depends_on_training_dataset() -> None:
    from src.dagster.ml_job import define_ml_jobs

    bundle = define_ml_jobs()
    training_job_asset = next(
        a for a in bundle.assets if a.key == dagster.AssetKey(["ML", "ML_TRAINING_JOB"])
    )
    assert dagster.AssetKey(["ML", "GOLD_ML_TRAINING_DATASET"]) in (
        training_job_asset.dependency_keys
    )


def test_ml_model_evaluation_depends_on_training_job() -> None:
    from src.dagster.ml_job import define_ml_jobs

    bundle = define_ml_jobs()
    evaluation_asset = next(
        a
        for a in bundle.assets
        if a.key == dagster.AssetKey(["ML", "ML_MODEL_EVALUATION"])
    )
    assert dagster.AssetKey(["ML", "ML_TRAINING_JOB"]) in (
        evaluation_asset.dependency_keys
    )


def test_define_ml_jobs_job_name() -> None:
    from src.dagster.ml_job import define_ml_jobs

    bundle = define_ml_jobs()
    assert bundle.jobs[0].name == "ml_quarterly_retrain_job"


def test_ml_model_evaluation_updates_endpoint_on_promotion() -> None:
    from src.dagster.ml_job import MlEvaluationConfig, ml_model_evaluation

    mock_s3_client = unittest.mock.MagicMock()
    challenger_tarball = _make_tarball(
        {"metadata.json": json.dumps({"metrics": {"rmse": 0.05}}).encode("utf-8")}
    )
    mock_s3_client.get_object.return_value = {
        "Body": unittest.mock.MagicMock(read=lambda: challenger_tarball)
    }
    mock_s3 = unittest.mock.MagicMock()
    mock_s3.get_client.return_value = mock_s3_client

    mock_sagemaker = unittest.mock.MagicMock()
    mock_sagemaker.model_artifacts_bucket = "finops-model-artifacts-dev"

    mock_ssm = unittest.mock.MagicMock()
    mock_ssm.get_parameter.side_effect = lambda name: {
        "/finops/model/active_version": None,
        "/finops/model/evaluation_threshold": None,
        "/finops/model/endpoint_name": "finops-endpoint",
    }.get(name)

    training_job_result = {
        "job_name": "finops-multimodal-regressor-20260703",
        "model_data_s3_uri": (
            "s3://bucket/finops-multimodal-regressor-20260703/output/model.tar.gz"
        ),
    }
    context = dagster.build_asset_context()

    result = ml_model_evaluation(
        context,
        training_job_result,
        MlEvaluationConfig(),
        mock_s3,
        mock_sagemaker,
        mock_ssm,
    )

    assert result.value is True  # no champion yet -> bootstrap promotion
    mock_sagemaker.deploy_model_version.assert_called_once()
    _, kwargs = mock_sagemaker.deploy_model_version.call_args
    assert kwargs["endpoint_name"] == "finops-endpoint"
    assert kwargs["model_name"] == "finops-multimodal-regressor-20260703"
    assert kwargs["model_data_s3_uri"] == (
        "s3://finops-model-artifacts-dev/"
        "finops-multimodal-regressor/finops-multimodal-regressor-20260703/model.tar.gz"
    )


def test_ml_model_evaluation_skips_endpoint_update_when_not_promoted() -> None:
    from src.dagster.ml_job import MlEvaluationConfig, ml_model_evaluation

    mock_s3_client = unittest.mock.MagicMock()
    challenger_tarball = _make_tarball(
        {"metadata.json": json.dumps({"metrics": {"rmse": 0.20}}).encode("utf-8")}
    )

    def get_object_side_effect(Bucket, Key):
        if Key.endswith("output/model.tar.gz"):
            body = challenger_tarball
        else:
            # Champion metadata read reads plain JSON directly (not a tarball).
            body = b'{"metrics": {"rmse": 0.05}}'
        return {"Body": unittest.mock.MagicMock(read=lambda: body)}

    mock_s3_client.get_object.side_effect = get_object_side_effect
    mock_s3 = unittest.mock.MagicMock()
    mock_s3.get_client.return_value = mock_s3_client

    mock_sagemaker = unittest.mock.MagicMock()
    mock_sagemaker.model_artifacts_bucket = "finops-model-artifacts-dev"

    mock_ssm = unittest.mock.MagicMock()
    mock_ssm.get_parameter.side_effect = lambda name: {
        "/finops/model/active_version": "finops-multimodal-regressor-20260601",
        "/finops/model/evaluation_threshold": "0.5",
        "/finops/model/endpoint_name": "finops-endpoint",
    }.get(name)

    training_job_result = {
        "job_name": "finops-multimodal-regressor-20260703",
        "model_data_s3_uri": (
            "s3://bucket/finops-multimodal-regressor-20260703/output/model.tar.gz"
        ),
    }
    context = dagster.build_asset_context()

    result = ml_model_evaluation(
        context,
        training_job_result,
        MlEvaluationConfig(),
        mock_s3,
        mock_sagemaker,
        mock_ssm,
    )

    assert result.value is False
    mock_sagemaker.deploy_model_version.assert_not_called()
