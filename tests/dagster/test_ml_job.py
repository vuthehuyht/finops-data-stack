"""Tests for ml_job.py."""

import datetime

import dagster


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
