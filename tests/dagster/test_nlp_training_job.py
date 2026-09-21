"""Tests for src.dagster.nlp_training_job."""

import io
import json
import tarfile
import unittest.mock

import dagster

from src.dagster.nlp_training_job import (
    NlpEvaluationConfig,
    NlpTrainingJobConfig,
    define_nlp_training_jobs,
    nlp_model_evaluation,
    nlp_training_job,
)
from src.nlp.evaluation import ACTIVE_VERSION_PARAM


def _tarball(metadata: dict) -> bytes:
    buffer = io.BytesIO()
    payload = json.dumps(metadata).encode("utf-8")
    with tarfile.open(fileobj=buffer, mode="w:gz") as tar:
        info = tarfile.TarInfo("metadata.json")
        info.size = len(payload)
        tar.addfile(info, io.BytesIO(payload))
    return buffer.getvalue()


def test_define_nlp_training_jobs_registers_assets_and_manual_job() -> None:
    bundle = define_nlp_training_jobs()

    keys = {key.to_user_string() for asset in bundle.assets for key in asset.keys}
    assert keys == {"NLP/NLP_TRAINING_JOB", "NLP/NLP_MODEL_EVALUATION"}
    assert [job.name for job in bundle.jobs] == ["nlp_finetune_job"]
    # Training needs a user-provided dataset prefix, so it is launched manually.
    assert not hasattr(bundle, "schedules") or not bundle.schedules


def test_training_job_forwards_hyperparameters_and_input_uri() -> None:
    sagemaker = unittest.mock.MagicMock(
        execution_role_arn="arn:role", model_artifacts_bucket="artifacts"
    )
    launched = unittest.mock.MagicMock(
        job_name="job-1", model_data_s3_uri="s3://out/job-1/model.tar.gz"
    )

    with unittest.mock.patch(
        "src.dagster.nlp_training_job.launch_nlp_training_job", return_value=launched
    ) as launch:
        result = nlp_training_job(
            dagster.build_asset_context(),
            NlpTrainingJobConfig(
                input_s3_uri="s3://bucket/nlp-training-data/", epochs=2
            ),
            sagemaker,
        )

    kwargs = launch.call_args.kwargs
    assert kwargs["input_s3_uri"] == "s3://bucket/nlp-training-data/"
    assert kwargs["hyperparameters"] == {
        "epochs": "2",
        "batch-size": "16",
        "learning-rate": "2e-05",
    }
    assert kwargs["model_artifacts_bucket"] == "artifacts"
    assert result.value == {
        "job_name": "job-1",
        "model_data_s3_uri": "s3://out/job-1/model.tar.gz",
    }


def _evaluate(challenger_meta, champion_meta, active_version, baseline=None):
    s3 = unittest.mock.MagicMock()
    client = s3.get_client.return_value
    client.get_object.side_effect = lambda Bucket, Key: {
        "Body": unittest.mock.MagicMock(
            read=lambda: (
                _tarball(challenger_meta)
                if Key.endswith("model.tar.gz") and "out/" in Key
                else json.dumps(champion_meta).encode("utf-8")
            )
        )
    }
    sagemaker = unittest.mock.MagicMock(model_artifacts_bucket="artifacts")
    ssm = unittest.mock.MagicMock()
    ssm.get_parameter.return_value = active_version

    result = nlp_model_evaluation(
        dagster.build_asset_context(),
        {"job_name": "job-2", "model_data_s3_uri": "s3://out/out/model.tar.gz"},
        NlpEvaluationConfig(baseline_score=baseline),
        s3,
        sagemaker,
        ssm,
    )
    return result, ssm, client


def test_evaluation_promotes_first_model_and_versions_artifacts() -> None:
    result, ssm, client = _evaluate({"val_macro_f1": 0.8}, None, None)

    assert result.value is True
    ssm.put_parameter.assert_called_once_with(ACTIVE_VERSION_PARAM, "job-2")
    copied = client.copy_object.call_args.kwargs
    assert copied["Key"] == "nlp-sentiment/versions/job-2/model.tar.gz"
    assert client.put_object.call_args.kwargs["Key"] == (
        "nlp-sentiment/versions/job-2/metadata.json"
    )


def test_evaluation_requires_beating_the_baseline_when_no_champion() -> None:
    # No promoted champion yet, but the pretrained model scored 0.85 on the
    # same evaluation set: a worse fine-tune must not replace it.
    result, ssm, _ = _evaluate({"val_macro_f1": 0.8}, None, None, baseline=0.85)

    assert result.value is False
    ssm.put_parameter.assert_not_called()


def test_evaluation_promotes_strictly_better_challenger() -> None:
    result, ssm, _ = _evaluate({"val_macro_f1": 0.9}, {"val_macro_f1": 0.8}, "job-1")

    assert result.value is True
    ssm.put_parameter.assert_called_once_with(ACTIVE_VERSION_PARAM, "job-2")


def test_evaluation_rejects_worse_challenger() -> None:
    result, ssm, _ = _evaluate({"val_macro_f1": 0.7}, {"val_macro_f1": 0.8}, "job-1")

    assert result.value is False
    ssm.put_parameter.assert_not_called()


def test_evaluation_refuses_to_compare_scores_from_different_sets() -> None:
    # Challenger was scored on in-domain data, champion on the public split.
    result, ssm, _ = _evaluate(
        {"val_macro_f1": 0.9, "domain_macro_f1": 0.6},
        {"val_macro_f1": 0.5},
        "job-1",
    )

    assert result.value is False
    ssm.put_parameter.assert_not_called()
