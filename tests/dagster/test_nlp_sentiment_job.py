"""Tests for src.dagster.nlp_sentiment_job asset/job/sensor wiring and logic."""

import re
import unittest.mock
from pathlib import Path

import dagster
import pandas as pd
import pytest

from src.dagster.nlp_sentiment_job import (
    NlpSentimentPrepConfig,
    _staging_schema,
    build_run_request,
    build_sentiment_payloads,
    build_unscored_articles_query,
    define_nlp_sentiment_jobs,
    materialization_partition,
    nlp_publish_sentiment_scores,
    nlp_sentiment_batch_transform,
    nlp_sentiment_prep,
)


def test_define_nlp_sentiment_jobs_registers_expected_assets() -> None:
    bundle = define_nlp_sentiment_jobs()
    asset_keys = {key.to_user_string() for asset in bundle.assets for key in asset.keys}

    assert "NLP/NLP_SENTIMENT_PREP" in asset_keys
    assert "NLP/NLP_SENTIMENT_BATCH_TRANSFORM" in asset_keys
    # Same key the silver sensor derives from stg_news_sentiment.
    assert "RAW/NEWS_SENTIMENT" in asset_keys


def test_define_nlp_sentiment_jobs_registers_one_sensor() -> None:
    bundle = define_nlp_sentiment_jobs()
    assert len(bundle.sensors) == 1
    assert bundle.sensors[0].name == "nlp_sentiment_sensor"


def test_build_sentiment_payloads_joins_text_parts_and_skips_empty() -> None:
    df = pd.DataFrame(
        {
            "article_id": ["A1", "A2", "A3"],
            "title": ["Tieu de", None, None],
            "summary": ["Tom tat", float("nan"), None],
            "content": ["Noi dung", None, "   "],
        }
    )

    payloads = build_sentiment_payloads(df)

    assert payloads == [{"article_id": "A1", "text": "Tieu de Tom tat Noi dung"}]


def _prep_with_articles(df: pd.DataFrame, tmp_path: Path):
    redshift = unittest.mock.MagicMock()
    s3bucket = unittest.mock.MagicMock(processed_bucket="processed")
    s3 = unittest.mock.MagicMock()
    uploaded: dict[str, str] = {}
    s3.get_client.return_value.upload_file.side_effect = lambda path, bucket, key: (
        uploaded.update({key: Path(path).read_text(encoding="utf-8")})
    )
    context = dagster.build_asset_context()
    with unittest.mock.patch("pandas.read_sql", return_value=df):
        result = nlp_sentiment_prep(
            context,
            NlpSentimentPrepConfig(batch_date="2026-09-14"),
            redshift,
            unittest.mock.MagicMock(),
            s3bucket,
            s3,
        )
    return result, uploaded


def test_prep_stages_unscored_articles_as_jsonl(tmp_path: Path) -> None:
    df = pd.DataFrame(
        {
            "article_id": ["A1"],
            "title": ["Tieu de"],
            "summary": [None],
            "content": ["Noi dung"],
        }
    )

    result, uploaded = _prep_with_articles(df, tmp_path)

    assert result.value["article_count"] == 1
    assert result.value["batch_date"] == "2026-09-14"
    (key,) = uploaded
    assert key.startswith("nlp-sentiment-input/2026-09-14/")
    assert '"article_id": "A1"' in uploaded[key]


def test_prep_returns_zero_when_nothing_to_score(tmp_path: Path) -> None:
    empty = pd.DataFrame(columns=["article_id", "title", "summary", "content"])

    result, uploaded = _prep_with_articles(empty, tmp_path)

    # batch_date is kept so the publish step can still emit a partition marker.
    assert result.value == {"article_count": 0, "batch_date": "2026-09-14"}
    assert uploaded == {}


def test_publish_skips_load_when_nothing_scored() -> None:
    redshift = unittest.mock.MagicMock()
    load_config = unittest.mock.MagicMock(iam_role_arn="arn:role")
    context = dagster.build_asset_context()

    with unittest.mock.patch(
        "src.dagster.nlp_sentiment_job.load_s3_to_redshift"
    ) as load:
        result = nlp_publish_sentiment_scores(
            context,
            {"article_count": 0, "batch_date": "2026-09-14"},
            redshift,
            load_config,
        )

    load.assert_not_called()
    assert result.metadata["row_count"].value == 0
    # An empty day must still report its partition, or mart sensors that wait
    # on every upstream never fire for it.
    assert result.metadata["conata_partition_key"].text == "2026-09-14"


def test_publish_loads_scores_and_emits_partition_key() -> None:
    redshift = unittest.mock.MagicMock()
    load_config = unittest.mock.MagicMock(iam_role_arn="arn:role")
    context = dagster.build_asset_context()
    transform_result = {
        "article_count": 2,
        "batch_date": "2026-09-14",
        "output_s3_uri": "s3://processed/nlp-sentiment-output/2026-09-14/r/input.jsonl.out",
    }

    with unittest.mock.patch(
        "src.dagster.nlp_sentiment_job.load_s3_to_redshift", return_value=2
    ) as load:
        result = nlp_publish_sentiment_scores(
            context, transform_result, redshift, load_config
        )

    kwargs = load.call_args.kwargs
    assert kwargs["table_name"] == "RAW_NEWS_SENTIMENT"
    assert kwargs["schema"] == "raw"
    assert kwargs["file_format"] == "json"
    assert kwargs["s3_url"] == transform_result["output_s3_uri"]
    assert kwargs["batch_date"] == "2026-09-14"
    # The silver sensor reads this metadata key to pick the dbt partition.
    assert result.metadata["conata_partition_key"].text == "2026-09-14"
    assert result.metadata["batch_date"].text == "2026-09-14"


def test_publish_rejects_malformed_batch_date() -> None:
    redshift = unittest.mock.MagicMock()
    load_config = unittest.mock.MagicMock(iam_role_arn="arn:role")
    context = dagster.build_asset_context()

    with pytest.raises(ValueError):
        nlp_publish_sentiment_scores(
            context,
            {
                "article_count": 1,
                "batch_date": "2026-09-14'; DROP TABLE x;--",
                "output_s3_uri": "s3://processed/x",
            },
            redshift,
            load_config,
        )


def _run_batch_transform(active_version):
    sagemaker = unittest.mock.MagicMock(model_artifacts_bucket="artifacts")
    s3bucket = unittest.mock.MagicMock(processed_bucket="processed")
    ssm = unittest.mock.MagicMock()
    ssm.get_parameter.return_value = active_version
    prep_result = {
        "article_count": 2,
        "input_s3_uri": "s3://processed/nlp-sentiment-input/2026-09-14/r/input.jsonl",
        "batch_date": "2026-09-14",
        "run_id": "abcdef0123456789",
    }
    result = nlp_sentiment_batch_transform(
        dagster.build_asset_context(),
        prep_result,
        sagemaker,
        ssm,
        s3bucket,
        unittest.mock.MagicMock(),
    )
    return result, sagemaker


def test_batch_transform_uses_pretrained_model_without_active_version() -> None:
    result, sagemaker = _run_batch_transform(None)

    kwargs = sagemaker.create_model_if_not_exists.call_args.kwargs
    assert kwargs["model_data_s3_uri"] == (
        "s3://artifacts/nlp-sentiment/wonrax_phobert-base-vietnamese-sentiment/"
        "model.tar.gz"
    )
    assert result.value["output_s3_uri"].endswith("/input.jsonl.out")


def test_batch_transform_uses_promoted_version_when_set() -> None:
    _, sagemaker = _run_batch_transform("finops-nlp-sentiment-finetune-2026")

    kwargs = sagemaker.create_model_if_not_exists.call_args.kwargs
    assert kwargs["model_name"] == "nlp-finops-nlp-sentiment-finetune-2026"
    assert kwargs["model_data_s3_uri"] == (
        "s3://artifacts/nlp-sentiment/versions/finops-nlp-sentiment-finetune-2026/"
        "model.tar.gz"
    )


def test_batch_transform_ignores_none_sentinel_version() -> None:
    _, sagemaker = _run_batch_transform("none")

    kwargs = sagemaker.create_model_if_not_exists.call_args.kwargs
    assert "/versions/" not in kwargs["model_data_s3_uri"]


def test_batch_transform_model_names_are_valid_for_sagemaker() -> None:
    realistic_job = "finops-nlp-sentiment-finetune-2026-09-21-12-30-45-123"
    for version in (None, "finops-nlp-sentiment-finetune-2026", realistic_job):
        _, sagemaker = _run_batch_transform(version)
        name = sagemaker.create_model_if_not_exists.call_args.kwargs["model_name"]
        # SageMaker model names allow only alphanumerics and hyphens, max 63.
        assert re.fullmatch(r"[A-Za-z0-9]([A-Za-z0-9-]*[A-Za-z0-9])?", name), name
        assert len(name) <= 63


def test_unscored_query_is_capped_newest_first_and_uses_given_schema() -> None:
    query = build_unscored_articles_query("staging", 250, None)

    assert "FROM staging.STG_NEWS_ARTICLES" in query
    assert "LEFT JOIN staging.STG_NEWS_SENTIMENT" in query
    assert "ORDER BY A.PUBLISH_TIME DESC" in query
    assert "LIMIT 250" in query
    assert "MODEL_VERSION" not in query


def test_unscored_query_can_select_articles_scored_by_another_model() -> None:
    query = build_unscored_articles_query("staging", 10, "job-2")

    assert "S.MODEL_VERSION <> 'job-2'" in query


@pytest.mark.parametrize(
    ("schema", "limit", "label"),
    [
        ("staging; DROP TABLE x", 10, None),
        ("staging", 0, None),
        ("staging", 10, "x'; DROP TABLE y;--"),
    ],
)
def test_unscored_query_rejects_unsafe_arguments(schema, limit, label) -> None:
    with pytest.raises(ValueError):
        build_unscored_articles_query(schema, limit, label)


def test_prep_reads_staging_schema_from_environment(monkeypatch) -> None:
    monkeypatch.delenv("REDSHIFT_STAGING_SCHEMA", raising=False)
    assert _staging_schema() == "staging"

    monkeypatch.setenv("REDSHIFT_STAGING_SCHEMA", "CI_STG_42")
    assert _staging_schema() == "CI_STG_42"

    monkeypatch.setenv("REDSHIFT_STAGING_SCHEMA", "bad name;")
    with pytest.raises(ValueError, match="Invalid staging schema"):
        _staging_schema()


def test_prep_queries_the_configured_schema(monkeypatch) -> None:
    monkeypatch.setenv("REDSHIFT_STAGING_SCHEMA", "CI_STG_7")
    df = pd.DataFrame(columns=["article_id", "title", "summary", "content"])
    with unittest.mock.patch("pandas.read_sql", return_value=df) as read_sql:
        nlp_sentiment_prep(
            dagster.build_asset_context(),
            NlpSentimentPrepConfig(batch_date="2026-09-14", max_articles=100),
            unittest.mock.MagicMock(),
            unittest.mock.MagicMock(),
            unittest.mock.MagicMock(processed_bucket="processed"),
            unittest.mock.MagicMock(),
        )

    query = read_sql.call_args.args[0]
    assert "CI_STG_7.STG_NEWS_ARTICLES" in query
    assert "LIMIT 100" in query


def test_prep_rescoring_targets_the_serving_model_version() -> None:
    ssm = unittest.mock.MagicMock()
    ssm.get_parameter.return_value = "job-9"
    df = pd.DataFrame(columns=["article_id", "title", "summary", "content"])
    with unittest.mock.patch("pandas.read_sql", return_value=df) as read_sql:
        nlp_sentiment_prep(
            dagster.build_asset_context(),
            NlpSentimentPrepConfig(batch_date="2026-09-14", rescore_stale=True),
            unittest.mock.MagicMock(),
            ssm,
            unittest.mock.MagicMock(processed_bucket="processed"),
            unittest.mock.MagicMock(),
        )

    assert "S.MODEL_VERSION <> 'job-9'" in read_sql.call_args.args[0]


def test_batch_transform_passes_batch_date_through_when_nothing_to_score() -> None:
    result = nlp_sentiment_batch_transform(
        dagster.build_asset_context(),
        {"article_count": 0, "batch_date": "2026-09-14"},
        unittest.mock.MagicMock(),
        unittest.mock.MagicMock(),
        unittest.mock.MagicMock(),
        unittest.mock.MagicMock(),
    )

    assert result.value == {"article_count": 0, "batch_date": "2026-09-14"}


def _materialization(partition=None, **metadata):
    return unittest.mock.MagicMock(partition=partition, metadata=metadata)


def test_materialization_partition_reads_native_partition_first() -> None:
    mat = _materialization(
        partition="2026-09-10",
        conata_partition_key=dagster.MetadataValue.text("2026-09-01"),
    )
    assert materialization_partition(mat) == "2026-09-10"


def test_materialization_partition_falls_back_to_metadata() -> None:
    conata = _materialization(
        conata_partition_key=dagster.MetadataValue.text("2026-09-11")
    )
    dbt_vars = _materialization(
        variables=dagster.MetadataValue.json({"partition_key": "2026-09-12"})
    )
    assert materialization_partition(conata) == "2026-09-11"
    assert materialization_partition(dbt_vars) == "2026-09-12"
    assert materialization_partition(_materialization()) is None


def test_run_request_scores_under_the_triggering_partition() -> None:
    request = build_run_request(42, "2026-09-11")

    assert request.run_key == "nlp_sentiment_42"
    op_config = request.run_config["ops"][nlp_sentiment_prep.op.name]["config"]
    assert op_config["batch_date"] == "2026-09-11"


def test_run_request_without_partition_keeps_default_config() -> None:
    assert build_run_request(7, None).run_config == {}


def test_run_request_rejects_malformed_partition() -> None:
    with pytest.raises(ValueError):
        build_run_request(1, "not-a-date")
