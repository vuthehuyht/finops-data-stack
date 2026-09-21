"""Tests for the RAW_NEWS_SENTIMENT DDL template."""

import os

from src.redshift.ddl_executor import _render_query

_TEMPLATE_PATH = os.path.join(
    os.path.dirname(__file__),
    "..",
    "..",
    "src",
    "redshift",
    "ddl",
    "raw",
    "RAW_NEWS_SENTIMENT.sql.jinja",
)


def test_news_sentiment_ddl_renders_with_schema_name() -> None:
    with open(_TEMPLATE_PATH, encoding="utf-8") as f:
        template = f.read()

    rendered = _render_query(template, {"schema_name_raw": "RAW"})

    assert '"RAW"."RAW_NEWS_SENTIMENT"' in rendered
    assert "ARTICLE_ID VARCHAR(256)" in rendered
    assert "SENTIMENT_SCORE NUMERIC(18, 4)" in rendered
    assert "SENTIMENT_LABEL VARCHAR(32)" in rendered
    assert "MODEL_VERSION VARCHAR(256)" in rendered
    assert "_CONATA_LOADED_AT TIMESTAMP" in rendered


def test_news_sentiment_ddl_does_not_drop_existing_scores() -> None:
    # Scores are expensive to recompute (SageMaker), so re-running the DDL
    # job must never wipe the table.
    with open(_TEMPLATE_PATH, encoding="utf-8") as f:
        template = f.read()

    assert "DROP TABLE" not in template.upper()
