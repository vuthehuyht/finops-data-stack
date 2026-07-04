"""Tests for the FCT_ML_FORECAST_RESULTS DDL template."""

import os

from src.redshift.ddl_executor import _render_query

_TEMPLATE_PATH = os.path.join(
    os.path.dirname(__file__),
    "..",
    "..",
    "src",
    "redshift",
    "ddl",
    "mart",
    "FCT_ML_FORECAST_RESULTS.sql.jinja",
)


def test_forecast_results_ddl_renders_with_schema_name() -> None:
    with open(_TEMPLATE_PATH, encoding="utf-8") as f:
        template = f.read()

    rendered = _render_query(template, {"schema_name_mart": "MART"})

    assert '"MART"."FCT_ML_FORECAST_RESULTS"' in rendered
    assert "TICKER VARCHAR(256)" in rendered
    assert "TRADING_DATE DATE" in rendered
    assert "PREDICTED_RETURN NUMERIC(18, 6)" in rendered
    assert "MODEL_VERSION VARCHAR(256)" in rendered
