import json
import os
import unittest.mock

import pytest

import src.pipeline.dagster as dagster_lib
from src.dagster import dbt_assets


@pytest.fixture
def dummy_manifest_path(tmp_path) -> str:
    """Create a dummy manifest.json file to satisfy dbt selector."""
    manifest_data = {
        "metadata": {
            "dbt_schema_version": "https://schemas.getdbt.com/dbt/manifest/v10.json",
            "project_id": "test_project"
        },
        "nodes": {
            "model.test_project.my_model": {
                "resource_type": "model",
                "name": "my_model",
                "package_name": "test_project",
                "fqn": ["test_project", "my_model"],
                "unique_id": "model.test_project.my_model",
                "config": {
                    "enabled": True,
                    "materialized": "table"
                },
                "database": "dev",
                "schema": "silver",
                "path": "models/my_model.sql",
                "original_file_path": "models/my_model.sql",
                "depends_on": {"nodes": ["model.test_project.upstream_model"]}
            },
            "model.test_project.upstream_model": {
                "resource_type": "model",
                "name": "upstream_model",
                "package_name": "test_project",
                "fqn": ["test_project", "upstream_model"],
                "unique_id": "model.test_project.upstream_model",
                "config": {
                    "enabled": True,
                    "materialized": "table"
                },
                "database": "dev",
                "schema": "silver",
                "path": "models/upstream_model.sql",
                "original_file_path": "models/upstream_model.sql",
                "depends_on": {"nodes": []}
            }
        },
        "sources": {},
        "metrics": {},
        "exposures": {},
        "parent_map": {
            "model.test_project.my_model": ["model.test_project.upstream_model"],
            "model.test_project.upstream_model": []
        },
        "child_map": {
            "model.test_project.my_model": [],
            "model.test_project.upstream_model": ["model.test_project.my_model"]
        },
        "disabled": {},
        "selectors": {}
    }
    manifest_file = tmp_path / "manifest.json"
    with open(manifest_file, "w") as f:
        json.dump(manifest_data, f)
    return str(manifest_file)


def test_custom_dagster_dbt_translator_with_asset_prefix(dummy_manifest_path) -> None:
    with unittest.mock.patch.dict(
        os.environ,
        {
            "DAGSTER_ASSET_PREFIX": "TEST_PREFIX",
        },
    ):

        @dagster_lib.dbt_assets(
            manifest=dummy_manifest_path,
            dagster_dbt_translator=dbt_assets.FwCustomDagsterDbtTranslator(),
        )
        def dbt_project_assets(): ...  # type: ignore[no-untyped-def]

        assert "DAGSTER_ASSET_PREFIX" in os.environ
        assert dbt_project_assets.asset_deps.keys()
        assert all(
            key.has_prefix(["TEST_PREFIX"])
            for key in dbt_project_assets.asset_deps.keys()
        )
        assert all(
            dep.has_prefix(["TEST_PREFIX"])
            for deps in dbt_project_assets.asset_deps.values()
            for dep in deps
        )


def test_custom_dagster_dbt_translator_without_prefix(
    dummy_manifest_path
) -> None:
    with unittest.mock.patch.dict(
        os.environ,
        {},
        clear=True,
    ):

        @dagster_lib.dbt_assets(
            manifest=dummy_manifest_path,
            dagster_dbt_translator=dbt_assets.FwCustomDagsterDbtTranslator(),
        )
        def dbt_project_assets(): ...  # type: ignore[no-untyped-def]

        assert "DAGSTER_ASSET_PREFIX" not in os.environ
        assert dbt_project_assets.asset_deps.keys()

        # Check that we have deps to check against (not empty)
        deps_list = [
            dep
            for deps in dbt_project_assets.asset_deps.values()
            for dep in deps
        ]
        assert len(deps_list) > 0

        assert not all(
            key.has_prefix(["TEST_PREFIX"])
            for key in dbt_project_assets.asset_deps.keys()
        )
        assert not all(
            dep.has_prefix(["TEST_PREFIX"])
            for dep in deps_list
        )


def test_get_compiled_code_path() -> None:
    """Test get_compiled_code_path."""
    error_message = """
1 of 9 ERROR creating sql incremental model DWH_BATCH.TRN_DOTC_ORDER

  Database Error in model TRN_DOTC_ORDER (models/DWH/CLEANED/BATCH/TRN_DOTC_ORDER.sql)
  100035 (22007): Timestamp '2024-12-03 21:44:32.101045 UTC' is not recognized
  compiled code at /target/dbt-f013ed6/DWH/CLEANED/BATCH/TRN_DOTC_ORDER.sql
"""
    assert (
        dbt_assets.get_compiled_code_path(error_message)
        == "/target/dbt-f013ed6/DWH/CLEANED/BATCH/TRN_DOTC_ORDER.sql"
    )
