import os
import unittest.mock

from src.dagster import environment


def test_is_prod() -> None:
    with unittest.mock.patch.dict(
        os.environ, {"DAGSTER_WORKSPACE_ENVIRONMENT": "prod"}
    ):
        assert environment.is_prod() is True

    with unittest.mock.patch.dict(os.environ, {"DAGSTER_WORKSPACE_ENVIRONMENT": "dev"}):
        assert environment.is_prod() is False

    with unittest.mock.patch.dict(os.environ, {}, clear=True):
        assert environment.is_prod() is False
