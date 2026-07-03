"""Unit tests for redshift_util."""

import os
import unittest.mock
from typing import Any

import pytest

from src.common import redshift_util


@pytest.fixture
def clean_env() -> Any:
    """Fixture to clean up Redshift environment variables before and after each test."""
    keys = [
        "REDSHIFT_PASSWORD",
        "REDSHIFT_USER",
        "REDSHIFT_HOST",
        "REDSHIFT_DATABASE",
        "REDSHIFT_PORT",
    ]
    old_values = {k: os.environ.get(k) for k in keys}

    for k in keys:
        if k in os.environ:
            del os.environ[k]

    yield

    for k, v in old_values.items():
        if v is not None:
            os.environ[k] = v
        elif k in os.environ:
            del os.environ[k]


def test_get_redshift_connection(clean_env: Any) -> None:
    """Test establishing psycopg2 connection to Redshift."""
    os.environ["REDSHIFT_HOST"] = "test_host"
    os.environ["REDSHIFT_PORT"] = "5439"
    os.environ["REDSHIFT_DATABASE"] = "test_db"
    os.environ["REDSHIFT_USER"] = "test_user"
    os.environ["REDSHIFT_PASSWORD"] = "test_pwd"

    with unittest.mock.patch("psycopg2.connect") as mock_connect:
        redshift_util.get_redshift_connection()
        mock_connect.assert_called_once_with(
            host="test_host",
            port=5439,
            database="test_db",
            user="test_user",
            password="test_pwd",
        )


def test_execute_query() -> None:
    """Test executing queries through the helper function."""
    mock_cursor = unittest.mock.Mock()
    mock_cursor.query = b"SELECT * FROM test;"

    redshift_util.execute_query(mock_cursor, "SELECT * FROM test;", (1, 2))
    mock_cursor.execute.assert_called_once_with("SELECT * FROM test;", (1, 2))
