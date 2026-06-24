"""Unit tests for redshift_util."""

import os
import unittest.mock
from typing import Any

import pytest
from botocore.exceptions import ClientError

from src.common import redshift_util


@pytest.fixture
def clean_env() -> Any:
    """Fixture to clean up Redshift environment variables before and after each test."""
    keys = [
        "FINOPS_ENVIRONMENT",
        "REDSHIFT_PASSWORD",
        "REDSHIFT_USER",
        "REDSHIFT_HOST",
        "REDSHIFT_DATABASE",
        "REDSHIFT_PORT",
        "RDS_PASSWORD",
        "RDS_USER",
        "RDS_HOST",
        "RDS_PORT",
        "RDS_DATABASE",
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


def test_inject_secrets_from_aws_not_prod(clean_env: Any) -> None:
    """Ensure secrets are not injected if FINOPS_ENVIRONMENT is not prod."""
    os.environ["FINOPS_ENVIRONMENT"] = "dev"
    with unittest.mock.patch("boto3.client") as mock_boto:
        redshift_util.inject_secrets_from_aws()
        mock_boto.assert_not_called()


def test_inject_secrets_from_aws_already_has_password(clean_env: Any) -> None:
    """Ensure secrets are not injected if REDSHIFT_PASSWORD is already set."""
    os.environ["FINOPS_ENVIRONMENT"] = "prod"
    os.environ["REDSHIFT_PASSWORD"] = "existing_password"
    with unittest.mock.patch("boto3.client") as mock_boto:
        redshift_util.inject_secrets_from_aws()
        mock_boto.assert_not_called()


def test_inject_secrets_from_aws_success(clean_env: Any) -> None:
    """Test successful retrieval of database credentials from AWS Secrets Manager."""
    os.environ["FINOPS_ENVIRONMENT"] = "prod"

    mock_client = unittest.mock.Mock()
    mock_client.get_secret_value.return_value = {
        "SecretString": (
            '{"password": "secret_pwd", "username": "secret_user", '
            '"host": "secret_host", "dbname": "secret_db"}'
        )
    }

    with unittest.mock.patch("boto3.client", return_value=mock_client) as mock_boto:
        redshift_util.inject_secrets_from_aws()
        mock_boto.assert_called_once_with(
            "secretsmanager", region_name="ap-southeast-1"
        )

        assert os.environ.get("REDSHIFT_PASSWORD") == "secret_pwd"
        assert os.environ.get("REDSHIFT_USER") == "secret_user"
        assert os.environ.get("REDSHIFT_HOST") == "secret_host"
        assert os.environ.get("REDSHIFT_DATABASE") == "secret_db"


def test_inject_secrets_from_aws_success_consolidated(clean_env: Any) -> None:
    """Test successful retrieval of consolidated database credentials."""
    os.environ["FINOPS_ENVIRONMENT"] = "prod"

    mock_client = unittest.mock.Mock()
    mock_client.get_secret_value.return_value = {
        "SecretString": (
            '{"redshift_password": "secret_pwd", "redshift_username": "secret_user", '
            '"redshift_host": "secret_host", "redshift_dbname": "secret_db", '
            '"redshift_port": "5439", "rds_password": "rds_pwd"}'
        )
    }

    with unittest.mock.patch("boto3.client", return_value=mock_client):
        redshift_util.inject_secrets_from_aws()

        assert os.environ.get("REDSHIFT_PASSWORD") == "secret_pwd"
        assert os.environ.get("REDSHIFT_USER") == "secret_user"
        assert os.environ.get("REDSHIFT_HOST") == "secret_host"
        assert os.environ.get("REDSHIFT_DATABASE") == "secret_db"
        assert os.environ.get("REDSHIFT_PORT") == "5439"
        assert os.environ.get("RDS_PASSWORD") == "rds_pwd"


def test_inject_secrets_from_aws_error(clean_env: Any) -> None:
    """Test error handling when AWS Secrets Manager call fails."""
    os.environ["FINOPS_ENVIRONMENT"] = "prod"

    mock_client = unittest.mock.Mock()
    # Mock botocore ClientError
    error_response = {
        "Error": {"Code": "DecryptionFailureException", "Message": "Error"}
    }
    mock_client.get_secret_value.side_effect = ClientError(
        error_response, "GetSecretValue"
    )

    with unittest.mock.patch("boto3.client", return_value=mock_client):
        with pytest.raises(ClientError):
            redshift_util.inject_secrets_from_aws()


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
