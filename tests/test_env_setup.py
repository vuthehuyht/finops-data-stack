import os
from unittest.mock import MagicMock, patch

from src.common.redshift import inject_secrets_from_aws


def test_inject_secrets_from_aws_dev(monkeypatch):
    """Verify that in dev environment, Secrets Manager is not contacted."""
    monkeypatch.setenv("FINOPS_ENVIRONMENT", "dev")
    with patch("boto3.client") as mock_client:
        inject_secrets_from_aws()
        mock_client.assert_not_called()


def test_inject_secrets_from_aws_prod_already_has_password(monkeypatch):
    """Verify that if password is already loaded, boto3 is not called."""
    monkeypatch.setenv("FINOPS_ENVIRONMENT", "prod")
    monkeypatch.setenv("REDSHIFT_PASSWORD", "pre-existing-password")
    with patch("boto3.client") as mock_client:
        inject_secrets_from_aws()
        mock_client.assert_not_called()


def test_inject_secrets_from_aws_prod_fetches_and_injects(monkeypatch):
    """Verify Secrets Manager values are fetched and injected correctly on prod."""
    monkeypatch.setenv("FINOPS_ENVIRONMENT", "prod")
    monkeypatch.delenv("REDSHIFT_PASSWORD", raising=False)
    monkeypatch.delenv("REDSHIFT_HOST", raising=False)
    monkeypatch.delenv("REDSHIFT_USER", raising=False)
    monkeypatch.delenv("REDSHIFT_DATABASE", raising=False)

    mock_boto_client = MagicMock()
    mock_boto_client.get_secret_value.return_value = {
        "SecretString": (
            '{"password": "secret-pass", "host": "secret-host", '
            '"username": "secret-user", "database": "secret-db"}'
        )
    }

    with patch("boto3.client", return_value=mock_boto_client) as mock_boto:
        inject_secrets_from_aws()
        mock_boto.assert_called_once_with(
            "secretsmanager", region_name="ap-southeast-1"
        )
        mock_boto_client.get_secret_value.assert_called_once_with(
            SecretId="prod/finops/redshift"
        )

        assert os.environ.get("REDSHIFT_PASSWORD") == "secret-pass"
        assert os.environ.get("REDSHIFT_HOST") == "secret-host"
        assert os.environ.get("REDSHIFT_USER") == "secret-user"
        assert os.environ.get("REDSHIFT_DATABASE") == "secret-db"
