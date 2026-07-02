"""Structural validation for the External Secrets Operator manifests that
sync AWS Secrets Manager's finops-db-credentials into the k8s Secret
dagster-pg-credentials (namespace dagster), refreshed hourly.

No live cluster or AWS Secrets Manager access is available in this
environment, so these tests only check YAML structure: the ExternalSecret
must remap rds_password -> postgresql-password (the key name the Dagster
Helm chart's own secret-postgres.yaml template expects), and must carry
over the other 9 keys from the AWS secret unchanged.
"""

from pathlib import Path

import yaml

MANIFEST_DIR = (
    Path(__file__).resolve().parents[2]
    / "src"
    / "k8s"
    / "manifest"
    / "external-secrets"
)

# The 10 keys in the AWS Secrets Manager secret "finops-db-credentials",
# per infrastructure/terraform/modules/secrets/main.tf.
AWS_SECRET_KEYS = {
    "rds_host",
    "rds_username",
    "rds_password",
    "rds_dbname",
    "redshift_host",
    "redshift_username",
    "redshift_password",
    "redshift_dbname",
    "ssi_token",
    "investing_token",
}


def _load(filename: str) -> dict:
    with (MANIFEST_DIR / filename).open() as f:
        return yaml.safe_load(f)


def test_secret_store_targets_aws_secrets_manager_via_irsa_service_account():
    manifest = _load("secret-store.yaml")

    assert manifest["apiVersion"] == "external-secrets.io/v1beta1"
    assert manifest["kind"] == "SecretStore"
    assert manifest["metadata"]["name"] == "aws-secrets-manager"
    assert manifest["metadata"]["namespace"] == "dagster"

    aws_provider = manifest["spec"]["provider"]["aws"]
    assert aws_provider["service"] == "SecretsManager"
    assert aws_provider["region"] == "ap-southeast-1"

    service_account_ref = aws_provider["auth"]["jwt"]["serviceAccountRef"]
    assert service_account_ref["name"] == "external-secrets-sa"
    assert service_account_ref["namespace"] == "external-secrets"


def test_external_secret_refreshes_hourly_and_targets_dagster_pg_credentials():
    manifest = _load("external-secret-dagster-pg.yaml")

    assert manifest["apiVersion"] == "external-secrets.io/v1beta1"
    assert manifest["kind"] == "ExternalSecret"
    assert manifest["metadata"]["name"] == "dagster-pg-credentials"
    assert manifest["metadata"]["namespace"] == "dagster"

    spec = manifest["spec"]
    assert spec["refreshInterval"] == "1h"
    assert spec["secretStoreRef"] == {
        "name": "aws-secrets-manager",
        "kind": "SecretStore",
    }
    assert spec["target"]["name"] == "dagster-pg-credentials"
    assert spec["target"]["creationPolicy"] == "Owner"


def test_external_secret_remaps_rds_password_to_postgresql_password():
    manifest = _load("external-secret-dagster-pg.yaml")

    entries = manifest["spec"]["data"]
    by_secret_key = {entry["secretKey"]: entry["remoteRef"] for entry in entries}

    assert by_secret_key["postgresql-password"] == {
        "key": "finops-db-credentials",
        "property": "rds_password",
    }
    # rds_password must NOT also appear unmapped under its own name -- the
    # Dagster chart only ever reads the "postgresql-password" key.
    assert "rds_password" not in by_secret_key


def test_external_secret_carries_over_the_other_nine_keys_unchanged():
    manifest = _load("external-secret-dagster-pg.yaml")

    entries = manifest["spec"]["data"]
    by_secret_key = {entry["secretKey"]: entry["remoteRef"] for entry in entries}

    remaining_aws_keys = AWS_SECRET_KEYS - {"rds_password"}
    for aws_key in remaining_aws_keys:
        assert by_secret_key[aws_key] == {
            "key": "finops-db-credentials",
            "property": aws_key,
        }

    # Exactly 10 entries total: 1 remapped (postgresql-password) + 9 passthrough.
    assert len(entries) == 10
