"""Structural validation for the deploy-dagster-to-eks composite action.

No live cluster/AWS access is available in this environment, so this only
checks YAML structure and that the shell steps contain the expected
commands in the expected order -- catching typos or missing steps without
needing to actually run the action.
"""

from pathlib import Path

import yaml

ACTION_PATH = (
    Path(__file__).resolve().parents[2]
    / ".github"
    / "actions"
    / "deploy-dagster-to-eks"
    / "action.yml"
)

REQUIRED_INPUTS = {
    "aws-role-arn",
    "eks-cluster-name",
    "ecr-repository-url",
    "rds-address",
    "rds-username",
    "rds-dbname",
    "dagster-sa-role-arn",
    "external-secrets-sa-role-arn",
}


def _load_action():
    with ACTION_PATH.open() as f:
        return yaml.safe_load(f)


def _all_run_scripts(action: dict) -> str:
    """Concatenate every step's `run` script, for substring checks."""
    steps = action["runs"]["steps"]
    return "\n".join(step.get("run", "") for step in steps)


def test_action_declares_composite_run_with_all_required_inputs():
    action = _load_action()

    assert action["runs"]["using"] == "composite"

    declared_inputs = set(action["inputs"].keys())
    assert REQUIRED_INPUTS <= declared_inputs

    for name in REQUIRED_INPUTS:
        assert action["inputs"][name]["required"] is True

    assert action["inputs"]["aws-region"]["default"] == "ap-southeast-1"


def test_action_configures_aws_credentials_before_any_aws_cli_step():
    action = _load_action()
    steps = action["runs"]["steps"]

    uses_values = [step.get("uses", "") for step in steps]
    aws_creds_index = next(
        i
        for i, u in enumerate(uses_values)
        if u.startswith("aws-actions/configure-aws-credentials")
    )
    eks_kubeconfig_index = next(
        i
        for i, step in enumerate(steps)
        if "aws eks update-kubeconfig" in step.get("run", "")
    )

    assert aws_creds_index < eks_kubeconfig_index


def test_action_creates_namespaces_and_service_accounts_before_helm_installs():
    action = _load_action()
    scripts = _all_run_scripts(action)

    dagster_ns_pos = scripts.find("namespace dagster")
    external_secrets_ns_pos = scripts.find("namespace external-secrets")
    dagster_sa_pos = scripts.find("name: dagster-sa")
    eso_sa_pos = scripts.find("name: external-secrets-sa")
    eso_helm_pos = scripts.find("helm upgrade --install external-secrets")
    external_secret_apply_pos = scripts.find("src/k8s/manifest/external-secrets/")
    dagster_helm_pos = scripts.find("helm upgrade --install dagster dagster/dagster")

    positions = [
        dagster_ns_pos,
        external_secrets_ns_pos,
        dagster_sa_pos,
        eso_sa_pos,
        eso_helm_pos,
        external_secret_apply_pos,
        dagster_helm_pos,
    ]
    assert all(p != -1 for p in positions), positions
    assert positions == sorted(positions)


def test_action_dagster_helm_upgrade_sets_ecr_and_rds_values_from_inputs():
    action = _load_action()
    scripts = _all_run_scripts(action)

    assert "dagster-user-deployments.deployments[0].image.repository" in scripts
    assert "postgresql.postgresqlHost" in scripts
    assert "postgresql.postgresqlUsername" in scripts
    assert "postgresql.postgresqlDatabase" in scripts
    assert "${{ inputs.ecr-repository-url }}" in scripts
    assert "${{ inputs.rds-address }}" in scripts
    assert "${{ inputs.rds-username }}" in scripts
    assert "${{ inputs.rds-dbname }}" in scripts


def test_action_service_accounts_annotated_with_correct_irsa_role_inputs():
    action = _load_action()
    scripts = _all_run_scripts(action)

    dagster_sa_block_start = scripts.find("name: dagster-sa")
    dagster_sa_block = scripts[dagster_sa_block_start : dagster_sa_block_start + 300]
    assert "${{ inputs.dagster-sa-role-arn }}" in dagster_sa_block

    eso_sa_block_start = scripts.find("name: external-secrets-sa")
    eso_sa_block = scripts[eso_sa_block_start : eso_sa_block_start + 300]
    assert "${{ inputs.external-secrets-sa-role-arn }}" in eso_sa_block


def test_action_waits_for_dagster_pg_credentials_secret_before_dagster_helm_upgrade():
    action = _load_action()
    scripts = _all_run_scripts(action)

    wait_pos = scripts.find("kubectl get secret dagster-pg-credentials")
    dagster_helm_pos = scripts.find("helm upgrade --install dagster dagster/dagster")

    assert wait_pos != -1
    assert wait_pos < dagster_helm_pos
