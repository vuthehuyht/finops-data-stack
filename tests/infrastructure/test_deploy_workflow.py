"""Structural validation for .github/workflows/deploy.yml: the two
independently-triggered jobs (deploy-app reads Terraform state only;
deploy-infra runs terraform apply) both end by calling the shared
deploy-dagster-to-eks composite action.
"""

from pathlib import Path

import yaml

WORKFLOW_PATH = (
    Path(__file__).resolve().parents[2] / ".github" / "workflows" / "deploy.yml"
)


def _load_workflow():
    with WORKFLOW_PATH.open() as f:
        return yaml.safe_load(f)


def test_deploy_app_job_triggers_only_on_helm_values_path_and_never_applies_terraform():
    workflow = _load_workflow()
    triggers = workflow[True]["push"]

    assert triggers["branches"] == ["main"]
    assert "infrastructure/helm/values.yaml" in triggers["paths"]
    assert "infrastructure/terraform/**" in triggers["paths"]

    job = workflow["jobs"]["deploy-app"]
    expected_if = (
        "contains(github.event.head_commit.modified, 'infrastructure/helm/values.yaml')"
    )
    assert job["if"] == expected_if

    steps = job["steps"]
    run_scripts = "\n".join(s.get("run", "") for s in steps)
    assert "terraform apply" not in run_scripts
    assert "terraform init" in run_scripts
    assert "terraform output" in run_scripts


def test_deploy_infra_job_triggers_on_terraform_path_and_runs_apply():
    workflow = _load_workflow()

    job = workflow["jobs"]["deploy-infra"]
    assert (
        job["if"]
        == "contains(github.event.head_commit.modified, 'infrastructure/terraform/')"
    )

    steps = job["steps"]
    run_scripts = "\n".join(s.get("run", "") for s in steps)
    assert "terraform apply -auto-approve" in run_scripts
    assert "terraform output" in run_scripts


def test_both_jobs_have_oidc_permissions_and_call_the_composite_action():
    workflow = _load_workflow()

    for job_name in ("deploy-app", "deploy-infra"):
        job = workflow["jobs"][job_name]
        assert job["permissions"]["id-token"] == "write"
        assert job["permissions"]["contents"] == "read"

        uses_values = [s.get("uses", "") for s in job["steps"]]
        assert "./.github/actions/deploy-dagster-to-eks" in uses_values


def test_both_jobs_pass_all_required_terraform_outputs_to_the_composite_action():
    workflow = _load_workflow()

    required_with_keys = {
        "aws-role-arn",
        "eks-cluster-name",
        "ecr-repository-url",
        "rds-address",
        "rds-username",
        "rds-dbname",
        "dagster-sa-role-arn",
        "external-secrets-sa-role-arn",
    }

    for job_name in ("deploy-app", "deploy-infra"):
        job = workflow["jobs"][job_name]
        deploy_step = next(
            s
            for s in job["steps"]
            if s.get("uses") == "./.github/actions/deploy-dagster-to-eks"
        )
        assert required_with_keys <= set(deploy_step["with"].keys())
