"""Structural validation for .github/workflows/docker.yml's CI/CD deploy
extension: path-filtered push trigger, ECR push, and auto-PR steps.

No live GitHub Actions runner is available in this environment, so this
only checks YAML structure and that expected steps/conditions exist.
"""

from pathlib import Path

import yaml

WORKFLOW_PATH = (
    Path(__file__).resolve().parents[2] / ".github" / "workflows" / "docker.yml"
)

EXPECTED_PUSH_PATHS = {
    "src/dagster/**",
    "src/pipeline/dagster/**",
    "src/common/**",
    "src/docker/**",
    "pyproject.toml",
    "uv.lock",
}


def _load_workflow():
    with WORKFLOW_PATH.open() as f:
        # "on:" parses as the boolean key True in PyYAML's default loader
        # under YAML 1.1 -- reproduce that instead of fighting it, since
        # that's how GitHub's own parser treats this file too.
        return yaml.safe_load(f)


def test_push_trigger_is_path_filtered_to_dagster_paths_but_pull_request_is_not():
    workflow = _load_workflow()

    triggers = workflow[True]  # the "on:" key
    assert triggers["pull_request"] is None

    push_trigger = triggers["push"]
    assert push_trigger["branches"] == ["main"]
    assert set(push_trigger["paths"]) == EXPECTED_PUSH_PATHS


def test_job_permissions_include_id_token_write_for_oidc():
    workflow = _load_workflow()
    job = workflow["jobs"]["check-docker"]

    assert job["permissions"]["id-token"] == "write"
    assert job["permissions"]["contents"] == "read"
    assert job["permissions"]["pull-requests"] == "write"


def test_docker_build_step_pushes_only_and_uses_timestamp_tag_on_push_events():
    workflow = _load_workflow()
    steps = workflow["jobs"]["check-docker"]["steps"]

    build_step = next(s for s in steps if s.get("name") == "Docker Build")
    assert build_step["with"]["push"] == "${{ github.event_name == 'push' }}"
    tags_expr = build_step["with"]["tags"]
    assert "env.IMAGE_TAG" in tags_expr
    # The ECR repository name must be part of the push target, not just the
    # registry hostname (steps.ecr-login.outputs.registry is registry-only,
    # e.g. "<account>.dkr.ecr.<region>.amazonaws.com" with no repo path) --
    # matches aws_ecr_repository.dagster_app's name in
    # infrastructure/terraform/modules/ecr/main.tf ("${var.project_name}-dagster-app",
    # project_name default "finops" per infrastructure/terraform/variables.tf).
    assert "finops-dagster-app" in tags_expr


def test_workflow_generates_utc_timestamp_tag_only_on_push():
    workflow = _load_workflow()
    steps = workflow["jobs"]["check-docker"]["steps"]

    tag_step = next(s for s in steps if s.get("name") == "Generate image tag")
    assert tag_step.get("if") == "github.event_name == 'push'"
    assert "date -u +%Y%m%d-%H%M" in tag_step["run"]


def test_bump_tag_step_edits_exact_values_yaml_line_with_sed():
    workflow = _load_workflow()
    steps = workflow["jobs"]["check-docker"]["steps"]

    bump_step = next(
        s for s in steps if s.get("name") == "Bump image tag in Helm values"
    )
    assert bump_step.get("if") == "github.event_name == 'push'"
    assert "infrastructure/helm/values.yaml" in bump_step["run"]
    assert "sed" in bump_step["run"]


def test_create_pull_request_step_uses_fixed_rolling_branch_name():
    workflow = _load_workflow()
    steps = workflow["jobs"]["check-docker"]["steps"]

    pr_step = next(
        s
        for s in steps
        if s.get("uses", "").startswith("peter-evans/create-pull-request")
    )
    assert pr_step.get("if") == "github.event_name == 'push'"
    assert pr_step["with"]["branch"] == "deploy/bump-dagster-image-tag"
