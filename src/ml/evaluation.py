"""Model evaluation, versioning, and Champion/Challenger promotion logic."""

import io
import json
import tarfile
from typing import Any

from src.ml.config import MODEL_NAME


def extract_metadata_from_tarball(tarball_bytes: bytes) -> dict[str, Any]:
    """Extract and parse `metadata.json` from a SageMaker model.tar.gz.

    Args:
        tarball_bytes: Raw bytes of the `model.tar.gz` archive.

    Returns:
        The parsed `metadata.json` contents.

    Raises:
        KeyError: If the tarball does not contain a usable metadata.json.
    """
    with tarfile.open(fileobj=io.BytesIO(tarball_bytes), mode="r:gz") as tar:
        try:
            member = tar.getmember("metadata.json")
        except KeyError as exc:
            raise KeyError("model.tar.gz does not contain metadata.json") from exc
        extracted = tar.extractfile(member)
        if extracted is None:
            raise KeyError("metadata.json member has no content")
        return json.loads(extracted.read())


def compare_and_promote(
    challenger_metrics: dict[str, float] | None,
    champion_metrics: dict[str, float] | None,
    threshold: float,
) -> bool:
    """Decide whether the Challenger model should replace the Champion.

    A missing Champion (bootstrap case, `champion_metrics is None`) always
    promotes. Otherwise the Challenger must reduce RMSE by at least
    `threshold` (a fraction, e.g. 0.05 = 5%) relative to the Champion.

    Args:
        challenger_metrics: `{"rmse": ..., "mae": ...}` of the new model.
        champion_metrics: Metrics of the currently active model, or None.
        threshold: Minimum required relative RMSE improvement to promote.

    Raises:
        ValueError: If `challenger_metrics` is None.
    """
    if challenger_metrics is None:
        raise ValueError("challenger_metrics is required")
    if champion_metrics is None:
        return True

    champion_rmse = champion_metrics["rmse"]
    challenger_rmse = challenger_metrics["rmse"]
    if champion_rmse <= 0:
        return challenger_rmse < champion_rmse
    improvement = (champion_rmse - challenger_rmse) / champion_rmse
    return improvement >= threshold


def model_version_prefix(version: str) -> str:
    """Build the S3 key prefix (within the artifacts bucket) for a model version."""
    return f"{MODEL_NAME}/{version}/"
