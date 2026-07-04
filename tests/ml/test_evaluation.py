"""Tests for src/ml/evaluation.py."""

import io
import json
import tarfile


def _make_tarball(files: dict[str, bytes]) -> bytes:
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode="w:gz") as tar:
        for name, content in files.items():
            info = tarfile.TarInfo(name=name)
            info.size = len(content)
            tar.addfile(info, io.BytesIO(content))
    return buffer.getvalue()


def test_extract_metadata_from_tarball_returns_parsed_json() -> None:
    from src.ml.evaluation import extract_metadata_from_tarball

    metadata = {"metrics": {"rmse": 0.05, "mae": 0.03}}
    tarball = _make_tarball(
        {
            "model.pt": b"fake-weights",
            "metadata.json": json.dumps(metadata).encode("utf-8"),
        }
    )

    result = extract_metadata_from_tarball(tarball)

    assert result == metadata


def test_extract_metadata_from_tarball_raises_when_missing() -> None:
    from src.ml.evaluation import extract_metadata_from_tarball

    tarball = _make_tarball({"model.pt": b"fake-weights"})

    try:
        extract_metadata_from_tarball(tarball)
        raise AssertionError("expected KeyError")
    except KeyError:
        pass


def test_compare_and_promote_bootstraps_when_no_champion() -> None:
    from src.ml.evaluation import compare_and_promote

    assert compare_and_promote({"rmse": 0.10}, None, threshold=0.05) is True


def test_compare_and_promote_true_when_improvement_meets_threshold() -> None:
    from src.ml.evaluation import compare_and_promote

    # champion rmse=0.10, challenger rmse=0.09 -> 10% improvement >= 5% threshold
    promoted = compare_and_promote({"rmse": 0.09}, {"rmse": 0.10}, threshold=0.05)
    assert promoted is True


def test_compare_and_promote_false_when_improvement_below_threshold() -> None:
    from src.ml.evaluation import compare_and_promote

    # champion rmse=0.10, challenger rmse=0.099 -> 1% improvement < 5% threshold
    promoted = compare_and_promote({"rmse": 0.099}, {"rmse": 0.10}, threshold=0.05)
    assert promoted is False


def test_compare_and_promote_false_when_challenger_worse() -> None:
    from src.ml.evaluation import compare_and_promote

    promoted = compare_and_promote({"rmse": 0.20}, {"rmse": 0.10}, threshold=0.05)
    assert promoted is False


def test_compare_and_promote_raises_without_challenger_metrics() -> None:
    from src.ml.evaluation import compare_and_promote

    try:
        compare_and_promote(None, {"rmse": 0.1}, threshold=0.05)
        raise AssertionError("expected ValueError")
    except ValueError:
        pass


def test_model_version_prefix() -> None:
    from src.ml.evaluation import model_version_prefix

    assert model_version_prefix("finops-multimodal-regressor-20260703") == (
        "finops-multimodal-regressor/finops-multimodal-regressor-20260703/"
    )
