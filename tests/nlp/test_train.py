"""Tests for the torch-free parts of src.nlp.train."""

import os

from src.nlp.config import MODEL_ID, SENTIMENT_SCHEMA_VERSION
from src.nlp.train import build_metadata, bundle_serving_code, iter_batches


def test_iter_batches_covers_every_index_once() -> None:
    batches = list(iter_batches(list(range(7)), batch_size=3))

    assert batches == [[0, 1, 2], [3, 4, 5], [6]]


def test_build_metadata_records_scores_and_versions() -> None:
    metadata = build_metadata(
        model_version="job-1",
        epochs=3,
        batch_size=16,
        learning_rate=2e-5,
        val_macro_f1=0.81,
        domain_macro_f1=None,
        train_rows=100,
        val_rows=20,
    )

    assert metadata["model_version"] == "job-1"
    assert metadata["base_model_id"] == MODEL_ID
    assert metadata["sentiment_schema_version"] == SENTIMENT_SCHEMA_VERSION
    assert metadata["val_macro_f1"] == 0.81
    assert metadata["domain_macro_f1"] is None
    assert metadata["hyperparameters"] == {
        "epochs": 3,
        "batch_size": 16,
        "learning_rate": 2e-5,
    }


def test_bundle_serving_code_copies_files_into_code_dir(tmp_path) -> None:
    bundle_serving_code(str(tmp_path))

    code_dir = tmp_path / "code"
    for filename in ("serve.py", "config.py", "sentiment_model.py", "requirements.txt"):
        assert os.path.exists(code_dir / filename), filename
