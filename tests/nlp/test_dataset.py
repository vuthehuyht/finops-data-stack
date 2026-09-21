"""Tests for src.nlp.dataset."""

import csv

import pytest

from src.nlp.dataset import LABEL_TO_ID, load_training_dataset


def _write_csv(path, rows) -> None:
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["text", "label"])
        writer.writerows(rows)


def test_label_ids_follow_pretrained_checkpoint_order() -> None:
    # The checkpoint's config.json is {0: NEG, 1: POS, 2: NEU}. Training with a
    # different order would make serve.py (which reads id2label) mislabel rows.
    assert LABEL_TO_ID == {"negative": 0, "positive": 1, "neutral": 2}


def test_load_training_dataset_parses_csv(tmp_path) -> None:
    path = tmp_path / "sample.csv"
    _write_csv(
        path,
        [
            ["san pham rat tot", "positive"],
            ["san pham te", "negative"],
            ["binh thuong", "neutral"],
        ],
    )

    texts, labels = load_training_dataset(str(path))

    assert texts == ["san pham rat tot", "san pham te", "binh thuong"]
    assert labels == [1, 0, 2]


def test_load_training_dataset_normalizes_label_case(tmp_path) -> None:
    path = tmp_path / "case.csv"
    _write_csv(path, [["abc", " Positive "]])

    assert load_training_dataset(str(path)) == (["abc"], [1])


def test_load_training_dataset_rejects_unknown_label(tmp_path) -> None:
    path = tmp_path / "bad.csv"
    _write_csv(path, [["abc", "mixed"]])

    with pytest.raises(ValueError, match="Unknown label"):
        load_training_dataset(str(path))


def test_load_training_dataset_rejects_empty_file(tmp_path) -> None:
    path = tmp_path / "empty.csv"
    _write_csv(path, [])

    with pytest.raises(ValueError, match="No rows"):
        load_training_dataset(str(path))
