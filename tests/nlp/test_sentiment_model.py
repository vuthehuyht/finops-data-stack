"""Tests for src.nlp.sentiment_model."""

import json

import pytest

from src.nlp.sentiment_model import (
    load_model_metadata,
    probs_to_class_scores,
    scores_to_sentiment,
)


def test_scores_to_sentiment_positive_dominant() -> None:
    score, label = scores_to_sentiment(
        {"positive": 0.9, "negative": 0.05, "neutral": 0.05}
    )
    assert score == 0.85
    assert label == "positive"


def test_scores_to_sentiment_negative_dominant() -> None:
    score, label = scores_to_sentiment(
        {"positive": 0.05, "negative": 0.9, "neutral": 0.05}
    )
    assert score == -0.85
    assert label == "negative"


def test_scores_to_sentiment_neutral_dominant() -> None:
    score, label = scores_to_sentiment(
        {"positive": 0.2, "negative": 0.2, "neutral": 0.6}
    )
    assert score == 0.0
    assert label == "neutral"


def test_scores_to_sentiment_missing_key_raises() -> None:
    with pytest.raises(KeyError):
        scores_to_sentiment({"positive": 0.5, "negative": 0.5})


def test_probs_to_class_scores_maps_checkpoint_labels() -> None:
    # Mirrors the wonrax checkpoint's id2label: {0: NEG, 1: POS, 2: NEU}.
    id2label = {0: "NEG", 1: "POS", 2: "NEU"}
    scores = probs_to_class_scores([0.1, 0.7, 0.2], id2label)
    assert scores == {"negative": 0.1, "positive": 0.7, "neutral": 0.2}


def test_probs_to_class_scores_accepts_string_ids() -> None:
    # config.json round-trips id2label keys as strings on some loaders.
    scores = probs_to_class_scores(
        [0.1, 0.7, 0.2], {"0": "NEG", "1": "POS", "2": "NEU"}
    )
    assert scores["positive"] == 0.7


def test_probs_to_class_scores_unknown_label_raises() -> None:
    with pytest.raises(ValueError, match="Unexpected checkpoint label"):
        probs_to_class_scores([0.5, 0.5], {0: "NEG", 1: "MIXED"})


def test_probs_to_class_scores_length_mismatch_raises() -> None:
    with pytest.raises(ValueError, match="does not match"):
        probs_to_class_scores([0.5, 0.5], {0: "NEG", 1: "POS", 2: "NEU"})


def test_load_model_metadata_returns_empty_when_missing(tmp_path) -> None:
    assert load_model_metadata(str(tmp_path)) == {}


def test_load_model_metadata_reads_matching_schema_version(tmp_path) -> None:
    metadata = {"model_version": "job-1", "sentiment_schema_version": 1}
    (tmp_path / "metadata.json").write_text(json.dumps(metadata), encoding="utf-8")

    assert load_model_metadata(str(tmp_path)) == metadata


def test_load_model_metadata_rejects_stale_schema_version(tmp_path) -> None:
    metadata = {"sentiment_schema_version": 999}
    (tmp_path / "metadata.json").write_text(json.dumps(metadata), encoding="utf-8")

    with pytest.raises(ValueError, match="schema version"):
        load_model_metadata(str(tmp_path))
