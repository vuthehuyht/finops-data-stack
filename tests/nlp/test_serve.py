"""Tests for src.nlp.serve (SageMaker inference container entrypoint)."""

import json

import pytest

from src.nlp.serve import input_fn, output_fn


def test_input_fn_parses_json() -> None:
    body = json.dumps({"article_id": "A1", "text": "Gia co phieu tang manh"}).encode()
    result = input_fn(body, "application/json")
    assert result == {"article_id": "A1", "text": "Gia co phieu tang manh"}


def test_input_fn_rejects_non_json_content_type() -> None:
    with pytest.raises(ValueError, match="Unsupported content type"):
        input_fn(b"irrelevant", "text/plain")


def test_output_fn_serializes_json() -> None:
    payload = {
        "article_id": "A1",
        "sentiment_score": 0.5,
        "sentiment_label": "positive",
    }
    result = output_fn(payload, "application/json")
    assert json.loads(result) == payload


def test_output_fn_rejects_non_json_accept() -> None:
    with pytest.raises(ValueError, match="Unsupported accept type"):
        output_fn({}, "text/plain")
