"""Torch-free score mapping and artifact metadata checks."""

import json
import os

try:
    from src.nlp.config import SENTIMENT_SCHEMA_VERSION
except ImportError:  # SageMaker copies code/ flat
    from config import SENTIMENT_SCHEMA_VERSION

_CLASSES = ("positive", "negative", "neutral")

# Checkpoint id2label is {0: NEG, 1: POS, 2: NEU}.
_CHECKPOINT_LABEL_TO_CLASS = {
    "NEG": "negative",
    "POS": "positive",
    "NEU": "neutral",
}


def probs_to_class_scores(
    probs: list[float], id2label: dict[int | str, str]
) -> dict[str, float]:
    """Name each probability via the checkpoint's id2label.

    Raises:
        ValueError: On a length mismatch or a label outside NEG/POS/NEU.
    """
    if len(probs) != len(id2label):
        raise ValueError(
            f"Probability count {len(probs)} does not match label count {len(id2label)}"
        )

    scores: dict[str, float] = {}
    for label_id, label in id2label.items():
        if label not in _CHECKPOINT_LABEL_TO_CLASS:
            raise ValueError(f"Unexpected checkpoint label: {label}")
        scores[_CHECKPOINT_LABEL_TO_CLASS[label]] = probs[int(label_id)]
    return scores


def scores_to_sentiment(scores: dict[str, float]) -> tuple[float, str]:
    """Return (P(positive) - P(negative), most likely class).

    Raises:
        KeyError: If a class is missing.
    """
    missing = [name for name in _CLASSES if name not in scores]
    if missing:
        raise KeyError(f"Missing sentiment classes: {missing}")

    label = max(_CLASSES, key=lambda name: scores[name])
    return round(scores["positive"] - scores["negative"], 4), label


def load_model_metadata(model_dir: str) -> dict:
    """Read metadata.json, or {} if absent.

    Raises:
        ValueError: If its schema version differs from the code's.
    """
    path = os.path.join(model_dir, "metadata.json")
    if not os.path.exists(path):
        return {}
    with open(path, encoding="utf-8") as f:
        metadata = json.load(f)

    version = metadata.get("sentiment_schema_version")
    if version is not None and version != SENTIMENT_SCHEMA_VERSION:
        raise ValueError(
            f"Model schema version {version} does not match "
            f"code schema version {SENTIMENT_SCHEMA_VERSION}; retrain the model."
        )
    return metadata
