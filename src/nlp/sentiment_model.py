"""Score-mapping logic for the news sentiment pipeline.

Torch-free by design (mirrors src/ml/features.py's split) so it's testable
without a GPU or the `transformers` dependency installed.
"""

_EXPECTED_LABELS = ("positive", "negative", "neutral")

# The pretrained checkpoint's config.json uses id2label {0: NEG, 1: POS, 2: NEU};
# map it to the class names scores_to_sentiment() expects.
_CHECKPOINT_LABEL_TO_CLASS = {
    "NEG": "negative",
    "POS": "positive",
    "NEU": "neutral",
}


def probs_to_class_scores(
    probs: list[float], id2label: dict[int | str, str]
) -> dict[str, float]:
    """Pair softmax probabilities with class names using the checkpoint's id2label.

    Reading the order from the model config (instead of hardcoding it) keeps
    scores correct if a fine-tuned checkpoint reorders its labels.

    Raises:
        ValueError: If the label count does not match `probs`, or a label is
            not one of NEG/POS/NEU.
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
    """Map a 3-class softmax distribution to a signed score and a label.

    Args:
        scores: Softmax probabilities keyed by "positive", "negative",
            "neutral" (as returned by the pretrained classifier's label set).

    Returns:
        (score, label) where score = P(positive) - P(negative) in [-1, 1],
        and label is whichever of the three classes has the highest
        probability.

    Raises:
        KeyError: If any of the three expected keys is missing.
    """
    missing = [key for key in _EXPECTED_LABELS if key not in scores]
    if missing:
        raise KeyError(f"Missing sentiment classes: {missing}")

    label = max(_EXPECTED_LABELS, key=lambda key: scores[key])
    return round(scores["positive"] - scores["negative"], 4), label
