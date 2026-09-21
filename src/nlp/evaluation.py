"""Champion/challenger comparison for the fine-tuned sentiment model.

Torch-free so it can run in the Dagster code location. A challenger only
replaces the active champion on a strict improvement, never a tie, to avoid
promotion churn from noise-level fluctuations.
"""

# SSM parameter holding the promoted fine-tuned version ("none"/unset means the
# pretrained Phase A checkpoint is still serving).
ACTIVE_VERSION_PARAM = "/finops/nlp_sentiment/active_version"


def macro_f1(labels: list[int], predictions: list[int]) -> float:
    """Unweighted mean F1 over the classes that appear in labels or predictions.

    Raises:
        ValueError: If the two sequences differ in length.
    """
    if len(labels) != len(predictions):
        raise ValueError(
            f"labels and predictions length differ: {len(labels)} vs {len(predictions)}"
        )

    scores = []
    for cls in sorted(set(labels) | set(predictions)):
        true_pos = sum(
            1 for y, p in zip(labels, predictions, strict=True) if y == cls and p == cls
        )
        false_pos = sum(
            1 for y, p in zip(labels, predictions, strict=True) if y != cls and p == cls
        )
        false_neg = sum(
            1 for y, p in zip(labels, predictions, strict=True) if y == cls and p != cls
        )
        denominator = 2 * true_pos + false_pos + false_neg
        scores.append(2 * true_pos / denominator if denominator else 0.0)
    return sum(scores) / len(scores) if scores else 0.0


def promotion_score(metadata: dict) -> float:
    """Score used for champion/challenger comparison.

    The in-domain (financial news) macro-F1 wins when the training run had an
    in-domain evaluation set, because the public training data is general
    domain and its own validation split overstates real quality.

    Raises:
        KeyError: If neither score is present.
    """
    domain_score = metadata.get("domain_macro_f1")
    if domain_score is not None:
        return float(domain_score)
    return float(metadata["val_macro_f1"])


def promotion_basis(metadata: dict) -> str:
    """Which evaluation set produced `promotion_score`: "domain" or "val".

    Scores from different sets are not comparable, so callers must only
    compare a challenger and champion measured on the same basis.
    """
    return "domain" if metadata.get("domain_macro_f1") is not None else "val"


def should_promote(challenger_score: float, champion_score: float | None) -> bool:
    """Decide whether the challenger should become the new active model.

    Returns:
        True if there is no champion yet, or the challenger strictly beats it.
    """
    if champion_score is None:
        return True
    return challenger_score > champion_score


def model_version_prefix(version: str) -> str:
    """S3 key prefix (within the artifacts bucket) for a fine-tuned model version."""
    return f"nlp-sentiment/versions/{version}/"
