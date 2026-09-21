"""Torch-free metrics and champion/challenger promotion for the sentiment model."""

# SSM parameter with the promoted version; unset or "none" means pretrained.
ACTIVE_VERSION_PARAM = "/finops/nlp_sentiment/active_version"


def macro_f1(labels: list[int], predictions: list[int]) -> float:
    """Unweighted mean F1 over classes seen in labels or predictions."""
    if len(labels) != len(predictions):
        raise ValueError(
            f"labels and predictions length differ: {len(labels)} vs {len(predictions)}"
        )

    scores = []
    for cls in sorted(set(labels) | set(predictions)):
        true_pos = sum(
            1 for y, p in zip(labels, predictions, strict=True) if y == cls == p
        )
        false_pos = sum(
            1 for y, p in zip(labels, predictions, strict=True) if y != cls == p
        )
        false_neg = sum(
            1 for y, p in zip(labels, predictions, strict=True) if y == cls != p
        )
        denominator = 2 * true_pos + false_pos + false_neg
        scores.append(2 * true_pos / denominator if denominator else 0.0)
    return sum(scores) / len(scores) if scores else 0.0


def promotion_score(metadata: dict) -> float:
    """In-domain macro-F1 if present, else validation macro-F1."""
    domain_score = metadata.get("domain_macro_f1")
    if domain_score is not None:
        return float(domain_score)
    return float(metadata["val_macro_f1"])


def promotion_basis(metadata: dict) -> str:
    """Set behind promotion_score: "domain" or "val"."""
    return "domain" if metadata.get("domain_macro_f1") is not None else "val"


def should_promote(challenger_score: float, champion_score: float | None) -> bool:
    """Promote if there is no champion or the challenger strictly beats it."""
    return champion_score is None or challenger_score > champion_score


def model_version_prefix(version: str) -> str:
    """S3 key prefix of a fine-tuned model version."""
    return f"nlp-sentiment/versions/{version}/"
