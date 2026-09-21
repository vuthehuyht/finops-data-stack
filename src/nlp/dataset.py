"""Labeled CSV loader (`text,label`) for fine-tuning."""

import csv

# Ids follow the checkpoint's id2label so the classifier head stays aligned.
LABEL_TO_ID = {"negative": 0, "positive": 1, "neutral": 2}


def load_training_dataset(path: str) -> tuple[list[str], list[int]]:
    """Load (texts, label ids).

    Raises:
        ValueError: On an unknown label or an empty file.
    """
    texts: list[str] = []
    labels: list[int] = []
    with open(path, encoding="utf-8") as f:
        for row in csv.DictReader(f):
            label = row["label"].strip().lower()
            if label not in LABEL_TO_ID:
                raise ValueError(f"Unknown label: {label!r}")
            texts.append(row["text"])
            labels.append(LABEL_TO_ID[label])

    if not texts:
        raise ValueError(f"No rows found in {path}")
    return texts, labels
