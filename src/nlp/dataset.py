"""Loads the labeled dataset for Phase C fine-tuning.

Expects a CSV with `text` and `label` columns, `label` in
{"negative", "neutral", "positive"}. The public dataset's own label scheme is
mapped to this shape once, offline, not at training time.
"""

import csv

# Integer ids follow the pretrained checkpoint's config.json id2label
# ({0: NEG, 1: POS, 2: NEU}) so fine-tuning keeps the classifier head aligned
# and serve.py can keep mapping classes through id2label.
LABEL_TO_ID = {"negative": 0, "positive": 1, "neutral": 2}


def load_training_dataset(path: str) -> tuple[list[str], list[int]]:
    """Load `(texts, labels)` from a `text,label` CSV.

    Raises:
        ValueError: If a row's `label` isn't one of the three known classes,
            or the file has no rows.
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
