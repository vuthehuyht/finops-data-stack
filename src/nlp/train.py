"""SageMaker script-mode entrypoint: fine-tune the sentiment model.

The `train` channel holds train.csv and val.csv, plus an optional
eval_domain.csv (hand-labeled financial news) that drives promotion.
"""

import argparse
import copy
import json
import os
import random
import shutil
from datetime import datetime
from zoneinfo import ZoneInfo

import torch
from transformers import AutoModelForSequenceClassification, AutoTokenizer

try:
    from src.nlp.config import MAX_TOKEN_LENGTH, MODEL_ID, SENTIMENT_SCHEMA_VERSION
    from src.nlp.dataset import load_training_dataset
    from src.nlp.evaluation import macro_f1
except ImportError:  # SageMaker copies source_dir flat
    from config import MAX_TOKEN_LENGTH, MODEL_ID, SENTIMENT_SCHEMA_VERSION
    from dataset import load_training_dataset
    from evaluation import macro_f1

# Bundled into the artifact so it is servable as-is.
_SERVING_FILES = ("serve.py", "config.py", "sentiment_model.py", "requirements.txt")
_SEED = 42


def iter_batches(indices: list[int], batch_size: int):
    """Yield consecutive slices of at most `batch_size`."""
    for start in range(0, len(indices), batch_size):
        yield indices[start : start + batch_size]


def bundle_serving_code(model_dir: str) -> None:
    """Copy the serving code into `model_dir/code/`."""
    source_dir = os.path.dirname(os.path.abspath(__file__))
    code_dir = os.path.join(model_dir, "code")
    os.makedirs(code_dir, exist_ok=True)
    for filename in _SERVING_FILES:
        shutil.copy(
            os.path.join(source_dir, filename), os.path.join(code_dir, filename)
        )


def build_metadata(
    model_version: str,
    epochs: int,
    batch_size: int,
    learning_rate: float,
    val_macro_f1: float,
    domain_macro_f1: float | None,
    train_rows: int,
    val_rows: int,
) -> dict:
    """Assemble metadata.json."""
    return {
        "model_version": model_version,
        "trained_at": datetime.now(ZoneInfo("Asia/Ho_Chi_Minh")).isoformat(),
        "base_model_id": MODEL_ID,
        "sentiment_schema_version": SENTIMENT_SCHEMA_VERSION,
        "hyperparameters": {
            "epochs": epochs,
            "batch_size": batch_size,
            "learning_rate": learning_rate,
        },
        "val_macro_f1": val_macro_f1,
        "domain_macro_f1": domain_macro_f1,
        "train_rows": train_rows,
        "val_rows": val_rows,
    }


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--epochs", type=int, default=3)
    parser.add_argument("--batch-size", type=int, default=16)
    parser.add_argument("--learning-rate", type=float, default=2e-5)
    parser.add_argument(
        "--train-dir", type=str, default=os.environ.get("SM_CHANNEL_TRAIN", ".")
    )
    parser.add_argument(
        "--model-dir", type=str, default=os.environ.get("SM_MODEL_DIR", ".")
    )
    return parser.parse_args()


def _encode(tokenizer, texts: list[str], device):
    return tokenizer(
        texts,
        truncation=True,
        padding=True,
        max_length=MAX_TOKEN_LENGTH,
        return_tensors="pt",
    ).to(device)


def _predict(model, tokenizer, texts: list[str], batch_size: int, device) -> list[int]:
    model.eval()
    predictions: list[int] = []
    with torch.no_grad():
        for batch in iter_batches(list(range(len(texts))), batch_size):
            encoded = _encode(tokenizer, [texts[i] for i in batch], device)
            predictions.extend(model(**encoded).logits.argmax(dim=-1).tolist())
    return predictions


def main() -> None:
    args = _parse_args()
    random.seed(_SEED)
    torch.manual_seed(_SEED)

    train_texts, train_labels = load_training_dataset(
        os.path.join(args.train_dir, "train.csv")
    )
    val_texts, val_labels = load_training_dataset(
        os.path.join(args.train_dir, "val.csv")
    )
    domain_path = os.path.join(args.train_dir, "eval_domain.csv")
    domain_texts, domain_labels = (
        load_training_dataset(domain_path) if os.path.exists(domain_path) else ([], [])
    )

    device = "cuda" if torch.cuda.is_available() else "cpu"
    tokenizer = AutoTokenizer.from_pretrained(MODEL_ID)
    model = AutoModelForSequenceClassification.from_pretrained(MODEL_ID).to(device)
    optimizer = torch.optim.AdamW(model.parameters(), lr=args.learning_rate)

    best_f1 = -1.0
    best_state = copy.deepcopy(model.state_dict())
    for epoch in range(args.epochs):
        model.train()
        order = list(range(len(train_texts)))
        random.shuffle(order)
        for batch in iter_batches(order, args.batch_size):
            encoded = _encode(tokenizer, [train_texts[i] for i in batch], device)
            labels = torch.tensor([train_labels[i] for i in batch], device=device)
            loss = model(**encoded, labels=labels).loss
            loss.backward()
            torch.nn.utils.clip_grad_norm_(model.parameters(), 1.0)
            optimizer.step()
            optimizer.zero_grad()

        val_f1 = macro_f1(
            val_labels, _predict(model, tokenizer, val_texts, args.batch_size, device)
        )
        print(f"epoch={epoch + 1} val_macro_f1={val_f1:.4f}")
        if val_f1 > best_f1:
            best_f1 = val_f1
            best_state = copy.deepcopy(model.state_dict())

    model.load_state_dict(best_state)
    domain_f1 = None
    if domain_texts:
        domain_f1 = macro_f1(
            domain_labels,
            _predict(model, tokenizer, domain_texts, args.batch_size, device),
        )
        print(f"domain_macro_f1={domain_f1:.4f}")

    model.save_pretrained(args.model_dir)
    tokenizer.save_pretrained(args.model_dir)
    bundle_serving_code(args.model_dir)

    metadata = build_metadata(
        model_version=os.environ.get("TRAINING_JOB_NAME", "local"),
        epochs=args.epochs,
        batch_size=args.batch_size,
        learning_rate=args.learning_rate,
        val_macro_f1=best_f1,
        domain_macro_f1=domain_f1,
        train_rows=len(train_texts),
        val_rows=len(val_texts),
    )
    with open(
        os.path.join(args.model_dir, "metadata.json"), "w", encoding="utf-8"
    ) as f:
        json.dump(metadata, f, indent=2)


if __name__ == "__main__":
    main()
