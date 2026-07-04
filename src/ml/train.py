"""SageMaker script-mode entrypoint for training the FinOps multimodal regressor.

Runs standalone inside the SageMaker PyTorch container — not imported by
Dagster. Uses `print()` (not `context.log`) since SageMaker captures stdout
to CloudWatch; there is no Dagster execution context here.
"""

import argparse
import glob
import json
import os
import shutil
from dataclasses import asdict, dataclass
from datetime import UTC, datetime

import pandas as pd
import torch
from torch.utils.data import DataLoader

try:
    # Package-relative import: used when pytest imports this module as
    # `src.ml.train` from the repo root, where the `src` package resolves.
    from src.ml.config import (
        SEQUENCE_FEATURE_COLUMNS,
        TABULAR_FEATURE_COLUMNS,
        WINDOW_SIZE,
    )
    from src.ml.dataset import StockSequenceDataset, time_based_split
    from src.ml.model import FusionModel
except ImportError:
    # Sibling import: SageMaker script mode copies `source_dir`'s contents
    # flat into /opt/ml/input/data/code/, so there is no `src` package there
    # — config.py/dataset.py/model.py/train.py are plain siblings.
    from config import SEQUENCE_FEATURE_COLUMNS, TABULAR_FEATURE_COLUMNS, WINDOW_SIZE
    from dataset import StockSequenceDataset, time_based_split
    from model import FusionModel


# Files copied into model_dir/code/ so every trained model.tar.gz is
# self-contained and directly servable by the SageMaker inference container
# (SAGEMAKER_PROGRAM=serve.py, SAGEMAKER_SUBMIT_DIRECTORY=/opt/ml/model/code)
# — no separate code-packaging step needed at promotion time.
_SERVING_FILES = ("serve.py", "inference.py", "model.py", "config.py")


def _bundle_serving_code(model_dir: str) -> None:
    """Copy the serving entrypoint + its dependencies into `model_dir/code/`."""
    source_dir = os.path.dirname(os.path.abspath(__file__))
    code_dir = os.path.join(model_dir, "code")
    os.makedirs(code_dir, exist_ok=True)
    for filename in _SERVING_FILES:
        shutil.copy(
            os.path.join(source_dir, filename), os.path.join(code_dir, filename)
        )


@dataclass
class TrainingMetrics:
    """Regression evaluation metrics."""

    rmse: float
    mae: float


def compute_regression_metrics(
    predictions: torch.Tensor, targets: torch.Tensor
) -> TrainingMetrics:
    """Compute RMSE and MAE between model predictions and true targets."""
    errors = predictions.detach().reshape(-1) - targets.detach().reshape(-1)
    rmse = torch.sqrt(torch.mean(errors**2)).item()
    mae = torch.mean(torch.abs(errors)).item()
    return TrainingMetrics(rmse=rmse, mae=mae)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--window-size", type=int, default=WINDOW_SIZE)
    parser.add_argument("--epochs", type=int, default=20)
    parser.add_argument("--batch-size", type=int, default=64)
    parser.add_argument("--learning-rate", type=float, default=1e-3)
    parser.add_argument("--train-end-date", type=str, required=True)
    parser.add_argument("--val-end-date", type=str, required=True)
    parser.add_argument(
        "--train-dir", type=str, default=os.environ.get("SM_CHANNEL_TRAIN", ".")
    )
    parser.add_argument(
        "--model-dir", type=str, default=os.environ.get("SM_MODEL_DIR", ".")
    )
    return parser.parse_args()


def _load_training_dataframe(train_dir: str) -> pd.DataFrame:
    """Load and concatenate all Parquet files under `train_dir`.

    Raises:
        FileNotFoundError: If no Parquet files are found.
    """
    parquet_files = sorted(glob.glob(os.path.join(train_dir, "*.parquet")))
    if not parquet_files:
        raise FileNotFoundError(f"No Parquet files found under {train_dir}")
    return pd.concat((pd.read_parquet(f) for f in parquet_files), ignore_index=True)


def _train_one_epoch(
    model: FusionModel,
    loader: DataLoader,
    optimizer: torch.optim.Optimizer,
    loss_fn: torch.nn.Module,
) -> float:
    model.train()
    total_loss = 0.0
    for sequence, tabular, target in loader:
        optimizer.zero_grad()
        prediction = model(sequence, tabular).squeeze(-1)
        loss = loss_fn(prediction, target)
        loss.backward()
        optimizer.step()
        total_loss += loss.item() * sequence.size(0)
    return total_loss / len(loader.dataset)


@torch.no_grad()
def _evaluate(model: FusionModel, loader: DataLoader) -> TrainingMetrics:
    model.eval()
    all_predictions = []
    all_targets = []
    for sequence, tabular, target in loader:
        prediction = model(sequence, tabular).squeeze(-1)
        all_predictions.append(prediction)
        all_targets.append(target)
    return compute_regression_metrics(
        torch.cat(all_predictions), torch.cat(all_targets)
    )


def main() -> None:
    args = _parse_args()
    df = _load_training_dataframe(args.train_dir)
    train_df, val_df, test_df = time_based_split(
        df, args.train_end_date, args.val_end_date
    )

    train_dataset = StockSequenceDataset(train_df, window_size=args.window_size)
    val_dataset = StockSequenceDataset(val_df, window_size=args.window_size)
    test_dataset = StockSequenceDataset(test_df, window_size=args.window_size)

    for split_name, dataset in (
        ("train", train_dataset),
        ("val", val_dataset),
        ("test", test_dataset),
    ):
        if len(dataset) == 0:
            raise ValueError(
                f"{split_name} split produced 0 windows (need >= "
                f"WINDOW_SIZE={args.window_size} trading rows per ticker) — "
                "widen the date range."
            )

    train_loader = DataLoader(train_dataset, batch_size=args.batch_size, shuffle=True)
    val_loader = DataLoader(val_dataset, batch_size=args.batch_size)
    test_loader = DataLoader(test_dataset, batch_size=args.batch_size)

    model = FusionModel(
        sequence_input_size=len(SEQUENCE_FEATURE_COLUMNS),
        tabular_input_size=len(TABULAR_FEATURE_COLUMNS),
    )
    optimizer = torch.optim.Adam(model.parameters(), lr=args.learning_rate)
    loss_fn = torch.nn.HuberLoss()

    for epoch in range(args.epochs):
        train_loss = _train_one_epoch(model, train_loader, optimizer, loss_fn)
        val_metrics = _evaluate(model, val_loader)
        print(
            f"epoch={epoch} train_loss={train_loss:.6f} "
            f"val_rmse={val_metrics.rmse:.6f} val_mae={val_metrics.mae:.6f}"
        )

    test_metrics = _evaluate(model, test_loader)
    print(f"test_rmse={test_metrics.rmse:.6f} test_mae={test_metrics.mae:.6f}")

    os.makedirs(args.model_dir, exist_ok=True)
    torch.save(model.state_dict(), os.path.join(args.model_dir, "model.pt"))
    _bundle_serving_code(args.model_dir)

    metadata = {
        "trained_at": datetime.now(UTC).isoformat(),
        "hyperparameters": {
            "window_size": args.window_size,
            "epochs": args.epochs,
            "batch_size": args.batch_size,
            "learning_rate": args.learning_rate,
            "train_end_date": args.train_end_date,
            "val_end_date": args.val_end_date,
        },
        "feature_columns": {
            "sequence": SEQUENCE_FEATURE_COLUMNS,
            "tabular": TABULAR_FEATURE_COLUMNS,
        },
        "metrics": asdict(test_metrics),
        "train_rows": len(train_dataset),
        "val_rows": len(val_dataset),
        "test_rows": len(test_dataset),
    }
    with open(
        os.path.join(args.model_dir, "metadata.json"), "w", encoding="utf-8"
    ) as f:
        json.dump(metadata, f, indent=2)


if __name__ == "__main__":
    main()
