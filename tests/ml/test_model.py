"""Tests for src/ml/model.py."""

import torch

from src.ml.config import (
    SECTOR_VOCAB,
    SEQUENCE_FEATURE_COLUMNS,
    TABULAR_VECTOR_SIZE,
    WINDOW_SIZE,
)
from src.ml.model import FusionModel, TabularBranch, TimeSeriesBranch


def test_fusion_model_forward_with_sector_idx() -> None:
    model = FusionModel(
        sequence_input_size=len(SEQUENCE_FEATURE_COLUMNS),
        tabular_input_size=TABULAR_VECTOR_SIZE,
        num_sectors=len(SECTOR_VOCAB),
    )
    model.eval()
    batch = 4
    sequence = torch.randn(batch, WINDOW_SIZE, len(SEQUENCE_FEATURE_COLUMNS))
    tabular = torch.randn(batch, TABULAR_VECTOR_SIZE)
    sector_idx = torch.randint(0, len(SECTOR_VOCAB), (batch,))
    out = model(sequence, tabular, sector_idx)
    assert out.shape == (batch, 1)
    assert torch.isfinite(out).all()


def test_fusion_model_batch_size_one() -> None:
    model = FusionModel(
        sequence_input_size=len(SEQUENCE_FEATURE_COLUMNS),
        tabular_input_size=TABULAR_VECTOR_SIZE,
        num_sectors=len(SECTOR_VOCAB),
    )
    model.eval()
    out = model(
        torch.randn(1, WINDOW_SIZE, len(SEQUENCE_FEATURE_COLUMNS)),
        torch.randn(1, TABULAR_VECTOR_SIZE),
        torch.tensor([0]),
    )
    assert out.shape == (1, 1)


def test_time_series_branch_forward_output_shape() -> None:
    batch_size, window_size, sequence_dim = 4, 30, 36
    branch = TimeSeriesBranch(input_size=sequence_dim, hidden_size=16)

    output = branch(torch.randn(batch_size, window_size, sequence_dim))

    assert output.shape == (batch_size, 16)


def test_tabular_branch_forward_output_shape() -> None:
    batch_size, tabular_dim = 4, 18
    branch = TabularBranch(input_size=tabular_dim, hidden_sizes=(32, 16))

    output = branch(torch.randn(batch_size, tabular_dim))

    assert output.shape == (batch_size, 16)
