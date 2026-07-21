"""Tests for src/ml/model.py."""

import torch


def test_fusion_model_forward_output_shape() -> None:
    from src.ml.model import FusionModel

    batch_size, window_size, sequence_dim, tabular_dim = 4, 30, 36, 18
    model = FusionModel(
        sequence_input_size=sequence_dim, tabular_input_size=tabular_dim
    )

    sequence = torch.randn(batch_size, window_size, sequence_dim)
    tabular = torch.randn(batch_size, tabular_dim)

    output = model(sequence, tabular)

    assert output.shape == (batch_size, 1)


def test_time_series_branch_forward_output_shape() -> None:
    from src.ml.model import TimeSeriesBranch

    batch_size, window_size, sequence_dim = 4, 30, 36
    branch = TimeSeriesBranch(input_size=sequence_dim, hidden_size=16)

    output = branch(torch.randn(batch_size, window_size, sequence_dim))

    assert output.shape == (batch_size, 16)


def test_tabular_branch_forward_output_shape() -> None:
    from src.ml.model import TabularBranch

    batch_size, tabular_dim = 4, 18
    branch = TabularBranch(input_size=tabular_dim, hidden_sizes=(32, 16))

    output = branch(torch.randn(batch_size, tabular_dim))

    assert output.shape == (batch_size, 16)
