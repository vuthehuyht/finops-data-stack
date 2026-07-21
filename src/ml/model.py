"""Multimodal Hybrid Neural Network: LSTM time-series branch + MLP tabular branch.

See docs/ml-architecture-design.md section 2 for the architecture rationale.
"""

import torch
from torch import nn

try:
    # Package-relative import: used when pytest imports this module as
    # `src.ml.model` from the repo root, where the `src` package resolves.
    from src.ml.config import (
        DROPOUT_RATE,
        FUSION_HIDDEN_SIZE,
        LSTM_HIDDEN_SIZE,
        LSTM_NUM_LAYERS,
        MLP_HIDDEN_SIZES,
    )
except ImportError:
    # Sibling import: SageMaker script mode copies `source_dir`'s contents
    # flat into /opt/ml/input/data/code/, so there is no `src` package there
    # — config.py is a plain sibling of model.py in that directory.
    from config import (
        DROPOUT_RATE,
        FUSION_HIDDEN_SIZE,
        LSTM_HIDDEN_SIZE,
        LSTM_NUM_LAYERS,
        MLP_HIDDEN_SIZES,
    )


class TimeSeriesBranch(nn.Module):
    """LSTM branch extracting momentum/volatility/sentiment signals from sequences."""

    def __init__(
        self,
        input_size: int,
        hidden_size: int = LSTM_HIDDEN_SIZE,
        num_layers: int = LSTM_NUM_LAYERS,
    ) -> None:
        super().__init__()
        self.lstm = nn.LSTM(
            input_size=input_size,
            hidden_size=hidden_size,
            num_layers=num_layers,
            batch_first=True,
        )

    def forward(self, sequence: torch.Tensor) -> torch.Tensor:
        """Args: sequence of shape (batch, window_size, input_size)."""
        _, (last_hidden, _) = self.lstm(sequence)
        return last_hidden[-1]


class TabularBranch(nn.Module):
    """MLP branch extracting fundamental/macro signals from the latest snapshot."""

    def __init__(
        self,
        input_size: int,
        hidden_sizes: tuple[int, ...] = MLP_HIDDEN_SIZES,
        dropout_rate: float = DROPOUT_RATE,
    ) -> None:
        super().__init__()
        layers: list[nn.Module] = []
        in_size = input_size
        for hidden_size in hidden_sizes:
            layers.append(nn.Linear(in_size, hidden_size))
            layers.append(nn.ReLU())
            layers.append(nn.Dropout(dropout_rate))
            in_size = hidden_size
        self.mlp = nn.Sequential(*layers)

    def forward(self, tabular: torch.Tensor) -> torch.Tensor:
        """Args: tabular of shape (batch, input_size)."""
        return self.mlp(tabular)


class FusionModel(nn.Module):
    """Concatenates the time-series and tabular branches to predict expected return."""

    def __init__(
        self,
        sequence_input_size: int,
        tabular_input_size: int,
        fusion_hidden_size: int = FUSION_HIDDEN_SIZE,
    ) -> None:
        super().__init__()
        self.time_series_branch = TimeSeriesBranch(input_size=sequence_input_size)
        self.tabular_branch = TabularBranch(input_size=tabular_input_size)
        fused_input_size = LSTM_HIDDEN_SIZE + MLP_HIDDEN_SIZES[-1]
        self.fusion = nn.Sequential(
            nn.Linear(fused_input_size, fusion_hidden_size),
            nn.ReLU(),
            nn.Linear(fusion_hidden_size, 1),
        )

    def forward(self, sequence: torch.Tensor, tabular: torch.Tensor) -> torch.Tensor:
        """Args: sequence (batch, window_size, seq_dim), tabular (batch, tab_dim)."""
        sequence_features = self.time_series_branch(sequence)
        tabular_features = self.tabular_branch(tabular)
        fused = torch.cat([sequence_features, tabular_features], dim=1)
        return self.fusion(fused)
