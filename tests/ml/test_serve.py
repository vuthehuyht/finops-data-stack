"""Tests for src/ml/serve.py."""

import json

import pytest
import torch


def test_model_fn_loads_state_dict_from_model_dir(tmp_path) -> None:
    from src.ml.config import SEQUENCE_FEATURE_COLUMNS, TABULAR_FEATURE_COLUMNS
    from src.ml.model import FusionModel
    from src.ml.serve import model_fn

    saved_model = FusionModel(
        sequence_input_size=len(SEQUENCE_FEATURE_COLUMNS),
        tabular_input_size=len(TABULAR_FEATURE_COLUMNS),
    )
    torch.save(saved_model.state_dict(), tmp_path / "model.pt")

    loaded = model_fn(str(tmp_path))

    assert loaded.training is False
    # Loaded weights match what was saved.
    for name, param in saved_model.state_dict().items():
        assert torch.equal(param, loaded.state_dict()[name])


def test_input_fn_parses_json_body() -> None:
    from src.ml.serve import input_fn

    body = json.dumps({"sequence": [[1.0]], "tabular": [2.0]}).encode("utf-8")

    result = input_fn(body, "application/json")

    assert result == {"sequence": [[1.0]], "tabular": [2.0]}


def test_input_fn_raises_on_unsupported_content_type() -> None:
    from src.ml.serve import input_fn

    with pytest.raises(ValueError, match="Unsupported content type"):
        input_fn(b"<xml/>", "application/xml")


def test_predict_fn_echoes_ticker_alongside_prediction() -> None:
    from src.ml.model import FusionModel
    from src.ml.serve import predict_fn

    model = FusionModel(sequence_input_size=2, tabular_input_size=2)
    model.eval()
    input_data = {
        "ticker": "AAA",
        "sequence": [[0.1, 0.2], [0.3, 0.4]],
        "tabular": [0.5, 0.6],
    }

    result = predict_fn(input_data, model)

    assert result["ticker"] == "AAA"
    assert isinstance(result["predicted_return"], float)


def test_output_fn_serializes_ticker_and_prediction_to_json() -> None:
    from src.ml.serve import output_fn

    result = output_fn(
        {"ticker": "AAA", "predicted_return": 0.0123}, "application/json"
    )

    assert json.loads(result) == {"ticker": "AAA", "predicted_return": 0.0123}


def test_output_fn_raises_on_unsupported_accept() -> None:
    from src.ml.serve import output_fn

    with pytest.raises(ValueError, match="Unsupported accept type"):
        output_fn({"predicted_return": 0.0}, "text/csv")
