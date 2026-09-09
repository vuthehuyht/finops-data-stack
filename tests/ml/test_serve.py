"""Tests for src/ml/serve.py."""

import json

import pytest
import torch

from src.ml.config import (
    FEATURE_SCHEMA_VERSION,
    SECTOR_VOCAB,
    SEQUENCE_FEATURE_COLUMNS,
    TABULAR_FEATURE_COLUMNS,
    TABULAR_VECTOR_SIZE,
)
from src.ml.model import FusionModel


def _write_artifact(tmp_path, *, schema=FEATURE_SCHEMA_VERSION):
    model = FusionModel(
        sequence_input_size=len(SEQUENCE_FEATURE_COLUMNS),
        tabular_input_size=TABULAR_VECTOR_SIZE,
        num_sectors=len(SECTOR_VOCAB),
    )
    torch.save(model.state_dict(), tmp_path / "model.pt")
    (tmp_path / "metadata.json").write_text(
        json.dumps(
            {
                "feature_schema_version": schema,
                "tabular_medians": {},
                "sector_vocab": SECTOR_VOCAB,
            }
        )
    )
    return model


def test_model_fn_returns_bundle_and_loads_weights(tmp_path) -> None:
    from src.ml.serve import model_fn

    saved = _write_artifact(tmp_path)
    model, medians, sector_vocab = model_fn(str(tmp_path))

    assert model.training is False
    assert medians == {}
    assert sector_vocab == SECTOR_VOCAB
    expected_device = "cuda" if torch.cuda.is_available() else "cpu"
    assert next(model.parameters()).device.type == expected_device
    for name, param in saved.state_dict().items():
        assert torch.equal(param.to(expected_device), model.state_dict()[name])


def test_model_fn_rejects_stale_schema(tmp_path) -> None:
    from src.ml.serve import model_fn

    _write_artifact(tmp_path, schema=1)
    with pytest.raises(ValueError, match="feature_schema_version"):
        model_fn(str(tmp_path))


def test_input_fn_parses_json_body() -> None:
    from src.ml.serve import input_fn

    body = json.dumps(
        {
            "ticker": "AAA",
            "sequence": [[1.0]],
            "tabular": {"roe": 0.1},
            "sector": "bank",
        }
    ).encode("utf-8")

    result = input_fn(body, "application/json")

    assert result["ticker"] == "AAA"
    assert result["sector"] == "bank"


def test_input_fn_raises_on_unsupported_content_type() -> None:
    from src.ml.serve import input_fn

    with pytest.raises(ValueError, match="Unsupported content type"):
        input_fn(b"<xml/>", "application/xml")


def _payload(sector="non_financial"):
    return {
        "ticker": "AAA",
        "sequence": [[0.0] * len(SEQUENCE_FEATURE_COLUMNS) for _ in range(30)],
        "tabular": dict.fromkeys(TABULAR_FEATURE_COLUMNS, 1.0),
        "sector": sector,
    }


def test_predict_fn_echoes_ticker_alongside_prediction() -> None:
    from src.ml.serve import predict_fn

    model = FusionModel(
        sequence_input_size=len(SEQUENCE_FEATURE_COLUMNS),
        tabular_input_size=TABULAR_VECTOR_SIZE,
        num_sectors=len(SECTOR_VOCAB),
    )
    model.eval()

    result = predict_fn(_payload(), (model, {}, SECTOR_VOCAB))

    assert result["ticker"] == "AAA"
    assert isinstance(result["predicted_return"], float)


def test_predict_fn_returns_none_on_non_finite() -> None:
    from src.ml.serve import predict_fn

    class _NanModel:
        def __call__(self, *a, **k):
            return torch.tensor([[float("nan")]])

    result = predict_fn(_payload("bank"), (_NanModel(), {}, SECTOR_VOCAB))

    assert result["ticker"] == "AAA"
    assert result["predicted_return"] is None
    assert "NaN" not in json.dumps(result)


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
