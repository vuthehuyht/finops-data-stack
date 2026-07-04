"""SageMaker inference container entrypoint (sagemaker-inference-toolkit convention).

Bundled into model.tar.gz's `code/` directory by `train.py` at training time
and run inside the built-in PyTorch inference DLC container.
`SAGEMAKER_PROGRAM=serve.py` + `SAGEMAKER_SUBMIT_DIRECTORY=/opt/ml/model/code`
(set on the SageMaker Model resource — see
`infrastructure/terraform/modules/sagemaker/` and the promotion step in
`src/dagster/ml_job.py`) tell the toolkit baked into the DLC image to import
this module and call the four functions below at request time. This is a
container-runtime convention independent of the `sagemaker` client SDK
version — symmetric to how `train.py` is a plain script the training DLC
image runs.
"""

import json
import os

import torch

try:
    # Package-relative import: used when pytest imports this module as
    # `src.ml.serve` from the repo root, where the `src` package resolves.
    from src.ml.config import SEQUENCE_FEATURE_COLUMNS, TABULAR_FEATURE_COLUMNS
    from src.ml.inference import predict_from_payload
    from src.ml.model import FusionModel
except ImportError:
    # Sibling import: SageMaker copies the bundled `code/` directory's
    # contents flat, so there is no `src` package there — config.py/
    # inference.py/model.py are plain siblings of serve.py in that directory.
    from config import SEQUENCE_FEATURE_COLUMNS, TABULAR_FEATURE_COLUMNS
    from inference import predict_from_payload
    from model import FusionModel

_CONTENT_TYPE_JSON = "application/json"


def model_fn(model_dir: str) -> FusionModel:
    """Load the trained FusionModel from `model_dir/model.pt`."""
    model = FusionModel(
        sequence_input_size=len(SEQUENCE_FEATURE_COLUMNS),
        tabular_input_size=len(TABULAR_FEATURE_COLUMNS),
    )
    state_dict = torch.load(
        os.path.join(model_dir, "model.pt"), map_location="cpu", weights_only=True
    )
    model.load_state_dict(state_dict)
    model.eval()
    return model


def input_fn(request_body: bytes, content_type: str) -> dict:
    """Parse the request body into the payload shape `predict_from_payload` expects.

    Raises:
        ValueError: If `content_type` is not `application/json`.
    """
    if content_type != _CONTENT_TYPE_JSON:
        raise ValueError(f"Unsupported content type: {content_type}")
    return json.loads(request_body)


def predict_fn(input_data: dict, model: FusionModel) -> dict:
    """Run inference using the already-loaded model.

    Echoes `ticker` from the input alongside the prediction so the Batch
    Transform output file is self-contained (no output-line-to-input-line
    position matching needed downstream).
    """
    prediction = predict_from_payload(model, input_data)
    return {"ticker": input_data["ticker"], **prediction}


def output_fn(prediction: dict, accept: str) -> bytes:
    """Serialize the prediction to JSON bytes.

    Raises:
        ValueError: If `accept` is not `application/json`.
    """
    if accept != _CONTENT_TYPE_JSON:
        raise ValueError(f"Unsupported accept type: {accept}")
    return json.dumps(prediction).encode("utf-8")
