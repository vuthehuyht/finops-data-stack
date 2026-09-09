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
    from src.ml.config import (
        FEATURE_SCHEMA_VERSION,
        SEQUENCE_FEATURE_COLUMNS,
        TABULAR_VECTOR_SIZE,
    )
    from src.ml.inference import predict_from_payload
    from src.ml.model import FusionModel
except ImportError:
    # Sibling import: SageMaker copies the bundled `code/` directory's
    # contents flat, so config.py/inference.py/model.py are plain siblings
    # of serve.py in that directory.
    from config import (
        FEATURE_SCHEMA_VERSION,
        SEQUENCE_FEATURE_COLUMNS,
        TABULAR_VECTOR_SIZE,
    )
    from inference import predict_from_payload
    from model import FusionModel

_CONTENT_TYPE_JSON = "application/json"


def model_fn(model_dir: str) -> tuple:
    """Load `(model, medians, sector_vocab)` and verify the artifact schema.

    Raises:
        ValueError: If `metadata.json`'s `feature_schema_version` does not
            match this code's `FEATURE_SCHEMA_VERSION` — a stale champion
            artifact must be retrained before it can be served.
    """
    with open(os.path.join(model_dir, "metadata.json"), encoding="utf-8") as f:
        metadata = json.load(f)

    schema = metadata.get("feature_schema_version")
    if schema != FEATURE_SCHEMA_VERSION:
        raise ValueError(
            f"Champion artifact feature_schema_version={schema} does not match "
            f"code FEATURE_SCHEMA_VERSION={FEATURE_SCHEMA_VERSION}. Retrain "
            "before serving."
        )

    sector_vocab = metadata["sector_vocab"]
    medians = metadata.get("tabular_medians", {})
    model = FusionModel(
        sequence_input_size=len(SEQUENCE_FEATURE_COLUMNS),
        tabular_input_size=TABULAR_VECTOR_SIZE,
        num_sectors=len(sector_vocab),
    )
    # Load onto CPU first (works with or without a GPU), then move the model
    # to CUDA when the serving container has one — the GPU inference image
    # (_INFERENCE_IMAGE) runs on ml.g4dn.xlarge.
    state_dict = torch.load(
        os.path.join(model_dir, "model.pt"), map_location="cpu", weights_only=True
    )
    model.load_state_dict(state_dict)
    device = "cuda" if torch.cuda.is_available() else "cpu"
    model.to(device)
    model.eval()
    return model, medians, sector_vocab


def input_fn(request_body: bytes, content_type: str) -> dict:
    """Parse the request body into the payload `predict_from_payload` expects.

    Raises:
        ValueError: If `content_type` is not `application/json`.
    """
    if content_type != _CONTENT_TYPE_JSON:
        raise ValueError(f"Unsupported content type: {content_type}")
    return json.loads(request_body)


def predict_fn(input_data: dict, bundle: tuple) -> dict:
    """Run inference and echo `ticker` alongside the prediction.

    `predicted_return` is `None` (serialized as JSON `null`) when the model
    output is non-finite, so the Batch Transform output never contains the
    literal token `NaN`.
    """
    prediction = predict_from_payload(bundle, input_data)
    return {"ticker": input_data["ticker"], **prediction}


def output_fn(prediction: dict, accept: str) -> bytes:
    """Serialize the prediction to JSON bytes.

    Raises:
        ValueError: If `accept` is not `application/json`.
    """
    if accept != _CONTENT_TYPE_JSON:
        raise ValueError(f"Unsupported accept type: {accept}")
    return json.dumps(prediction).encode("utf-8")
