"""SageMaker inference container entrypoint (sagemaker-inference-toolkit convention).

Bundled into model.tar.gz's `code/` directory by `package_model.py` at
packaging time. `SAGEMAKER_PROGRAM=serve.py` on the SageMaker Model resource
(set by src/dagster/nlp_sentiment_job.py, mirroring src/ml/serve.py's own
convention) tells the toolkit baked into the DLC image to import this module
and call the four functions below at request time.
"""

import json

try:
    # Package-relative import: used when pytest imports this module as
    # `src.nlp.serve` from the repo root, where the `src` package resolves.
    from src.nlp.config import MAX_TOKEN_LENGTH, MODEL_ID
    from src.nlp.sentiment_model import probs_to_class_scores, scores_to_sentiment
except ImportError:
    # Sibling import: SageMaker copies the bundled `code/` directory's
    # contents flat, so config.py/sentiment_model.py are plain siblings.
    from config import MAX_TOKEN_LENGTH, MODEL_ID
    from sentiment_model import probs_to_class_scores, scores_to_sentiment

_CONTENT_TYPE_JSON = "application/json"


def model_fn(model_dir: str) -> tuple:
    """Load `(model, tokenizer)` from the bundled artifact directory."""
    import torch
    from transformers import AutoModelForSequenceClassification, AutoTokenizer

    tokenizer = AutoTokenizer.from_pretrained(model_dir)
    model = AutoModelForSequenceClassification.from_pretrained(model_dir)
    device = "cuda" if torch.cuda.is_available() else "cpu"
    model.to(device)
    model.eval()
    return model, tokenizer


def input_fn(request_body: bytes, content_type: str) -> dict:
    """Parse the request body into `{"article_id": str, "text": str}`.

    Raises:
        ValueError: If `content_type` is not `application/json`.
    """
    if content_type != _CONTENT_TYPE_JSON:
        raise ValueError(f"Unsupported content type: {content_type}")
    return json.loads(request_body)


def predict_fn(input_data: dict, bundle: tuple) -> dict:
    """Run sentiment classification and return the scored payload."""
    import torch

    model, tokenizer = bundle
    device = next(model.parameters()).device
    inputs = tokenizer(
        input_data["text"],
        truncation=True,
        max_length=MAX_TOKEN_LENGTH,
        return_tensors="pt",
    ).to(device)
    with torch.no_grad():
        logits = model(**inputs).logits
    probs = torch.softmax(logits, dim=-1).squeeze(0).tolist()
    scores = probs_to_class_scores(probs, model.config.id2label)
    sentiment_score, sentiment_label = scores_to_sentiment(scores)
    return {
        "article_id": input_data["article_id"],
        "sentiment_score": sentiment_score,
        "sentiment_label": sentiment_label,
        "model_version": MODEL_ID,
    }


def output_fn(prediction: dict, accept: str) -> bytes:
    """Serialize the prediction to JSON bytes.

    Raises:
        ValueError: If `accept` is not `application/json`.
    """
    if accept != _CONTENT_TYPE_JSON:
        raise ValueError(f"Unsupported accept type: {accept}")
    return json.dumps(prediction).encode("utf-8")
