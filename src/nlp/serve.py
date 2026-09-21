"""SageMaker inference entrypoint (model_fn, input_fn, predict_fn, output_fn)."""

import json

import torch
from transformers import AutoModelForSequenceClassification, AutoTokenizer

try:
    from src.nlp.config import MAX_TOKEN_LENGTH, MODEL_ID
    from src.nlp.sentiment_model import (
        load_model_metadata,
        probs_to_class_scores,
        scores_to_sentiment,
    )
except ImportError:  # SageMaker copies code/ flat
    from config import MAX_TOKEN_LENGTH, MODEL_ID
    from sentiment_model import (
        load_model_metadata,
        probs_to_class_scores,
        scores_to_sentiment,
    )

_CONTENT_TYPE_JSON = "application/json"


def model_fn(model_dir: str) -> tuple:
    """Load (model, tokenizer, model_version); rejects a stale schema version."""
    model_version = load_model_metadata(model_dir).get("model_version", MODEL_ID)
    tokenizer = AutoTokenizer.from_pretrained(model_dir)
    model = AutoModelForSequenceClassification.from_pretrained(model_dir)
    model.to("cuda" if torch.cuda.is_available() else "cpu")
    model.eval()
    return model, tokenizer, model_version


def input_fn(request_body: bytes, content_type: str) -> dict:
    """Parse a JSON body into {"article_id", "text"}."""
    if content_type != _CONTENT_TYPE_JSON:
        raise ValueError(f"Unsupported content type: {content_type}")
    return json.loads(request_body)


def predict_fn(input_data: dict, bundle: tuple) -> dict:
    """Score one article."""
    model, tokenizer, model_version = bundle
    inputs = tokenizer(
        input_data["text"],
        truncation=True,
        max_length=MAX_TOKEN_LENGTH,
        return_tensors="pt",
    ).to(next(model.parameters()).device)
    with torch.no_grad():
        logits = model(**inputs).logits
    probs = torch.softmax(logits, dim=-1).squeeze(0).tolist()
    scores = probs_to_class_scores(probs, model.config.id2label)
    sentiment_score, sentiment_label = scores_to_sentiment(scores)
    return {
        "article_id": input_data["article_id"],
        "sentiment_score": sentiment_score,
        "sentiment_label": sentiment_label,
        "model_version": model_version,
    }


def output_fn(prediction: dict, accept: str) -> bytes:
    """Serialize the prediction as JSON bytes."""
    if accept != _CONTENT_TYPE_JSON:
        raise ValueError(f"Unsupported accept type: {accept}")
    return json.dumps(prediction).encode("utf-8")
