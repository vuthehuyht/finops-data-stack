"""Constants for the news sentiment pipeline."""

# General-domain Vietnamese PhoBERT checkpoint (MIT); expect a domain-mismatch hit.
MODEL_ID = "wonrax/phobert-base-vietnamese-sentiment"

MAX_TOKEN_LENGTH = 256

# Bump when MODEL_ID or the label set changes; stale artifacts are then rejected.
SENTIMENT_SCHEMA_VERSION = 1
