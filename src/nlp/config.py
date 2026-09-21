"""Configuration constants for the news sentiment pipeline."""

# General-domain Vietnamese sentiment PhoBERT checkpoint (MIT). Not trained on
# financial news, so expect a domain-mismatch accuracy hit (see spec, Phase C).
MODEL_ID = "wonrax/phobert-base-vietnamese-sentiment"

MAX_TOKEN_LENGTH = 256

# Bumped whenever MODEL_ID changes or the label set changes, so a stale
# champion artifact from Phase C can be rejected loudly instead of silently
# producing garbage — mirrors src/ml/config.py's FEATURE_SCHEMA_VERSION.
SENTIMENT_SCHEMA_VERSION = 1
