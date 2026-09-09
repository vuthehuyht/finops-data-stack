"""Configuration constants for the ML training pipeline.

Column lists are explicit (not inferred from the FACT_ML_FEATURE_SET schema)
so that new columns added to the dbt model don't silently change the model's
input shape.
"""

WINDOW_SIZE = 30

MODEL_NAME = "finops-multimodal-regressor"

TARGET_COLUMN = "label_next_5d_return"

# Daily-changing signals fed into the LSTM branch as a WINDOW_SIZE-day sequence.
SEQUENCE_FEATURE_COLUMNS = [
    "moving_average_20d",
    "moving_average_50d",
    "price_momentum_1m",
    "price_momentum_3m",
    "volatility_30d",
    "relative_strength_vs_vnindex",
]

# Slow-changing, quarter-driven snapshot fed into the MLP branch.
TABULAR_FEATURE_COLUMNS = [
    "pe_ratio",
    "pb_ratio",
    "roe",
    "roa",
    "revenue_growth_yoy",
    "net_profit_growth_yoy",
    "gross_margin",
    "debt_to_equity",
    "operating_cash_flow_to_net_income",
    "foreign_buy_ratio_10d",
    "net_foreign_flow_momentum_1m",
    "prop_trading_net_val_5d",
    "prop_vs_foreign_correlation_10d",
]

LSTM_HIDDEN_SIZE = 64
LSTM_NUM_LAYERS = 1
MLP_HIDDEN_SIZES = (32, 16)
FUSION_HIDDEN_SIZE = 32
DROPOUT_RATE = 0.4

# ── Sector-aware feature handling ────────────────────────────────────────
# Order is the nn.Embedding index: APPEND-ONLY, never reorder.
SECTOR_VOCAB = ["bank", "securities", "insurance", "real_estate", "non_financial"]
SECTOR_EMBEDDING_DIM = 4

# The tabular model input is [values ++ applicability_flags].
TABULAR_VECTOR_SIZE = 2 * len(TABULAR_FEATURE_COLUMNS)

# feature -> sectors it is defined for. A feature absent from this dict
# applies to EVERY sector. gross_margin / debt_to_equity are structurally
# undefined for banks, securities and insurance (different statement shape).
FEATURE_APPLICABILITY: dict[str, set[str]] = {
    "gross_margin": {"real_estate", "non_financial"},
    "debt_to_equity": {"real_estate", "non_financial"},
}

# Bumped whenever the feature vector layout or semantics change so a stale
# champion artifact is rejected loudly instead of producing garbage.
FEATURE_SCHEMA_VERSION = 2
