"""Configuration constants for the ML training pipeline.

Column lists are explicit (not inferred from the FACT_ML_FEATURE_SET schema)
so that new columns added to the dbt model don't silently change the model's
input shape.
"""

WINDOW_SIZE = 30

MODEL_NAME = "finops-multimodal-regressor"

TARGET_COLUMN = "label_next_5d_return"

# Daily-changing signals fed into the LSTM branch as a WINDOW_SIZE-day sequence.
SEQUENCE_FEATURE_COLUMNS = ["close", "volume", "sma_20", "sma_50", "rsi_14", "macd"]

# Slow-changing, quarter-driven snapshot fed into the MLP branch.
TABULAR_FEATURE_COLUMNS = [
    "market_cap",
    "pe_ratio",
    "pb_ratio",
    "roe",
    "roa",
    "price_momentum_1m",
    "price_momentum_3m",
    "volatility_30d",
    "relative_strength_vs_vnindex",
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
DROPOUT_RATE = 0.2
