import glob
import logging
from pathlib import Path

import numpy as np
import pandas as pd

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class FinOpsDatasetBuilder:
    def __init__(
        self,
        raw_dir: str = "data/raw",
        output_dir: str = "data/processed",
        seq_length: int = 30,
    ):
        self.raw_dir = Path(raw_dir)
        self.output_dir = Path(output_dir)
        self.seq_length = seq_length
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def _load_csvs(self, folder_name: str) -> pd.DataFrame:
        path_pattern = self.raw_dir / folder_name / "*.csv"
        files = glob.glob(str(path_pattern))
        if not files:
            return pd.DataFrame()
        dfs = [pd.read_csv(f) for f in files]
        return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()

    def _get_report_date(self, row):
        y = int(row["year"])
        q = str(row["period"]).upper()
        if "1" in q:
            return pd.Timestamp(year=y, month=5, day=1)
        elif "2" in q:
            return pd.Timestamp(year=y, month=8, day=1)
        elif "3" in q:
            return pd.Timestamp(year=y, month=11, day=1)
        elif "4" in q:
            return pd.Timestamp(year=y + 1, month=2, day=1)
        return pd.Timestamp(year=y + 1, month=4, day=1)

    def _safe_divide(self, a, b):
        return np.where(b != 0, a / b, np.nan)

    def _process_fundamentals(self):
        logger.info("Processing Fundamentals (Income, Balance, Cashflow)...")
        df_income = self._load_csvs("income_statement")
        df_balance = self._load_csvs("balance_sheet")
        df_cashflow = self._load_csvs("cashflow_statement")

        if df_income.empty or df_balance.empty or df_cashflow.empty:
            return pd.DataFrame()

        df_income["report_date"] = df_income.apply(self._get_report_date, axis=1)
        df_balance["report_date"] = df_balance.apply(self._get_report_date, axis=1)
        df_cashflow["report_date"] = df_cashflow.apply(self._get_report_date, axis=1)

        # Merge on ticker, year, period
        df_fund = pd.merge(
            df_income,
            df_balance,
            on=["ticker", "year", "period", "report_date"],
            how="outer",
        )
        df_fund = pd.merge(
            df_fund,
            df_cashflow,
            on=["ticker", "year", "period", "report_date"],
            how="outer",
        )

        df_fund = df_fund.sort_values(["ticker", "year", "period"]).reset_index(
            drop=True
        )

        # Precompute YoY metrics inside the quarterly series per ticker
        def compute_yoy(group):
            # Shift 4 for YoY since it's quarterly
            group["revenue_growth_yoy"] = (
                group["revenue"] / group["revenue"].shift(4) - 1
            )
            group["net_profit_growth_yoy"] = (
                group["net_profit_after_tax"] / group["net_profit_after_tax"].shift(4)
                - 1
            )
            group["asset_growth_yoy"] = (
                group["total_assets"] / group["total_assets"].shift(4) - 1
            )

            # Margins
            group["gross_margin"] = self._safe_divide(
                group["gross_profit"], group["revenue"]
            )
            group["net_margin"] = self._safe_divide(
                group["net_profit_after_tax"], group["revenue"]
            )
            group["operating_margin"] = self._safe_divide(
                group["operating_profit"], group["revenue"]
            )

            # Ratios
            group["roe"] = self._safe_divide(
                group["net_profit_after_tax"] * 4, group["equity"]
            )  # Annualized proxy
            group["roa"] = self._safe_divide(
                group["net_profit_after_tax"] * 4, group["total_assets"]
            )
            total_debt = group["short_term_debt"].fillna(0) + group[
                "long_term_debt"
            ].fillna(0)
            group["debt_to_equity"] = self._safe_divide(total_debt, group["equity"])

            # Current Ratio proxy (using short_term_debt if current_liabilities missing)
            group["current_ratio"] = self._safe_divide(
                group["current_assets"], group["short_term_debt"].replace(0, np.nan)
            )
            group["cash_to_assets"] = self._safe_divide(
                group["cash"], group["total_assets"]
            )
            group["equity_multiplier"] = self._safe_divide(
                group["total_assets"], group["equity"]
            )

            # Cashflow
            group["operating_cash_flow_to_net_income"] = self._safe_divide(
                group["cfo"], group["net_profit_after_tax"]
            )
            group["free_cash_flow"] = group["cfo"] - group["capex"].fillna(0)
            group["cash_flow_to_debt"] = self._safe_divide(group["cfo"], total_debt)

            return group

        df_fund = df_fund.groupby("ticker", group_keys=False).apply(compute_yoy)
        df_fund = df_fund.sort_values("report_date")
        return df_fund

    def _process_prices(self):
        logger.info("Processing Prices & Index...")
        df_price = self._load_csvs("stock_price_eod")
        df_index = self._load_csvs("index_price_eod")

        df_price["trading_date"] = pd.to_datetime(df_price["trading_date"])
        df_price = df_price.sort_values(["ticker", "trading_date"])

        # Compute label
        df_price["label_next_5d_return"] = df_price.groupby("ticker")[
            "close"
        ].transform(lambda x: (x.shift(-5) / x) - 1)

        # Price Momentums & MAs
        def compute_price_features(g):
            g["price_momentum_1m"] = g["close"] / g["close"].shift(20) - 1
            g["price_momentum_3m"] = g["close"] / g["close"].shift(60) - 1
            g["volatility_30d"] = g["close"].pct_change().rolling(30).std()
            g["moving_average_20d"] = g["close"].rolling(20).mean()
            g["moving_average_50d"] = g["close"].rolling(50).mean()
            g["moving_average_200d"] = g["close"].rolling(200).mean()

            # Additional Indicators
            g["sma_20"] = g["close"].rolling(20).mean()
            g["sma_50"] = g["close"].rolling(50).mean()

            delta = g["close"].diff()
            gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
            loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
            rs = gain / loss
            g["rsi_14"] = 100 - (100 / (1 + rs))

            ema_12 = g["close"].ewm(span=12, adjust=False).mean()
            ema_26 = g["close"].ewm(span=26, adjust=False).mean()
            g["macd"] = ema_12 - ema_26

            return g

        df_price = df_price.groupby("ticker", group_keys=False).apply(
            compute_price_features
        )

        # Index relative strength
        if not df_index.empty:
            df_index["trading_date"] = pd.to_datetime(df_index["trading_date"])
            df_index = df_index[df_index["index_name"] == "VNINDEX"].sort_values(
                "trading_date"
            )
            df_index["vnindex_return_1m"] = (
                df_index["close"] / df_index["close"].shift(20) - 1
            )

            df_price = pd.merge(
                df_price,
                df_index[["trading_date", "vnindex_return_1m"]],
                on="trading_date",
                how="left",
            )
            df_price["relative_strength_vs_vnindex"] = (
                df_price["price_momentum_1m"] - df_price["vnindex_return_1m"]
            )
        else:
            df_price["relative_strength_vs_vnindex"] = np.nan

        return df_price

    def _process_flows(self, df_price):
        logger.info("Processing Trading Flows (Foreign, Proprietary)...")
        df_foreign = self._load_csvs("foreign_trading")
        df_prop = self._load_csvs("proprietary_trading")

        if not df_foreign.empty:
            df_foreign["trading_date"] = pd.to_datetime(df_foreign["trading_date"])
            df_foreign["net_foreign_val"] = df_foreign["buyForeignValue"].fillna(
                0
            ) - df_foreign["sellForeignValue"].fillna(0)
            df_price = pd.merge(
                df_price,
                df_foreign[
                    ["ticker", "trading_date", "buyForeignValue", "net_foreign_val"]
                ],
                on=["ticker", "trading_date"],
                how="left",
            )

            def compute_foreign(g):
                g["total_trading_val"] = g["close"] * g["volume"]
                g["foreign_buy_ratio_10d"] = self._safe_divide(
                    g["buyForeignValue"].rolling(10).sum(),
                    g["total_trading_val"].rolling(10).sum(),
                )
                g["net_foreign_flow_momentum_1m"] = (
                    g["net_foreign_val"].rolling(20).mean()
                )
                return g

            df_price = df_price.groupby("ticker", group_keys=False).apply(
                compute_foreign
            )
        else:
            df_price["foreign_buy_ratio_10d"] = np.nan
            df_price["net_foreign_flow_momentum_1m"] = np.nan

        if not df_prop.empty:
            df_prop["trading_date"] = pd.to_datetime(df_prop["trading_date"])
            df_prop = df_prop.rename(columns={"net_val": "prop_net_val"})
            df_price = pd.merge(
                df_price,
                df_prop[["ticker", "trading_date", "prop_net_val"]],
                on=["ticker", "trading_date"],
                how="left",
            )

            def compute_prop(g):
                g["prop_trading_net_val_5d"] = g["prop_net_val"].rolling(5).sum()
                if "net_foreign_val" in g.columns:
                    g["prop_vs_foreign_correlation_10d"] = (
                        g["prop_net_val"].rolling(10).corr(g["net_foreign_val"])
                    )
                else:
                    g["prop_vs_foreign_correlation_10d"] = np.nan
                return g

            df_price = df_price.groupby("ticker", group_keys=False).apply(compute_prop)
        else:
            df_price["prop_trading_net_val_5d"] = np.nan
            df_price["prop_vs_foreign_correlation_10d"] = np.nan

        return df_price

    def _merge_fundamentals_asof(
        self, df_price: pd.DataFrame, df_fund: pd.DataFrame
    ) -> pd.DataFrame:
        """Merge quarterly fundamentals into daily price rows via merge_asof."""
        cols_to_merge = [
            "report_date",
            "net_profit_after_tax",
            "equity",
            "revenue_growth_yoy",
            "net_profit_growth_yoy",
            "gross_margin",
            "net_margin",
            "operating_margin",
            "roe",
            "roa",
            "asset_growth_yoy",
            "debt_to_equity",
            "current_ratio",
            "cash_to_assets",
            "equity_multiplier",
            "operating_cash_flow_to_net_income",
            "free_cash_flow",
            "cash_flow_to_debt",
        ]
        fallback_cols = [
            "net_profit_after_tax",
            "equity",
            "revenue_growth_yoy",
            "net_profit_growth_yoy",
            "roe",
            "roa",
        ]

        merged_list = []
        for ticker, group in df_price.groupby("ticker"):
            if not df_fund.empty:
                fund_group = df_fund[df_fund["ticker"] == ticker]
                if not fund_group.empty:
                    # Drop duplicated report dates if any
                    fund_group = fund_group.drop_duplicates(
                        subset=["report_date"]
                    ).sort_values("report_date")
                    group = pd.merge_asof(
                        group,
                        fund_group[cols_to_merge],
                        left_on="trading_date",
                        right_on="report_date",
                        direction="backward",
                    )
                else:
                    for c in fallback_cols:
                        group[c] = np.nan
            merged_list.append(group)

        return pd.concat(merged_list, ignore_index=True)

    def _build_training_tensors(
        self, df_price: pd.DataFrame, seq_features: list, tab_features: list
    ) -> tuple:
        """Slide seq_length windows per ticker into (X_seq, X_tab, y, metadata)."""
        X_seq_list, X_tab_list, y_list, metadata_list = [], [], [], []

        for ticker, group in df_price.groupby("ticker"):
            group = group.dropna(subset=seq_features + ["label_next_5d_return"])
            if len(group) < self.seq_length:
                continue

            seq_vals = group[seq_features].values
            tab_vals = group[tab_features].fillna(0).values
            target_vals = group["label_next_5d_return"].values
            date_vals = group["trading_date"].astype(str).values

            for i in range(len(group) - self.seq_length + 1):
                window_seq = seq_vals[i : i + self.seq_length]
                window_target = target_vals[i + self.seq_length - 1]
                window_tab = tab_vals[i + self.seq_length - 1]
                window_date = date_vals[i + self.seq_length - 1]

                X_seq_list.append(window_seq)
                X_tab_list.append(window_tab)
                y_list.append(window_target)
                metadata_list.append([ticker, window_date])

        return (
            np.array(X_seq_list),
            np.array(X_tab_list),
            np.array(y_list),
            np.array(metadata_list),
        )

    def build_dataset(self):
        # 1. Process Data
        df_price = self._process_prices()
        if df_price.empty:
            logger.error("No price data.")
            return

        df_price = self._process_flows(df_price)
        df_fund = self._process_fundamentals()

        # Merge Company Profile
        df_profile = self._load_csvs("company_profile")
        if not df_profile.empty and "outstanding_share" in df_profile.columns:
            df_profile = df_profile[["ticker", "outstanding_share"]].drop_duplicates()
            df_price = df_price.merge(df_profile, on="ticker", how="left")
            df_price["market_cap"] = df_price["close"] * df_price["outstanding_share"]
        else:
            df_price["market_cap"] = np.nan

        # 2. Merge Fundamentals (As-of)
        logger.info("Merging Tabular Features using merge_asof...")
        df_price = df_price.sort_values("trading_date")
        df_price = self._merge_fundamentals_asof(df_price, df_fund)

        # Dynamic Valuations
        df_price["pe_ratio"] = self._safe_divide(
            df_price["market_cap"], df_price.get("net_profit_after_tax", 0) * 4
        )
        df_price["pb_ratio"] = self._safe_divide(
            df_price["market_cap"], df_price.get("equity", 0)
        )

        # 3. Build Tensors
        logger.info("Building 3D and 2D Tensors...")
        seq_features = ["close", "volume", "sma_20", "sma_50", "rsi_14", "macd"]
        tab_features = [
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

        # Ensure columns exist to avoid KeyError
        for col in tab_features:
            if col not in df_price.columns:
                df_price[col] = np.nan

        X_seq, X_tab, y, metadata = self._build_training_tensors(
            df_price, seq_features, tab_features
        )

        logger.info(
            f"Generated Tensors: X_seq: {X_seq.shape}, X_tab: {X_tab.shape}, "
            f"y: {y.shape}"
        )

        # Save full dataframe for existing train.py compatibility
        df_parquet = df_price.copy()
        df_parquet = df_parquet.rename(
            columns={"ticker": "TICKER", "trading_date": "TRADING_DATE"}
        )
        df_parquet.to_parquet(self.output_dir / "features.parquet", index=False)
        logger.info(
            f"Saved full feature dataframe to {self.output_dir / 'features.parquet'} "
            "for SageMaker training!"
        )

        np.save(self.output_dir / "X_seq.npy", X_seq)
        np.save(self.output_dir / "X_tab.npy", X_tab)
        np.save(self.output_dir / "y.npy", y)
        np.save(self.output_dir / "metadata.npy", metadata)
        logger.info("Dataset strictly built according to architecture docs!")


if __name__ == "__main__":
    builder = FinOpsDatasetBuilder()
    builder.build_dataset()
