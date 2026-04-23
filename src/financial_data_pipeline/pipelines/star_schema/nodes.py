import logging

import pandas as pd

logger = logging.getLogger(__name__)

MACRO_COLUMNS = ["FEDFUNDS", "UNRATE", "CPIAUCSL", "T10Y2Y"]


def build_dim_ticker(feat_market_data):
    """Build ticker dimension — one row per unique ticker."""
    tickers = (
        feat_market_data[["ticker"]]
        .drop_duplicates()
        .sort_values("ticker")
        .reset_index(drop=True)
    )
    tickers["ticker_key"] = range(1, len(tickers) + 1)
    tickers["exchange"] = "NASDAQ/NYSE"
    return tickers[["ticker_key", "ticker", "exchange"]]


def build_dim_date(feat_market_data):
    """Build date dimension with calendar attributes."""
    df = feat_market_data[["date"]].drop_duplicates().copy()
    df["date"] = pd.to_datetime(df["date"])
    df["date_key"] = df["date"].dt.strftime("%Y%m%d").astype(int)
    df["year"] = df["date"].dt.year
    df["quarter"] = df["date"].dt.quarter
    df["month"] = df["date"].dt.month
    df["day_of_week"] = df["date"].dt.dayofweek
    df["is_weekend"] = df["day_of_week"] >= 5
    return df.sort_values("date").reset_index(drop=True)


def build_dim_macro(feat_market_data):
    """Build macro dimension — one row per date with macro columns."""
    present = [c for c in MACRO_COLUMNS if c in feat_market_data.columns]
    df = feat_market_data[["date"] + present].drop_duplicates(subset=["date"])
    df["date"] = pd.to_datetime(df["date"])
    return df.sort_values("date").reset_index(drop=True)


def build_fact_prices(feat_market_data):
    """Build fact table — one row per date per ticker."""
    df = feat_market_data.copy()
    df["date"] = pd.to_datetime(df["date"])
    df["date_key"] = df["date"].dt.strftime("%Y%m%d").astype(int)
    macro_cols = [c for c in MACRO_COLUMNS if c in df.columns]
    df = df.drop(columns=macro_cols)
    return df.sort_values(["ticker", "date"]).reset_index(drop=True)
