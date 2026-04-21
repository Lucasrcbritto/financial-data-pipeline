import logging

import pandas as pd

logger = logging.getLogger(__name__)


def clean_stock_prices(raw_stock_prices, null_threshold, required_stock_columns):
    """Clean and validate raw stock price data.

    Args:
        raw_stock_prices: Raw DataFrame from ingestion layer
        null_threshold: Minimum ratio of non-null values to keep a column
        required_stock_columns: Column names that must be present

    Returns:
        Cleaned DataFrame with correct types, no nulls, no duplicates
    """
    df = raw_stock_prices.copy()

    df["date"] = pd.to_datetime(df["date"])
    df["ticker"] = df["ticker"].astype(str).str.upper().str.strip()

    missing = set(required_stock_columns) - set(df.columns)
    if missing:
        raise ValueError(f"Required columns missing: {missing}")

    for col in ["open", "high", "low", "close"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    null_ratios = df.notna().mean()
    cols_to_keep = null_ratios[null_ratios >= null_threshold].index.tolist()
    df = df[cols_to_keep]

    missing = set(required_stock_columns) - set(df.columns)
    if missing:
        raise ValueError(f"Required columns missing: {missing}")

    df = df.dropna(subset=["open", "high", "low", "close"])
    df = df.drop_duplicates(subset=["date", "ticker"])
    df = df.sort_values(["ticker", "date"]).reset_index(drop=True)
    logger.info("Cleaned stock prices: %d rows", len(df))
    return df


def clean_macro_indicators(raw_macro_indicators):
    """Clean and validate raw macro indicator data.

    Args:
        raw_macro_indicators: Raw DataFrame from ingestion layer

    Returns:
        Cleaned DataFrame with columns: [date, series_id, value]
    """
    df = raw_macro_indicators.copy()

    df["date"] = pd.to_datetime(df["date"])
    df["series_id"] = df["series_id"].astype(str).str.upper().str.strip()
    df["value"] = pd.to_numeric(df["value"], errors="coerce")

    df = df.drop_duplicates(subset=["date", "series_id"])

    df = df.sort_values(["series_id", "date"]).reset_index(drop=True)
    df["value"] = df.groupby("series_id")["value"].ffill()

    df = df.dropna(subset=["value"])
    logger.info("Cleaned macro indicators: %d rows", len(df))
    return df


def join_market_data(int_stock_prices, int_macro_indicators):
    """Join stock prices with macro indicators using merge_asof.

    Args:
        int_stock_prices: Cleaned stock price DataFrame
        int_macro_indicators: Cleaned macro indicators DataFrame (long format)

    Returns:
        Wide DataFrame with macro columns merged onto each stock row
    """
    df = int_stock_prices.copy()
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date").reset_index(drop=True)

    macro = int_macro_indicators.copy()
    macro["date"] = pd.to_datetime(macro["date"])

    macro_wide = macro.pivot_table(
        index="date", columns="series_id", values="value", aggfunc="last"
    ).reset_index()
    macro_wide.columns.name = None
    macro_wide = macro_wide.sort_values("date").reset_index(drop=True)

    result = pd.merge_asof(df, macro_wide, on="date", direction="backward")
    result = result.sort_values(["ticker", "date"]).reset_index(drop=True)
    logger.info(
        "Joined market data: %d rows, %d columns", len(result), len(result.columns)
    )
    return result
