import logging

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


def engineer_features(prm_market_data, rolling_windows, annualization_factor):
    """Add technical and statistical features to market data.

    Args:
        prm_market_data: Joined market data from primary layer
        rolling_windows: List of window sizes for rolling calculations
        annualization_factor: Multiplier to annualize volatility (252 for daily)

    Returns:
        DataFrame with original columns plus engineered features
    """
    df = prm_market_data.copy()
    df = df.sort_values(["ticker", "date"]).reset_index(drop=True)

    result_frames = []
    for ticker, group in df.groupby("ticker", sort=False):
        group = group.copy().sort_values("date").reset_index(drop=True)

        group["daily_return"] = group["close"].pct_change()
        group["log_return"] = np.log(group["close"] / group["close"].shift(1))

        for w in rolling_windows:
            group[f"rolling_vol_{w}d"] = (
                group["daily_return"]
                .rolling(window=w, min_periods=2)
                .std()
                * np.sqrt(annualization_factor)
            )
            group[f"sma_{w}d"] = (
                group["close"].rolling(window=w, min_periods=2).mean()
            )

        group["rsi_14"] = _compute_rsi(group["close"])
        result_frames.append(group)

    result = pd.concat(result_frames, ignore_index=True)
    result = result.sort_values(["ticker", "date"]).reset_index(drop=True)
    logger.info(
        "Feature engineering complete: %d rows, %d columns",
        len(result),
        len(result.columns),
    )
    return result


def _compute_rsi(series: pd.Series, window: int = 14) -> pd.Series:
    """Compute the Relative Strength Index for a price series.

    Args:
        series: Closing price series (already sorted by date, single ticker)
        window: Lookback window (default 14)

    Returns:
        RSI series aligned to the input index
    """
    delta = series.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)

    avg_gain = gain.rolling(window=window, min_periods=window).mean()
    avg_loss = loss.rolling(window=window, min_periods=window).mean()

    # Avoid division by zero when avg_loss is 0 (all gains, RSI = 100)
    rs = avg_gain / avg_loss.replace(0, float("nan"))
    rsi = 100 - (100 / (1 + rs))
    return rsi
