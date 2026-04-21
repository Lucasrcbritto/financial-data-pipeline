import logging

import pandas as pd
import yfinance as yf

logger = logging.getLogger(__name__)


def ingest_stock_prices(tickers, start_date, end_date):
    """Download OHLCV stock prices from Yahoo Finance.

    Args:
        tickers: List of ticker symbols e.g. ["AAPL", "MSFT"]
        start_date: Start date string in YYYY-MM-DD format
        end_date: End date string in YYYY-MM-DD format

    Returns:
        Long-format DataFrame with columns:
        [date, ticker, open, high, low, close, volume]
    """
    logger.info("Ingesting stock prices for %d tickers", len(tickers))

    raw = yf.download(
        tickers=tickers,
        start=start_date,
        end=end_date,
        auto_adjust=True,
        progress=False,
    )

    raw.columns = raw.columns.swaplevel(0, 1)
    raw = raw.stack(level=0, future_stack=True).reset_index()
    raw.columns = [c.lower() for c in raw.columns]
    raw = raw.rename(columns={"level_1": "ticker"})

    raw = raw[["date", "ticker", "open", "high", "low", "close", "volume"]]
    logger.info("Ingested %d rows", len(raw))
    return raw


def ingest_macro_indicators(fred_series, start_date, end_date):
    """Fetch macro indicator series from FRED.

    Args:
        fred_series: List of FRED series IDs e.g. ["FEDFUNDS", "UNRATE"]
        start_date: Start date string in YYYY-MM-DD format
        end_date: End date string in YYYY-MM-DD format

    Returns:
        Long-format DataFrame with columns: [date, series_id, value]
    """
    logger.info("Ingesting %d FRED series", len(fred_series))

    frames = []
    for series_id in fred_series:
        try:
            import pandas_datareader.data as web
            df = web.DataReader(series_id, "fred", start_date, end_date)
            df = df.reset_index()
            df.columns = ["date", "value"]
            df["series_id"] = series_id
            frames.append(df[["date", "series_id", "value"]])
            logger.info("Fetched %s: %d rows", series_id, len(df))
        except Exception as exc:  # noqa: BLE001
            logger.warning("Failed to fetch %s: %s", series_id, exc)

    result = pd.concat(frames, ignore_index=True)
    logger.info("Macro ingestion complete: %d rows", len(result))
    return result
