import pandas as pd
import pytest


@pytest.fixture
def sample_raw_stock_prices():
    return pd.DataFrame({
        "date": pd.to_datetime([
            "2023-01-02", "2023-01-03", "2023-01-04",
            "2023-01-02", "2023-01-03", "2023-01-04",
        ]),
        "ticker": ["AAPL", "AAPL", "AAPL", "MSFT", "MSFT", "MSFT"],
        "open":   [130.0, 131.0, 132.0, 240.0, 241.0, 242.0],
        "high":   [132.0, 133.0, 134.0, 243.0, 244.0, 245.0],
        "low":    [129.0, 130.0, 131.0, 238.0, 239.0, 240.0],
        "close":  [131.0, 132.0, None,  241.0, 242.0, 243.0],
        "volume": [1_000_000, 1_100_000, 900_000, 500_000, 600_000, 550_000],
    })


@pytest.fixture
def sample_raw_macro_indicators():
    return pd.DataFrame({
        "date": pd.to_datetime([
            "2023-01-01", "2023-02-01",
            "2023-01-01", "2023-02-01",
        ]),
        "series_id": ["FEDFUNDS", "FEDFUNDS", "UNRATE", "UNRATE"],
        "value": [4.33, 4.57, 3.4, 3.6],
    })
