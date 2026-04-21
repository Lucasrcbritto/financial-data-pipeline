import numpy as np
import pandas as pd
import pytest

from financial_data_pipeline.pipelines.feature_engineering.nodes import (
    engineer_features,
)


@pytest.fixture
def sample_market_data():
    dates = pd.date_range("2023-01-02", periods=30, freq="B")
    tickers = ["AAPL", "MSFT"]
    rows = []
    for ticker in tickers:
        close = 100.0
        for date in dates:
            close = close * (1 + np.random.uniform(-0.02, 0.02))
            rows.append({
                "date": date,
                "ticker": ticker,
                "open": close * 0.99,
                "high": close * 1.01,
                "low": close * 0.98,
                "close": close,
                "volume": 1_000_000,
            })
    return pd.DataFrame(rows)


class TestEngineerFeatures:
    def test_does_not_mutate_input(self, sample_market_data):
        original_cols = list(sample_market_data.columns)
        engineer_features(sample_market_data, [7], 252)
        assert list(sample_market_data.columns) == original_cols

    def test_adds_daily_return_column(self, sample_market_data):
        result = engineer_features(sample_market_data, [7], 252)
        assert "daily_return" in result.columns

    def test_adds_log_return_column(self, sample_market_data):
        result = engineer_features(sample_market_data, [7], 252)
        assert "log_return" in result.columns

    def test_adds_rolling_vol_for_each_window(self, sample_market_data):
        result = engineer_features(sample_market_data, [7, 21], 252)
        assert "rolling_vol_7d" in result.columns
        assert "rolling_vol_21d" in result.columns

    def test_adds_sma_for_each_window(self, sample_market_data):
        result = engineer_features(sample_market_data, [7, 21], 252)
        assert "sma_7d" in result.columns
        assert "sma_21d" in result.columns

    def test_adds_rsi_column(self, sample_market_data):
        result = engineer_features(sample_market_data, [7], 252)
        assert "rsi_14" in result.columns

    def test_daily_return_computed_per_ticker(self, sample_market_data):
        result = engineer_features(sample_market_data, [7], 252)
        first_rows = (
            result.sort_values(["ticker", "date"])
            .drop_duplicates(subset=["ticker"], keep="first")
        )
        assert first_rows["daily_return"].isna().all()

    def test_output_row_count_matches_input(self, sample_market_data):
        result = engineer_features(sample_market_data, [7], 252)
        assert len(result) == len(sample_market_data)
