import pandas as pd
import pytest

from financial_data_pipeline.pipelines.star_schema.nodes import (
    build_dim_date,
    build_dim_macro,
    build_dim_ticker,
    build_fact_prices,
)

MACRO_COLS = ["FEDFUNDS", "UNRATE", "CPIAUCSL", "T10Y2Y"]


@pytest.fixture
def sample_feat_market_data():
    return pd.DataFrame({
        "date": pd.to_datetime([
            "2023-01-02", "2023-01-03", "2023-01-02", "2023-01-03",
        ]),
        "ticker": ["AAPL", "AAPL", "MSFT", "MSFT"],
        "open":   [130.0, 131.0, 240.0, 241.0],
        "high":   [132.0, 133.0, 243.0, 244.0],
        "low":    [129.0, 130.0, 238.0, 239.0],
        "close":  [131.0, 132.0, 241.0, 242.0],
        "volume": [1_000_000, 1_100_000, 500_000, 600_000],
        "FEDFUNDS": [4.33, 4.33, 4.33, 4.33],
        "UNRATE":   [3.4,  3.4,  3.4,  3.4],
        "CPIAUCSL": [300.0, 300.0, 300.0, 300.0],
        "T10Y2Y":   [0.5,  0.5,  0.5,  0.5],
        "daily_return": [None, 0.008, None, 0.004],
    })


class TestBuildDimTicker:
    def test_one_row_per_ticker(self, sample_feat_market_data):
        result = build_dim_ticker(sample_feat_market_data)
        assert len(result) == 2

    def test_has_ticker_key_column(self, sample_feat_market_data):
        result = build_dim_ticker(sample_feat_market_data)
        assert "ticker_key" in result.columns

    def test_ticker_keys_are_unique(self, sample_feat_market_data):
        result = build_dim_ticker(sample_feat_market_data)
        assert result["ticker_key"].nunique() == len(result)


class TestBuildDimDate:
    def test_one_row_per_date(self, sample_feat_market_data):
        result = build_dim_date(sample_feat_market_data)
        assert len(result) == 2

    def test_has_date_key_column(self, sample_feat_market_data):
        result = build_dim_date(sample_feat_market_data)
        assert "date_key" in result.columns

    def test_date_key_format(self, sample_feat_market_data):
        result = build_dim_date(sample_feat_market_data)
        assert result["date_key"].iloc[0] == 20230102


class TestBuildDimMacro:
    def test_one_row_per_date(self, sample_feat_market_data):
        result = build_dim_macro(sample_feat_market_data)
        assert len(result) == 2

    def test_contains_macro_columns(self, sample_feat_market_data):
        result = build_dim_macro(sample_feat_market_data)
        for col in MACRO_COLS:
            assert col in result.columns


class TestBuildFactPrices:
    def test_macro_columns_removed(self, sample_feat_market_data):
        result = build_fact_prices(sample_feat_market_data)
        for col in MACRO_COLS:
            assert col not in result.columns

    def test_has_date_key(self, sample_feat_market_data):
        result = build_fact_prices(sample_feat_market_data)
        assert "date_key" in result.columns

    def test_row_count_unchanged(self, sample_feat_market_data):
        result = build_fact_prices(sample_feat_market_data)
        assert len(result) == len(sample_feat_market_data)
