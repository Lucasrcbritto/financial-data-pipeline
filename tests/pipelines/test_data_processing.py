import pytest

from financial_data_pipeline.pipelines.data_processing.nodes import (
    clean_macro_indicators,
    clean_stock_prices,
)

REQUIRED_COLS = ["date", "ticker", "open", "high", "low", "close", "volume"]


class TestCleanStockPrices:
    def test_drops_rows_with_null_close(self, sample_raw_stock_prices):
        result = clean_stock_prices(sample_raw_stock_prices, 0.7, REQUIRED_COLS)
        assert result["close"].isna().sum() == 0

    def test_does_not_mutate_input(self, sample_raw_stock_prices):
        original_nulls = sample_raw_stock_prices["close"].isna().sum()
        clean_stock_prices(sample_raw_stock_prices, 0.7, REQUIRED_COLS)
        assert sample_raw_stock_prices["close"].isna().sum() == original_nulls

    def test_ticker_is_uppercase(self, sample_raw_stock_prices):
        df = sample_raw_stock_prices.copy()
        df["ticker"] = df["ticker"].str.lower()
        result = clean_stock_prices(df, 0.7, REQUIRED_COLS)
        assert result["ticker"].str.isupper().all()

    def test_deduplicates(self, sample_raw_stock_prices):
        import pandas as pd
        dupe = sample_raw_stock_prices.iloc[[0]].copy()
        df = pd.concat([sample_raw_stock_prices, dupe], ignore_index=True)
        result = clean_stock_prices(df, 0.7, REQUIRED_COLS)
        assert result.duplicated(subset=["date", "ticker"]).sum() == 0

    def test_raises_on_missing_columns(self):
        import pandas as pd
        df = pd.DataFrame({"date": ["2023-01-02"], "ticker": ["AAPL"]})
        with pytest.raises(ValueError, match="Required columns missing"):
            clean_stock_prices(df, 0.0, REQUIRED_COLS)


class TestCleanMacroIndicators:
    def test_does_not_mutate_input(self, sample_raw_macro_indicators):
        original_len = len(sample_raw_macro_indicators)
        clean_macro_indicators(sample_raw_macro_indicators)
        assert len(sample_raw_macro_indicators) == original_len

    def test_series_id_is_uppercase(self, sample_raw_macro_indicators):
        df = sample_raw_macro_indicators.copy()
        df["series_id"] = df["series_id"].str.lower()
        result = clean_macro_indicators(df)
        assert result["series_id"].str.isupper().all()

    def test_deduplicates(self, sample_raw_macro_indicators):
        import pandas as pd
        dupe = sample_raw_macro_indicators.iloc[[0]].copy()
        df = pd.concat([sample_raw_macro_indicators, dupe], ignore_index=True)
        result = clean_macro_indicators(df)
        assert result.duplicated(subset=["date", "series_id"]).sum() == 0
