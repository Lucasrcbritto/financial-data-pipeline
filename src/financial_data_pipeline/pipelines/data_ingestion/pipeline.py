from kedro.pipeline import Pipeline, node, pipeline

from .nodes import ingest_macro_indicators, ingest_stock_prices


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            node(
                func=ingest_stock_prices,
                inputs={
                    "tickers": "params:ingestion.tickers",
                    "start_date": "params:ingestion.start_date",
                    "end_date": "params:ingestion.end_date",
                },
                outputs="raw_stock_prices",
                name="ingest_stock_prices_node",
            ),
            node(
                func=ingest_macro_indicators,
                inputs={
                    "fred_series": "params:ingestion.fred_series",
                    "start_date": "params:ingestion.start_date",
                    "end_date": "params:ingestion.end_date",
                },
                outputs="raw_macro_indicators",
                name="ingest_macro_indicators_node",
            ),
        ]
    )
