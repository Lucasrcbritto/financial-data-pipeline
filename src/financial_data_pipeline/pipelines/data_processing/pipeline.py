from kedro.pipeline import Pipeline, node, pipeline

from .nodes import clean_macro_indicators, clean_stock_prices, join_market_data


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            node(
                func=clean_stock_prices,
                inputs={
                    "raw_stock_prices": "raw_stock_prices",
                    "null_threshold": "params:processing.null_threshold",
                    "required_stock_columns": (
                        "params:processing.required_stock_columns"
                    ),
                },
                outputs="int_stock_prices",
                name="clean_stock_prices_node",
            ),
            node(
                func=clean_macro_indicators,
                inputs={
                    "raw_macro_indicators": "raw_macro_indicators",
                },
                outputs="int_macro_indicators",
                name="clean_macro_indicators_node",
            ),
            node(
                func=join_market_data,
                inputs={
                    "int_stock_prices": "int_stock_prices",
                    "int_macro_indicators": "int_macro_indicators",
                },
                outputs="prm_market_data",
                name="join_market_data_node",
            ),

        ]
    )
