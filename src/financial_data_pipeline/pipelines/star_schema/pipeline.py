from kedro.pipeline import Pipeline, node, pipeline

from .nodes import build_dim_date, build_dim_macro, build_dim_ticker, build_fact_prices


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            node(
                func=build_dim_ticker,
                inputs={"feat_market_data": "feat_market_data"},
                outputs="dim_ticker",
                name="build_dim_ticker_node",
            ),
            node(
                func=build_dim_date,
                inputs={"feat_market_data": "feat_market_data"},
                outputs="dim_date",
                name="build_dim_date_node",
            ),
            node(
                func=build_dim_macro,
                inputs={"feat_market_data": "feat_market_data"},
                outputs="dim_macro",
                name="build_dim_macro_node",
            ),
            node(
                func=build_fact_prices,
                inputs={"feat_market_data": "feat_market_data"},
                outputs="fact_prices",
                name="build_fact_prices_node",
            ),
        ]
    )
