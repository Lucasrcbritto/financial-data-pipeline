from kedro.pipeline import Pipeline, node, pipeline

from .nodes import engineer_features


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            node(
                func=engineer_features,
                inputs={
                    "prm_market_data": "prm_market_data",
                    "rolling_windows": "params:feature_engineering.rolling_windows",
                    "annualization_factor": (
                        "params:feature_engineering.annualization_factor"
                    ),
                },
                outputs="feat_market_data",
                name="engineer_features_node",
            ),
        ]
    )
