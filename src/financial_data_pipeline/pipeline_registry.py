from kedro.pipeline import Pipeline

from financial_data_pipeline.pipelines.data_ingestion import (
    pipeline as ingestion_pipeline,
)
from financial_data_pipeline.pipelines.data_processing import (
    pipeline as processing_pipeline,
)
from financial_data_pipeline.pipelines.feature_engineering import (
    pipeline as feature_pipeline,
)
from financial_data_pipeline.pipelines.star_schema import (
    pipeline as star_schema_pipeline,
)


def register_pipelines() -> dict[str, Pipeline]:
    ingestion = ingestion_pipeline.create_pipeline()
    processing = processing_pipeline.create_pipeline()
    features = feature_pipeline.create_pipeline()
    star_schema = star_schema_pipeline.create_pipeline()

    return {
        "__default__": ingestion + processing + features + star_schema,
        "ingestion": ingestion,
        "processing": processing,
        "features": features,
        "star_schema": star_schema,
    }
