from kedro.pipeline import Pipeline

from financial_data_pipeline.pipelines.bigquery_load import pipeline as bq_pipeline
from financial_data_pipeline.pipelines.data_ingestion import (
    pipeline as ingestion_pipeline,
)
from financial_data_pipeline.pipelines.data_processing import (
    pipeline as processing_pipeline,
)
from financial_data_pipeline.pipelines.feature_engineering import (
    pipeline as feature_pipeline,
)


def register_pipelines() -> dict[str, Pipeline]:
    ingestion = ingestion_pipeline.create_pipeline()
    processing = processing_pipeline.create_pipeline()
    features = feature_pipeline.create_pipeline()
    bq_load = bq_pipeline.create_pipeline()

    return {
        "__default__": ingestion + processing + features + bq_load,
        "ingestion": ingestion,
        "processing": processing,
        "features": features,
        "bq_load": bq_load,
    }
