# Financial Data Pipeline

A production-grade financial data pipeline built with Kedro, following the Medallion Architecture pattern. Ingests stock prices and macroeconomic indicators, engineers features, and loads a star schema into BigQuery.

## Architecture

    Bronze       Silver          Gold          Features        BigQuery
    ------       ------          ----          --------        -------
    raw_stock -> int_stock  |               feat_market ->  fact_prices
    raw_macro -> int_macro  | -> prm_market -> data          dim_ticker
                                                              dim_date
                                                              dim_macro

## Pipelines

| Pipeline     | Description                                                 |
|--------------|-------------------------------------------------------------|
| `ingestion`  | Fetches stock prices (yfinance) and macro indicators (FRED) |
| `processing` | Cleans, validates, and joins data                           |
| `features`   | Engineers returns, volatility, SMA, RSI per ticker          |
| `bq_load`    | Builds star schema tables for BigQuery                      |

## Stack

- **Kedro** — pipeline orchestration
- **uv** — dependency and environment management
- **pandas** — data transformation
- **yfinance + pandas-datareader** — data ingestion
- **BigQuery** — data warehouse (GCP)
- **Docker** — containerisation
- **ruff** — linting and formatting
- **pytest** — testing

## Quickstart

    # Install dependencies
    uv sync

    # Run full pipeline
    uv run kedro run

    # Run individual pipeline
    uv run kedro run --pipeline ingestion

    # Run tests
    uv run pytest tests/ -v

    # Lint
    uv run ruff check src/ tests/

## Run with Docker

    # Set your GCP project
    export GCP_PROJECT_ID=your-project-id

    # Build and run
    docker compose up --build

## Project Structure

    src/financial_data_pipeline/
    ├── pipelines/
    │   ├── data_ingestion/
    │   ├── data_processing/
    │   ├── feature_engineering/
    │   └── bigquery_load/
    ├── pipeline_registry.py
    conf/
    ├── base/
    │   ├── catalog.yml
    │   ├── parameters.yml
    │   └── logging.yml
    └── local/       # git-ignored — real credentials go here
    tests/
    └── pipelines/
        ├── test_data_processing.py
        ├── test_feature_engineering.py
        └── test_bigquery_load.py

## Configuration

All parameters are in `conf/base/parameters.yml`. GCP credentials go in `conf/local/credentials.yml` (never committed).

Required environment variable:

    GCP_PROJECT_ID=your-gcp-project-id
