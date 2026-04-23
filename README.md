# Financial Data Pipeline

A production-grade financial data pipeline built with Kedro, following the Medallion Architecture pattern. Ingests stock prices and macroeconomic indicators, engineers features, and outputs a star schema ready for analytical querying.

## Architecture

    Bronze       Silver          Gold          Features        Star Schema
    ------       ------          ----          --------        -----------
    raw_stock -> int_stock  |               feat_market ->  fact_prices
    raw_macro -> int_macro  | -> prm_market -> data          dim_ticker
                                                              dim_date
                                                              dim_macro

## Pipelines

| Pipeline       | Description                                                 |
|----------------|-------------------------------------------------------------|
| `ingestion`    | Fetches stock prices (yfinance) and macro indicators (FRED) |
| `processing`   | Cleans, validates, and joins data                           |
| `features`     | Engineers returns, volatility, SMA, RSI per ticker          |
| `star_schema`  | Builds fact and dimension tables for analytical querying    |

## Stack

- **Kedro** — pipeline orchestration
- **uv** — dependency and environment management
- **pandas** — data transformation
- **yfinance + pandas-datareader** — data ingestion
- **ruff** — linting and formatting
- **pytest** — testing
- **Streamlit** — data visualisation dashboard

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

    # Launch dashboard
    uv run streamlit run app.py

## Project Structure

    src/financial_data_pipeline/
    ├── pipelines/
    │   ├── data_ingestion/
    │   ├── data_processing/
    │   ├── feature_engineering/
    │   └── star_schema/
    ├── pipeline_registry.py
    conf/
    ├── base/
    │   ├── catalog.yml
    │   ├── parameters.yml
    │   └── logging.yml
    └── local/       # git-ignored
    tests/
    └── pipelines/
        ├── test_data_processing.py
        ├── test_feature_engineering.py
        └── test_star_schema.py

## Configuration

All parameters are in `conf/base/parameters.yml`.
