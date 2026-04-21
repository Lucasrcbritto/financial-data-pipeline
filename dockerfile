FROM python:3.12-slim

# Set working directory
WORKDIR /app

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

# Copy dependency files first (for layer caching)
COPY pyproject.toml uv.lock ./

# Install dependencies
RUN uv sync --frozen --no-dev

# Copy source code and config
COPY src/ src/
COPY conf/ conf/

# Set Python path so Kedro can find the package
ENV PYTHONPATH=/app/src

# Default command runs the full pipeline
CMD ["uv", "run", "kedro", "run"]