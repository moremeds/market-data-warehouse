FROM python:3.13-slim

WORKDIR /app

# Install dependencies
RUN pip install --no-cache-dir \
    ib-async \
    pyarrow \
    duckdb \
    rich \
    boto3 \
    httpx \
    requests \
    pandas \
    polars

# Copy application code
COPY clients/ clients/
COPY scripts/ scripts/
COPY presets/ presets/
COPY docker/ibroker-mkt-data/entrypoint.py entrypoint.py

ENV PYTHONUNBUFFERED=1

ENTRYPOINT ["python", "entrypoint.py"]
