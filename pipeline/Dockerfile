# Start with slim Python 3.13 image
FROM python:3.13.10-slim

# Copy uv binary from official uv image (multi-stage build pattern)
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/

# Set working directory
WORKDIR /app

# Add virtual environment to PATH so we can use installed packages
ENV PATH="/app/.venv/bin:$PATH"

# Copy dependency files first (better layer caching)
COPY "pyproject.toml" "uv.lock" ".python-version" ./
# Install dependencies from lock file (ensures reproducible builds)
RUN uv sync --locked

# Copy ingestion script
COPY Ingest_data.py ./

# Set entry point
ENTRYPOINT ["python", "Ingest_data.py"]

# uv sync --locked installs exact versions from uv.lock for reproducibility 
# Dependencies (pandas, sqlalchemy, psycopg2) are already in pyproject.toml
# Multi-stage build pattern copies uv from official image
# Copying dependency files before code improves Docker layer caching
