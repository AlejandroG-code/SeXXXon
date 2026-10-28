FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy pyproject.toml, README.md, and source code
COPY pyproject.toml README.md ./
COPY app ./app
COPY scripts ./scripts
COPY tests ./tests

# Install dependencies and project in non-editable mode
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir .

# Create storage directory for CSV artifacts
RUN mkdir -p /app/storage/reports

EXPOSE 8000

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
