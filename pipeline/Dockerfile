# Flight Pricing Pipeline
FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
RUN pip install --no-cache-dir dbt-snowflake

# Copy pipeline files
COPY . .

# Default command
CMD ["python", "ingest.py", "--records", "1000"]
