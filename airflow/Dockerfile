# Airflow with dbt and Snowflake
FROM apache/airflow:2.8.0-python3.11

USER root

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Install Python packages
RUN pip install --no-cache-dir \
    dbt-snowflake==1.7.0 \
    snowflake-connector-python[pandas] \
    python-dotenv \
    apache-airflow-providers-snowflake

# Copy dbt profiles
COPY --chown=airflow:root dbt_profiles.yml /home/airflow/.dbt/profiles.yml
