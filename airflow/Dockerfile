FROM apache/airflow:3.0.0-python3.11

USER root

# Copy only pyproject.toml to install dependencies
# The actual code comes via volume mount at runtime
COPY python/pyproject.toml /opt/crypto_pipeline/pyproject.toml

RUN chown -R airflow:root /opt/crypto_pipeline/

USER airflow

# Install crypto_pipeline dependencies only (no code needed at build time)
RUN pip install --no-cache-dir /opt/crypto_pipeline/

RUN pip install --no-cache-dir dbt-bigquery

RUN pip install --no-cache-dir apache-airflow-providers-fab