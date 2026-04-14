FROM apache/airflow:3.0.0-python3.11
 
USER root
 
# Copy the python ingestion package into the image
# and grant the airflow user write access so pip can
# create crypto_pipeline.egg-info during wheel build
COPY python/ /opt/crypto_pipeline/
RUN chown -R airflow:root /opt/crypto_pipeline/
 
USER airflow
 
# Install in separate steps so pip resolves each dependency graph independently.
# Installing all at once causes ResolutionTooDeep due to overlapping constraints
# between dbt-bigquery and apache-airflow-providers-fab.
RUN pip install --no-cache-dir /opt/crypto_pipeline/
 
RUN pip install --no-cache-dir dbt-bigquery
 
RUN pip install --no-cache-dir apache-airflow-providers-fab