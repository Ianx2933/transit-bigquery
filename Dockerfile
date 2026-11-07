# Warehouse step of the monthly pipeline, run as a Cloud Run Job.
#
#   Cloud SQL (PostgreSQL) -> parquet in /tmp -> BigQuery transit_staging -> dbt build
#
# The image holds no credentials. The job's service account authenticates to
# BigQuery through Application Default Credentials, and the database password
# arrives as PGPASSWORD from Secret Manager.
FROM python:3.12-slim

WORKDIR /app
ENV PYTHONUNBUFFERED=1 \
    DBT_PROFILES_DIR=/app/transit_dbt/profiles \
    DBT_TARGET_PATH=/tmp/dbt/target \
    DBT_LOG_PATH=/tmp/dbt/logs

RUN pip install --no-cache-dir \
      "dbt-core>=1.12,<1.13" \
      "dbt-bigquery>=1.12,<1.13" \
      "google-cloud-bigquery>=3.25" \
      "pyarrow>=22" \
      "psycopg2-binary>=2.9.12" \
 && dbt --version

COPY transit_bigquery.py run_warehouse.sh ./
COPY transit_dbt ./transit_dbt

ENTRYPOINT ["bash", "/app/run_warehouse.sh"]
