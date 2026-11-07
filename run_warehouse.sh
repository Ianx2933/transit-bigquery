#!/usr/bin/env bash
# Export Cloud SQL to parquet, load BigQuery staging, rebuild the dbt models.
#
# Same steps as the local Airflow `warehouse` DAG, in one container so a Cloud
# Workflows step can run it as a single job. Every step stops the run on
# failure, and `dbt build` runs the tests with the models, so transit_marts is
# not rebuilt from data that failed a check.
#
# Environment:
#   PGPASSWORD         from Secret Manager (db-password)
#   BQ_PROJECT         BigQuery project holding transit_staging and transit_marts
#   CLOUD_SQL_INSTANCE project:region:instance, mounted at /cloudsql/<instance>
set -euo pipefail

: "${PGPASSWORD:?PGPASSWORD is not set}"
: "${BQ_PROJECT:?BQ_PROJECT is not set}"
: "${CLOUD_SQL_INSTANCE:?CLOUD_SQL_INSTANCE is not set}"

export PGHOST="/cloudsql/${CLOUD_SQL_INSTANCE}"
export PGPORT=5432 PGUSER="${PGUSER:-postgres}" PGDATABASE="${PGDATABASE:-Seoul_Transit}"
OUT="/tmp/export/$(date -u +%Y%m%dT%H%M%SZ)"

echo "== export from ${CLOUD_SQL_INSTANCE}"
python /app/transit_bigquery.py export --out "$OUT" --overwrite \
  --host "$PGHOST" --port "$PGPORT" --user "$PGUSER" --dbname "$PGDATABASE"

echo "== load into ${BQ_PROJECT}.transit_staging"
python /app/transit_bigquery.py load --folder "$OUT" --project "$BQ_PROJECT" --replace

echo "== dbt build (prod)"
dbt build --target prod --project-dir /app/transit_dbt

echo "== done"
