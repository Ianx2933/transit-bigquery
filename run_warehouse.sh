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
#   WAREHOUSE_STEPS    all (default): export, load, OD loads, dbt build, typical OD export
#                      od: only the OD loads (route stop OD, district mode OD), by hand
#                      od-typical: only the typical OD export to Cloud Storage, by hand
#                      (gcloud run jobs execute ... --update-env-vars WAREHOUSE_STEPS=od)
#
# The typical OD export (od_typical_export.py) writes gs://.../od-typical/<day>/
# for SeoulTransitPlatform's transit-od-typical-load job; it does nothing when
# the folder already holds the same numbers.
set -euo pipefail

: "${PGPASSWORD:?PGPASSWORD is not set}"
: "${BQ_PROJECT:?BQ_PROJECT is not set}"
: "${CLOUD_SQL_INSTANCE:?CLOUD_SQL_INSTANCE is not set}"

export PGHOST="/cloudsql/${CLOUD_SQL_INSTANCE}"
export PGPORT=5432 PGUSER="${PGUSER:-postgres}" PGDATABASE="${PGDATABASE:-Seoul_Transit}"
OUT="/tmp/export/$(date -u +%Y%m%dT%H%M%SZ)"
STEPS="${WAREHOUSE_STEPS:-all}"

if [ "$STEPS" = "od" ]; then
  echo "== OD loads into ${BQ_PROJECT}.transit_staging"
  python /app/od_load.py --project "$BQ_PROJECT"
  python /app/od_dong_load.py --project "$BQ_PROJECT"
  echo "== done (OD loads only)"
  exit 0
fi
if [ "$STEPS" = "od-typical" ]; then
  echo "== typical OD export to Cloud Storage"
  python /app/od_typical_export.py --project "$BQ_PROJECT"
  echo "== done (typical OD export only)"
  exit 0
fi
if [ "$STEPS" != "all" ]; then
  echo "WAREHOUSE_STEPS must be all, od or od-typical, not ${STEPS}" >&2
  exit 2
fi

echo "== export from ${CLOUD_SQL_INSTANCE}"
python /app/transit_bigquery.py export --out "$OUT" --overwrite \
  --host "$PGHOST" --port "$PGPORT" --user "$PGUSER" --dbname "$PGDATABASE"

echo "== load into ${BQ_PROJECT}.transit_staging"
python /app/transit_bigquery.py load --folder "$OUT" --project "$BQ_PROJECT" --replace

echo "== OD loads into ${BQ_PROJECT}.transit_staging"
python /app/od_load.py --project "$BQ_PROJECT"
python /app/od_dong_load.py --project "$BQ_PROJECT"

echo "== dbt build (prod)"
dbt build --target prod --project-dir /app/transit_dbt

echo "== typical OD export to Cloud Storage"
python /app/od_typical_export.py --project "$BQ_PROJECT"

echo "== done"
