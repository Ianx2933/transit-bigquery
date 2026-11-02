-- Dimensional model in transit_warehouse.
-- Raw source tables live in transit_staging (see transit_bigquery.py); this
-- dataset holds only the modelled tables built from them.
--
-- Run in the BigQuery console or:
--   bq query --use_legacy_sql=false < sql/01_create_tables.sql

-- ---------------------------------------------------------------------------
-- fact_boarding
--
-- Grain: one row per route x stop x hour x calendar month.
-- The source primary key is (use_ym, route_no, stop_ars, hour) -- a monthly
-- hourly aggregate with no day-level detail, so there is no date dimension.
--
-- Partitioned on service_ym (INT64), not on service_month (DATE). The sandbox
-- enforces a 60-day partition expiration measured from the partition's own
-- date, so day-partitioned historical data expires on arrival: the load job
-- reports DONE, the table holds zero rows, and no error is raised anywhere.
-- RANGE_BUCKET on an integer is outside that rule, and since the source is a
-- monthly aggregate a day-grain partition key was never the right choice.
--
-- Columns are NULLABLE rather than REQUIRED: Parquet written by pandas/pyarrow
-- is nullable by default, and a REQUIRED column rejects the load with
-- "Field X has changed mode from REQUIRED to NULLABLE". Integrity is asserted
-- by sql/02_validate.sql against the source instead.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS transit_warehouse.fact_boarding (
  service_ym    INT64   OPTIONS (description = 'YYYYMM as an integer; partition key'),
  service_month DATE    OPTIONS (description = 'First of the month; readable form of service_ym, not the partition key'),
  hour          INT64   OPTIONS (description = 'Hour of day, 0-23'),
  route_no      STRING  OPTIONS (description = 'Route identifier; joins dim_route'),
  stop_key      STRING  OPTIONS (description = 'ARS code; joins dim_stop'),
  boardings     INT64   OPTIONS (description = 'Measure. Source stores double precision but holds no fractional part'),
  alightings    INT64   OPTIONS (description = 'Measure')
)
PARTITION BY RANGE_BUCKET(service_ym, GENERATE_ARRAY(202401, 202700, 1))
CLUSTER BY route_no, stop_key
OPTIONS (
  require_partition_filter = TRUE,
  description = 'Hourly boarding and alighting counts per route and stop, by calendar month.'
);

-- ---------------------------------------------------------------------------
-- dim_stop
--
-- One row per stop_ars, matching the fact table's join key.
--
-- match_method carries the provenance forward rather than filtering unmatched
-- rows away. Of 12,529 stops, 10,713 resolve against the Seoul stop master;
-- the rest are Gyeonggi-do stops on cross-boundary routes and route terminus
-- markers, together 4.5% of total boardings. Dropping them would break the
-- reconciliation in sql/02_validate.sql and would silently narrow the universe
-- an analyst sees.
--
-- The single virtual_terminal row is expected, not a loss: every terminus
-- marker carries the literal ARS '~' across 133 distinct names. The grain of a
-- dimension is its key, not the number of source labels mapping to it.
--
-- terminal_seq is the five-digit ordinal embedded in the stop name -- e.g.
-- 한림아파트(00020) -- which is the stop's position along the route.
-- (route_no, terminal_seq) is unique across all 38,941 route-stop pairs, so it
-- is available as a fallback key for coordinate interpolation later.
--
-- dataset_id is retained because upstream terrain values are published as
-- immutable versioned snapshots. A dimension keeping the slope value but
-- discarding which snapshot produced it would make two loads taken months apart
-- indistinguishable.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS transit_warehouse.dim_stop (
  stop_key     STRING    OPTIONS (description = 'ARS code; joins fact_boarding'),
  stop_ars     STRING,
  stop_id      STRING,
  node_id      STRING    OPTIONS (description = 'Stop master node id; null when unmatched'),
  stop_name    STRING,
  match_method STRING    OPTIONS (description = 'ars_exact | gyeonggi_unmatched | virtual_terminal'),
  terminal_seq INT64     OPTIONS (description = 'Ordinal position along the route, parsed from stop_name'),
  lon          FLOAT64,
  lat          FLOAT64,
  elev_m       FLOAT64   OPTIONS (description = 'From geo-raster-pipeline terrain snapshot'),
  slope_pct    FLOAT64   OPTIONS (description = 'From geo-raster-pipeline terrain snapshot'),
  dataset_id   STRING    OPTIONS (description = 'Terrain snapshot the elevation and slope came from')
)
OPTIONS (
  description = 'Stop dimension with match provenance and terrain attributes. Run sql/03_geography.sql after loading to add the geog column.'
);

-- ---------------------------------------------------------------------------
-- dim_route
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS transit_warehouse.dim_route (
  route_no   STRING  OPTIONS (description = 'Route identifier; joins fact_boarding'),
  route_name STRING  OPTIONS (description = 'route_name is not unique per route_no in the source; the most recent non-null name is kept')
)
OPTIONS (description = 'Route dimension.');
