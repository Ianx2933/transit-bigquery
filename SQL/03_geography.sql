-- Add the GEOGRAPHY point to dim_stop.
--
-- This runs after the load, not during it: Parquet has no GEOGRAPHY type, so
-- the column cannot be carried through the export.
--
-- CREATE OR REPLACE TABLE AS SELECT rather than UPDATE, because the sandbox
-- does not support DML. CTAS is DDL and runs there.
--
-- Rerunning is safe: SELECT * EXCEPT(geog) drops the previous column before
-- recomputing it, so the statement is idempotent rather than accumulating
-- geog_1, geog_2 on each run.
--
--   bq query --use_legacy_sql=false < sql/03_geography.sql

CREATE OR REPLACE TABLE transit_warehouse.dim_stop AS
SELECT
  * EXCEPT (geog),
  CASE WHEN lon IS NOT NULL AND lat IS NOT NULL
       THEN ST_GEOGPOINT(lon, lat)
  END AS geog
FROM transit_warehouse.dim_stop;

-- Verify. Points must exist for exactly the ars_exact population, and all of
-- them must fall inside a plausible bounding box for the Seoul metropolitan
-- area -- a swapped lon/lat pair produces a valid GEOGRAPHY somewhere in the
-- Indian Ocean and would otherwise pass silently.
SELECT
  match_method,
  COUNT(*)                AS row_count,
  COUNTIF(geog IS NOT NULL) AS with_point,
  COUNTIF(geog IS NOT NULL
          AND NOT ST_INTERSECTSBOX(geog, 126.0, 36.5, 128.0, 38.5)) AS outside_metro_area
FROM transit_warehouse.dim_stop
GROUP BY match_method
ORDER BY row_count DESC;
