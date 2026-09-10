-- Reconcile the warehouse against the PostGIS source.
-- Every query here has a PostgreSQL counterpart in the README; the two sides
-- must produce identical numbers.
--
--   bq query --use_legacy_sql=false < sql/02_validate.sql

-- ---------------------------------------------------------------------------
-- 1. Row counts, aggregate totals, distinct keys.
--
-- PostgreSQL side:
--   SELECT count(*), sum(boarding_passengers), sum(alighting_passengers),
--          count(DISTINCT stop_ars)
--   FROM hourly_bus_stop_passenger;
--
-- Measure the baseline on the fact table alone. An early version of this check
-- joined to the stop master and reported 793,288,854 boardings -- the join fans
-- out on Gyeonggi stops carrying multiple names, inflating the sum by 127,070.
-- A validation query that joins is not validating the fact table.
-- ---------------------------------------------------------------------------
SELECT
  COUNT(*)                    AS row_count,      -- expect 5,581,104
  SUM(boardings)              AS total_boardings, -- expect 793,161,784
  SUM(alightings)             AS total_alightings,-- expect 783,117,413
  COUNT(DISTINCT stop_key)    AS distinct_stops   -- expect 12,529
FROM transit_warehouse.fact_boarding
WHERE service_ym BETWEEN 202501 AND 202506;

-- ---------------------------------------------------------------------------
-- 2. Per-partition counts against the monthly counts in the source.
--
-- INFORMATION_SCHEMA.PARTITIONS is readable without a partition filter, which
-- also makes it the way to spot a __NULL__ partition -- the symptom of a
-- partition key that failed to populate during export.
-- ---------------------------------------------------------------------------
SELECT partition_id, total_rows
FROM transit_warehouse.INFORMATION_SCHEMA.PARTITIONS
WHERE table_name = 'fact_boarding'
ORDER BY partition_id;

-- ---------------------------------------------------------------------------
-- 3. Referential integrity: every fact stop_key resolves in dim_stop.
-- Expect 0.
-- ---------------------------------------------------------------------------
SELECT COUNT(DISTINCT f.stop_key) AS orphan_stop_keys
FROM transit_warehouse.fact_boarding f
LEFT JOIN transit_warehouse.dim_stop d USING (stop_key)
WHERE f.service_ym BETWEEN 202501 AND 202506
  AND d.stop_key IS NULL;

-- ---------------------------------------------------------------------------
-- 4. Match populations. Expect 10,713 / 1,815 / 1, summing to 12,529.
-- no_coord must equal the row count for both unmatched populations: an
-- unmatched stop with coordinates would mean the match_method is wrong.
-- ---------------------------------------------------------------------------
SELECT
  match_method,
  COUNT(*)                  AS row_count,
  COUNTIF(lon IS NULL)      AS no_coord
FROM transit_warehouse.dim_stop
GROUP BY match_method
ORDER BY row_count DESC;

-- ---------------------------------------------------------------------------
-- 5. require_partition_filter is active.
-- This statement MUST fail with:
--   Cannot query over table 'transit_warehouse.fact_boarding' without a filter
--   over column(s) 'service_ym' that can be used for partition elimination
-- If it succeeds, the guard is not in place.
-- ---------------------------------------------------------------------------
-- SELECT COUNT(*) FROM transit_warehouse.fact_boarding;
