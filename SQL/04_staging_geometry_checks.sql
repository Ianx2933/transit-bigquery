-- Check WKT presence and GEOGRAPHY convertibility in the staging tables.
--
-- These are the raw 1:1 copies in transit_staging, not the modelled tables.
-- Geometry is exported as EPSG:4326 2D WKT because Parquet has no GEOGRAPHY
-- type; this confirms every value survives the round trip before anything is
-- built on top of it.
--
-- 'rows' is a reserved keyword in BigQuery, hence row_count.
-- If invalid_wkt is greater than zero, inspect source geometries rather than hiding failures as NULL.
SELECT 'admin_dong_boundary' AS table_name, COUNT(*) AS row_count,
  COUNTIF(geom_wkt IS NULL) AS null_wkt,
  COUNTIF(geom_wkt IS NOT NULL AND SAFE.ST_GEOGFROMTEXT(geom_wkt) IS NULL) AS invalid_wkt
FROM `project-9c77d932-f0d2-47bb-947.transit_staging.admin_dong_boundary`
UNION ALL
SELECT 'bus_stop_location', COUNT(*), COUNTIF(geom_wkt IS NULL),
  COUNTIF(geom_wkt IS NOT NULL AND SAFE.ST_GEOGFROMTEXT(geom_wkt) IS NULL)
FROM `project-9c77d932-f0d2-47bb-947.transit_staging.bus_stop_location`
UNION ALL
SELECT 'integrated_bus_stop_location', COUNT(*), COUNTIF(geom_wkt IS NULL),
  COUNTIF(geom_wkt IS NOT NULL AND SAFE.ST_GEOGFROMTEXT(geom_wkt) IS NULL)
FROM `project-9c77d932-f0d2-47bb-947.transit_staging.integrated_bus_stop_location`
UNION ALL
SELECT 'bus_stop_terrain_stats', COUNT(*), COUNTIF(geom_wkt IS NULL),
  COUNTIF(geom_wkt IS NOT NULL AND SAFE.ST_GEOGFROMTEXT(geom_wkt) IS NULL)
FROM `project-9c77d932-f0d2-47bb-947.transit_staging.bus_stop_terrain_stats`;
