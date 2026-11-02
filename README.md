# transit-bigquery

This repository moves the analytical part of
[SeoulTransitPlatform](https://github.com/Ianx2933/SeoulTransitPlatform) into BigQuery as a
partitioned and clustered star schema, and checks it month by month against the PostgreSQL
source. The serving endpoints were left in PostGIS. They have a 192 ms latency target, and
BigQuery is priced and built for scans, not point lookups.

Loading is done in Python and transformation in dbt. 32 months of data (2024-01 to 2026-08,
29.9M rows) are loaded and modelled.

---

## What moved and what stayed

The platform's district-demand endpoint went from 9.5–12.9 s to 192 ms after query-plan
analysis and GIST, composite and expression indexes. Running it on BigQuery would make it
slower and more expensive, so the split follows the workload rather than the tables:

| Stays in PostGIS | Moved to BigQuery |
|---|---|
| District-demand and node-catchment endpoints | Hourly boarding facts (29.9M rows) |
| `ST_Covers` against dong polygons at query time | Aggregations by route, stop and hour |
| GIST-indexed spatial predicates | Terrain statistics joined to stops |
| Anything with a latency target | Ad-hoc analysis |

BigQuery has a `GEOGRAPHY` type and functions such as `ST_DWITHIN` and `ST_CONTAINS`, but it
has no index control comparable to GIST and works on spherical geometry only. Stop points are
stored as `GEOGRAPHY` here for analysis; spatial serving stays on PostGIS.

---

## Pipeline

```
PostgreSQL --(transit_bigquery.py)--> transit_staging --(dbt)--> modelled tables
```

**Extract and load: `transit_bigquery.py`.** Exports 13 source tables from a read-only
`REPEATABLE READ` snapshot and writes a manifest with a SHA-256 checksum per file and row
counts and passenger totals per month. A load-state file makes reruns idempotent, and the
script will not append to a table it has no load record for. After loading, it recomputes the
monthly totals in BigQuery and stops if any month differs. It does not transform anything.

**Transform: `transit_dbt/`.** All renames, casts, joins and deduplication are dbt models.
`dbt build` runs the models and their tests in one step, so a build fails if the data does not
pass.

The first version did the transformation in Python (`extract.py`: PostgreSQL to Parquet to the
warehouse tables). I moved it to dbt to keep the logic in SQL next to the data, to get tests
on every build, and to make it clear where each column comes from. Writing the models also
exposed the stop-name issue described below, which the Python version never measured.

| Dataset | Contents |
|---|---|
| `transit_staging` | 1:1 copy of 13 source tables: same column names and grain, geometry as EPSG:4326 WKT |
| `transit_marts` | Modelled tables, built with `dbt build --target prod` |
| `dbt_dev` | Development copy of the same models, the default dbt target |
| `transit_warehouse` | The original six-month load, made before the upstream loader fix. Kept for reference only |

Staging and the modelled tables are kept in separate datasets so it is always clear which
tables are copies and which are designed. The tests also reconcile against staging instead of
querying PostgreSQL each time.

---

## Grain

The source primary key is `(use_ym, route_no, stop_ars, hour)`. There is no date column:
`use_ym` is a `YYYYMM` string, and the data is a monthly aggregate by hour.

Fact grain: one row per route, stop, hour and calendar month.

**No `dim_date`.** The data has no daily detail, so a daily date dimension would suggest
observations that do not exist. `service_month` is stored as a `DATE` set to the first of the
month for readability.

**No OD fact.** An earlier draft had origin and destination stop keys on the boarding fact,
but the source has neither. Boardings are measured per stop, while OD is estimated later by a
separate pipeline. Putting measured and estimated values in one table would leave the grain
unclear. If OD is added later it will be a separate table.

---

## Star schema

```
                    +--------------------+
                    |     dim_route      |
                    |--------------------|
                    | route_no      (PK) |
                    | route_name         |
                    | route_name_count   |
                    +---------+----------+
                              |
        +---------------------v--------------------+
        |              fct_boarding                |
        |------------------------------------------|
        | service_ym      INT64   (partition key)  |
        | service_month   DATE                     |
        | hour            INT64                    |
        | route_no        STRING  -> dim_route     |
        | stop_key        STRING  -> dim_stop      |
        | boardings       INT64   (measure)        |
        | alightings      INT64   (measure)        |
        +---------------------+--------------------+
                              |
                    +---------v----------+
                    |      dim_stop      |
                    |--------------------|
                    | stop_key      (PK) |
                    | stop_name          |
                    | stop_name_count    |
                    | match_method       |
                    | lon / lat / geog   |
                    | elev_m             |
                    | slope_pct          |
                    | dataset_id         |
                    +--------------------+

dim_stop_name_history   every (stop, name) pair with the first and last month it appears
route_stop_sequence     position of each stop on each route, with the months it applies to
```

`dim_stop` includes terrain attributes from
[geo-raster-pipeline](https://github.com/Ianx2933/geo-raster-pipeline): `elev_m`, `slope_pct`
and `dataset_id`. This is where the two repositories connect. Only the active terrain dataset
is joined, so publishing another terrain run will not duplicate stops.

`dataset_id` is kept because the terrain tables upstream are published as versioned
snapshots. Without it, two loads made months apart could not be told apart.

When the source has several values for one key, such as a route or stop that appears under
more than one name, the value from the most recent month is used, with ties broken
alphabetically. This makes every build produce the same result. The old `DISTINCT ON` in
`extract.py` did not guarantee that.

---

## Stop names

Stop names in the source end with the stop's position on the route, for example
`동묘앞(00029)`. A stop served by several routes appears with a different number on each one.
Counted as-is, 68% of Seoul stops looked as if they had been renamed during the 32 months. The
earlier six-month analysis made the same mistake and reported "1,815 Gyeonggi stops with 2,963
names" as rename history.

With the number split off into a separate column (`route_stop_seq`), the counts are:

| `match_method` | Stops | Stops with more than one name |
|---|---|---|
| `ars_exact` | 10,800 | 1,468 (14%) |
| `gyeonggi_unmatched` | 2,044 | 156 (8%) |
| `virtual_terminal` | 1 | n/a |

Most of the remaining cases are actual renames that follow one after another: exit numbers
added (`태릉입구역` to `태릉입구역1번출구`), apartment or building names changed, and one spelling
correction (`파인에빈뉴` to `파인에비뉴`). `dim_stop` uses the latest name, and
`dim_stop_name_history` keeps every name with the months it was used. This history is built
from the monthly data rather than with a dbt snapshot, since snapshots only record changes
from the time they start running.

Some stops have two names in the same months:

- Village buses (마을버스) and city buses sometimes name the same stop differently. ARS `04266`
  is `서울숲1번출입구` on village bus 성동13 and `서울숲1번출구` on city bus 121.
- ARS `20117` appears as both `문촌마을6.7단지...` and `사육신공원` until 2024-11. These are
  unrelated names, so the code is probably shared or reused in the source.

The route position is kept in `route_stop_sequence` at the grain `(route_no, stop_key,
route_stop_seq)`. A route that passes the same stop twice (outbound and at the turnaround) has
two rows for that pair. This is the same case that caused the upstream loader bug described
under Validation.

---

## Partitioning

`fct_boarding` is partitioned on `service_ym` (INT64) with integer-range partitioning, not on
`service_month` (DATE).

The project started in the BigQuery sandbox, which sets a 60-day partition expiration that
cannot be turned off. Expiration is counted from the partition's own date, so a day-partitioned
table of 2025 data was empty right after loading:

```
$ bq load --source_format=PARQUET transit_warehouse.fact_boarding fact_boarding.parquet
Waiting on bqjob_... (3s) Current status: DONE

$ bq query "SELECT count(*) FROM transit_warehouse.fact_boarding WHERE service_month IS NOT NULL"
0
```

The job reported `DONE` and no error was shown. Integer-range partitions are not affected by
this rule. The data is monthly anyway, so a daily partition key would not have fit. I kept the
integer key after enabling billing.

**Sandbox settings stay after billing is enabled.** Linking a billing account removes the
limit for new datasets, but existing settings are not changed. Both datasets still had a 60-day
`defaultTableExpirationMs`, and each table kept its own expiration time. When I checked, the
twelve staging tables had already been deleted and the three warehouse tables were due to
expire in seven days. Each setting has to be cleared separately:

```
bq update --default_table_expiration 0 --default_partition_expiration 0 <dataset>
bq update --expiration 0 <dataset>.<table>
```

The table is clustered on `route_no, stop_key`, which follows the source key order and the
filters most queries use.

The original table had `require_partition_filter = TRUE`. The dbt model does not, because the
reconciliation tests read all months and would each need a placeholder filter to pass. Cost is
limited instead by `maximum_bytes_billed` in the dbt profile, which rejects any query that
would scan more than the set amount.

---

## Validation

Validation is done by the dbt tests, which run on every `dbt build`.

| Test | Fails when |
|---|---|
| Monthly reconciliation to staging | Any month's row count, boarding sum or alighting sum differs from the source copy |
| Unique `(service_ym, route_no, stop_key, hour)` | The fact grain has duplicates |
| Non-empty fact | The fact table has no rows after a successful load |
| `dim_stop` row count equals distinct source stops | A join drops stops or duplicates them |
| `relationships` from fact to dimensions | A route or stop key in the fact has no dimension row |
| Unique and not-null keys, accepted `match_method` values | Standard dimension checks |

Current figures:

| | PostgreSQL | BigQuery (`fct_boarding`) |
|---|---|---|
| Rows | 29,855,640 | 29,855,640 |
| Boardings | 4,509,456,552 | 4,509,456,552 |
| Months | 32 | 32 |
| Distinct stops | 12,845 | 12,845 |

The reconciliation compares totals by month only and does not join to the stop table. In the
first version, a validation query joined to the stop master, and stops with several names
caused the join to duplicate rows. The boarding total came out as 793,288,854, which was
127,070 too high. The test is written to avoid that join.

The first six-month load matched the source exactly, but the source itself was wrong. The
upstream loader kept only one row when a route passed the same stop twice, which understated
monthly boardings by about 3.6%. Matching the source shows that the copy is correct, not that
the numbers are. The 32-month reload uses the corrected source. The old six months remain in
`transit_warehouse` so the difference can still be checked.

---

## Scan cost

| Query range | Bytes scanned |
|---|---|
| One month (`service_ym = 202503`) | 22.3 MB |
| Six months (`202501` to `202506`) | 133.9 MB |

These were measured on the original six-month table. Each month has about the same number of
rows after the reload, so the cost per month should be similar. Building the full fact table
with dbt processes 1.3 GiB. The free tier includes 10 GB of storage and 1 TB of queries per
month, so the project currently costs nothing.

---

## Reproducing

```bash
pip install -r requirements.txt
python -m unittest test_transit_bigquery      # offline; no PostgreSQL or GCP calls
```

Load the staging copy:

```bash
python transit_bigquery.py export --out export_02   # prompts for the password
python transit_bigquery.py load  --folder export_02
bq query --use_legacy_sql=false < sql/04_staging_geometry_checks.sql
```

Build the models:

```bash
gcloud auth application-default login
cd transit_dbt
dbt build                  # development, writes to dbt_dev
dbt build --target prod    # writes to transit_marts
```

`~/.dbt/profiles.yml` is kept outside the repository. It uses OAuth, so no key file is needed:

```yaml
transit_dbt:
  target: dev
  outputs:
    dev:
      type: bigquery
      method: oauth
      project: <project-id>
      dataset: dbt_dev
      location: asia-northeast3
      threads: 4
      maximum_bytes_billed: 5000000000
    prod:
      type: bigquery
      method: oauth
      project: <project-id>
      dataset: transit_marts
      location: asia-northeast3
      threads: 4
      maximum_bytes_billed: 5000000000
```

`requirements.txt` pins `pyarrow>=22` and `psycopg2-binary>=2.9.12`, the first versions with
Python 3.14 wheels, and `dbt-core` / `dbt-bigquery` 1.12. dbt 1.11 and earlier do not run on
Python 3.14 (they fail with a `mashumaro` serialization error).

---

## Repository layout

```
transit_bigquery.py          Snapshot export and verified staging load
test_transit_bigquery.py     Offline tests; no PostgreSQL or GCP calls
transit_dbt/
  models/staging/            Sources and light cleaning of the staging copy
  models/marts/              fct_boarding, dim_route, dim_stop, dim_stop_name_history,
                             route_stop_sequence
  tests/                     Reconciliation and grain tests
sql/04_staging_geometry_checks.sql   WKT round-trip check on staging
legacy/                      Warehouse DDL and validation from before dbt (six-month load)
```

---

## To do

- `route_type` on `dim_route`. Seoul route names indicate the bus type (village, trunk,
  branch, night), which would explain the naming differences above.
- `dim_district`. The dong boundary table has the 1,208 administrative units of the
  2025-06-30 snapshot but is still imported by hand upstream.
- Airflow orchestration of export, load and `dbt build`.
- A flag on `dim_stop` for stops with two names at the same time.
- Estimated coordinates for unmatched Gyeonggi stops using `route_stop_sequence`.
