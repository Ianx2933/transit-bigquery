# transit-bigquery

Migrates the analytical half of
[SeoulTransitPlatform](https://github.com/Ianx2933/SeoulTransitPlatform) to BigQuery as a
partitioned, clustered star schema, reconciled row-for-row against the PostGIS source. The
serving endpoints stayed in PostGIS: a 192 ms point-lookup budget is the wrong workload for a
scan-priced columnar warehouse, and moving it would have undone the indexing work it depends
on.

---

## What moved, and what did not

BigQuery is an analytical warehouse: columnar, scan-priced, with no concept of a point lookup
returning in 200 ms. The platform's district-demand endpoint was taken from 9.5–12.9 s to
192 ms through query-plan analysis and GIST / composite / expression indexes. That is a
*serving* concern. Moving it here would have made it slower and more expensive.

So the split is by workload, not by table:

| Stays in PostGIS  | Moved to BigQuery |
|---|---|
| District-demand and node-catchment endpoints | Hourly boarding facts (5.58M rows) |
| `ST_Covers` against dong polygons at query time | Aggregations by route, stop, hour |
| GIST-indexed spatial predicates | Terrain statistics joined to stops |
| Anything with a latency budget | Ad-hoc analyst queries |

BigQuery has a native `GEOGRAPHY` type and `ST_DWITHIN` / `ST_DISTANCE` / `ST_CONTAINS`, but
it is not PostGIS — no GIST index control, a different function set, spherical geometry only.
Stop points are loaded as `GEOGRAPHY` to demonstrate BigQuery GIS; the spatial serving path
stays on PostGIS.

---

## Two datasets

| Dataset | Contents |
|---|---|
| `transit_staging` | A faithful 1:1 copy of twelve source tables — same column names, same grain, geometry as EPSG:4326 WKT |
| `transit_warehouse` | Only the modelled tables: `fact_boarding`, `dim_stop`, `dim_route` |

Merging them would leave a reader unable to tell which tables were designed and which were
copied, and that distinction is the point of the exercise. Staging also gives the validation
queries something to reconcile against without reaching back into PostgreSQL every time.

`transit_bigquery.py` does the staging half: a read-only `REPEATABLE READ` snapshot, a
manifest with per-file SHA-256, a load-state record making reruns idempotent, and a refusal to
append to any table it has no successful load record for. `extract.py` does the modelling
half.

---

## Grain

The source primary key is `(use_ym, route_no, stop_ars, hour)`. There is **no date column** —
`use_ym` is a `YYYYMM` string. The source is a monthly hourly aggregate, not a daily
observation series.

Fact grain: **one row per route × stop × hour × calendar month.**

Two consequences:

**No `dim_date`.** *(날짜 차원을 만들지 않았다.)* A dense date dimension over data with no
day-level detail would imply observations that do not exist. `service_month` is carried as a
readable `DATE` pinned to the first of the month; month attributes derive from it directly.

**No OD fact.** An earlier draft had `origin_stop_key` /
`dest_stop_key` on the boarding fact. The source has neither. Boardings are measured per stop;
OD is *estimated* downstream by a separate pipeline. Mixing a measured fact and a derived fact
in one table leaves the grain undefined. If OD is warehoused later it gets its own table,
named so the distinction survives.

---

## Star schema

```
                    ┌──────────────────┐
                    │    dim_route     │
                    │──────────────────│
                    │ route_no    (PK) │
                    │ route_name       │
                    └────────┬─────────┘
                             │
        ┌────────────────────▼─────────────────────┐
        │            fact_boarding                 │
        │──────────────────────────────────────────│
        │ service_ym      INT64   ← partition key  │
        │ service_month   DATE                     │
        │ hour            INT64                    │
        │ route_no        STRING  → dim_route      │
        │ stop_key        STRING  → dim_stop       │
        │ boardings       INT64   ← measure        │
        │ alightings      INT64   ← measure        │
        └────────────────────┬─────────────────────┘
                             │
                    ┌────────▼─────────┐
                    │     dim_stop     │
                    │──────────────────│
                    │ stop_key    (PK) │
                    │ stop_name        │
                    │ match_method     │
                    │ terminal_seq     │
                    │ lon / lat / geog │
                    │ elev_m           │
                    │ slope_pct        │
                    │ dataset_id       │
                    └──────────────────┘
```

`dim_stop` carries the terrain attributes from
[geo-raster-pipeline](https://github.com/Ianx2933/geo-raster-pipeline) — `elev_m`,
`slope_pct`, `dataset_id`. The raster snapshot becomes stop attributes here, which is what
connects the two repositories.

`dataset_id` is carried rather than dropped because the upstream terrain tables are published
as immutable versioned snapshots, not as columns on the stop master. A dimension that kept the
slope value but discarded which snapshot produced it would make two loads taken months apart
indistinguishable.

---

## Partitioning

`fact_boarding` is partitioned by `service_ym` (INT64) using `RANGE_BUCKET`, **not** by
`service_month` (DATE).

This was not the first choice. The BigQuery sandbox enforces a 60-day partition expiration
that cannot be disabled, and expiry is measured from the *partition's own date*. Day-based
partitioning over 2025 data therefore expires on arrival:

```
$ bq load --source_format=PARQUET transit_warehouse.fact_boarding fact_boarding.parquet
Waiting on bqjob_... (3s) Current status: DONE

$ bq query "SELECT count(*) FROM transit_warehouse.fact_boarding WHERE service_month IS NOT NULL"
0
```

The load job reports `DONE`, `bq show` reports 0 rows, and no error is raised anywhere.
This is the same class of silent-emptying failure the upstream terrain snapshots were designed around
 — a derived value written where the next operation destroys it, with the interface still returning success.

`RANGE_BUCKET` on an integer is outside that rule. Pruning and `require_partition_filter`
behave identically — and since the source is a monthly aggregate, a day-grain partition key
was never the right choice to begin with.

```sql
PARTITION BY RANGE_BUCKET(service_ym, GENERATE_ARRAY(202401, 202700, 1))
CLUSTER BY route_no, stop_key
OPTIONS (require_partition_filter = TRUE)
```

`require_partition_filter = TRUE` forces every query to filter on the partition column — the
single most effective guard against a `SELECT *` that scans the whole table and bills for it:

```
$ bq query "SELECT service_ym, count(*) FROM transit_warehouse.fact_boarding GROUP BY service_ym"
Cannot query over table 'transit_warehouse.fact_boarding' without a filter over
column(s) 'service_ym' that can be used for partition elimination
```

Clustering is on `route_no, stop_key`, matching both the source primary key order and the
filters analysts actually use. Clustering is free and reduces scan further.

Columns are `NULLABLE` rather than `REQUIRED`. Parquet written by pandas/pyarrow is nullable
by default, and a `REQUIRED` column rejects the load with *"Field X has changed mode from
REQUIRED to NULLABLE"*. Integrity is asserted by `sql/02_validate.sql` against the source
instead, which is a stronger check than a column constraint anyway.

---

## Stop matching and provenance

Of 12,529 distinct stops in the fact source, 10,713 match the Seoul stop master on ARS code.
The remainder are two distinct populations:

| `match_method` | Rows | Coordinates | What they are |
|---|---|---|---|
| `ars_exact` | 10,713 | yes | matched to Seoul stop master |
| `gyeonggi_unmatched` | 1,815 | no | Gyeonggi-do stops on cross-boundary routes — outside the Seoul master's scope |
| `virtual_terminal` | 1 | no | route origin/terminus marker |

Together the unmatched rows are **4.5% of total boardings** — the tail, not the core.

The single `virtual_terminal` row is deliberate, not a loss. Every terminus marker carries the literal ARS `~` across
133 distinct names. Since `stop_ars` is the fact table's join key, the dimension holds exactly
one row for it — the grain of a dimension is its key, not the number of source labels that map
to it. Splitting them would require a synthetic key the fact table does not have.

Unmatched rows are not deleted. They are carried with `match_method` recording how each
identifier was resolved, for two reasons: aggregate totals must reconcile against the source,
and an analyst doing spatial work needs to exclude them *explicitly* rather than silently
inherit a filtered universe.

This is the same discipline the source platform already applies — its four-stage OD identifier
recovery and three-stage coordinate matcher both record per-row method and confidence.
Carrying that into the warehouse rather than flattening it on the way in is the point.

**A known SCD candidate.** 
The 1,815 Gyeonggi stops carry 2,963 distinct names — ARS `05181` appears as both
`국가기록원성남분원.코이카` and `나라기록관.코이카`. That is a rename history, and the textbook
argument for a Type 2 slowly-changing dimension. This phase keeps `dim_stop` as Type 1 on the
most recent name and records the decision rather than pretending the question does not exist.

Stop names embed a five-digit sequence — `한림아파트(00020)` — the stop's ordinal position
along the route. `(route_no, terminal_seq)` is unique across all 38,941 route-stop pairs and
every stop name carries one, so it is available as a fallback key for coordinate interpolation
later.

---

## Load path

```
PostgreSQL  →  Parquet  →  bq load  →  transit_staging  →  transit_warehouse
                                       (1:1 source copy)   (dimensional model)
```

No GCS bucket: the sandbox has no billing account, so `bq load` reads the local Parquet file
directly.

The Arrow schema is pinned explicitly rather than inferred per batch: a batch where a column
happens to be all-null would infer a different type and be rejected by the writer.

Passenger counts are stored as `double precision` in Postgres but hold no fractional part
(verified: zero rows where `value <> floor(value)`), so they are narrowed to `INT64`.

Extraction of 5.58M rows takes ~27 s.

---

## Validation

Row counts, aggregate totals and distinct key counts, both sides:

| | PostgreSQL | BigQuery |
|---|---|---|
| Rows | 5,581,104 | 5,581,104 |
| Boardings | 793,161,784 | 793,161,784 |
| Alightings | 783,117,413 | 783,117,413 |
| Distinct stops | 12,529 | 12,529 |

Referential integrity holds in both directions: every `stop_key` in the fact table resolves in
`dim_stop` (zero orphans), and the three `match_method` populations sum to 12,529.

Per-partition counts also reconcile against the monthly counts in the source:

| Partition | Rows |
|---|---|
| 202501 | 930,720 |
| 202502 | 929,976 |
| 202503 | 930,600 |
| 202504 | 930,744 |
| 202505 | 931,128 |
| 202506 | 927,936 |

**One note on how the baseline was computed.** 
An initial boarding total of 793,288,854 came from a query that joined to the stop master. 
That join fans out on the Gyeonggi stops carrying multiple names, inflating the sum by 127,070. 
The correct baseline is measured on the fact table alone.
A validation query that joins is not validating the fact table.

**What this reconciliation does not prove.** The two sides agree exactly, but the source they
agree on was itself wrong. The upstream loader kept only one row per stop *visit* where a
route passes the same stop twice, understating monthly boarding by about 3.6%; the defect was
found and fixed in the platform repository after this load. So the figures above establish
that the transfer was faithful, not that the numbers were right. Those are different claims,
and a warehouse that only ever checks itself against its source can never distinguish them.
The reload described below is what closes the gap.

---

## Scan cost

| Query range | Bytes scanned |
|---|---|
| One month (`service_ym = 202503`) | 22.3 MB |
| Six months (`202501`–`202506`) | 133.9 MB |

Six-fold difference, exactly the partition count. Both are well under the table's 254 MB
logical size — column pruning reads only `hour` and `boardings`.

Free tier covers 10 GB storage and 1 TB of queries per month, so at this scale cost is zero.

---

## Reproducing

```bash
pip install -r requirements.txt
python -m unittest test_transit_bigquery      # offline; no PostgreSQL or GCP calls
```

**Staging** — 1:1 source copy into `transit_staging`:

```bash
python transit_bigquery.py export --out export_01   # prompts for the password
python transit_bigquery.py load  --folder export_01
bq query --use_legacy_sql=false < sql/04_staging_geometry_checks.sql
```

**Warehouse** — the dimensional model in `transit_warehouse`:

```bash
# 1. create tables
bq query --use_legacy_sql=false < sql/01_create_tables.sql

# 2. extract to Parquet
set PGPASSWORD=...
python extract.py

# 3. load
bq load --replace --source_format=PARQUET transit_warehouse.fact_boarding fact_boarding.parquet
bq load --replace --source_format=PARQUET transit_warehouse.dim_route    dim_route.parquet
bq load --replace --source_format=PARQUET transit_warehouse.dim_stop     dim_stop.parquet

# 4. add the GEOGRAPHY column
bq query --use_legacy_sql=false < sql/03_geography.sql

# 5. reconcile
bq query --use_legacy_sql=false < sql/02_validate.sql
```

`sql/03_geography.sql` rebuilds the dimension with a `GEOGRAPHY` point rather than issuing an
`UPDATE`: the sandbox does not support DML, and `CREATE OR REPLACE TABLE AS SELECT` is DDL.

`requirements.txt` pins `pyarrow>=22` and `psycopg2-binary>=2.9.12` — the first releases
publishing cp314 wheels. Older versions fall back to building from source on Python 3.14 and
fail without a local Arrow C++ toolchain.

---

## Repository layout

```
extract.py            Postgres -> Parquet for the dimensional model
transit_bigquery.py   Read-only snapshot export and idempotent staging load
test_transit_bigquery.py   Offline tests; no PostgreSQL or GCP calls
sql/01_create_tables.sql        Warehouse DDL with partitioning and clustering
sql/02_validate.sql             Reconciliation against the PostGIS source
sql/03_geography.sql            GEOGRAPHY column, added post-load
sql/04_staging_geometry_checks.sql   WKT round-trip check on staging
```

---

## Current scope

Loaded: 2025-01 through 2025-06, 5.58M rows — **extracted before the upstream loader fix**,
so these six months carry the understated boarding figures described in Validation above.
They are a working warehouse on stale data, not a current one.

The source now holds 2024-01 through 2026-08, about 30M rows, rebuilt and reconciled. Moving
to it is a full reload rather than an incremental extension: the six months already here have
to be replaced, not appended to.

Loaded and reconciled: `fact_boarding`, `dim_stop`, `dim_route`.

Not yet done: the 32-month reload; `dim_district` — the dong boundary table now holds the
1,208 administrative units of the 2025-06-30 snapshot, though it is still imported manually
with no loader script upstream; Airflow orchestration of the load; the Type 2 decision above;
and coordinate interpolation for the unmatched populations using `(route_no, terminal_seq)`.
