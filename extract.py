"""
Seoul_Transit (PostgreSQL) -> Parquet -> BigQuery

Usage:
    set PGPASSWORD=...
    python extract.py

Environment: PGHOST PGPORT PGDATABASE PGUSER PGPASSWORD
"""

import os
import sys
import time

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from sqlalchemy import create_engine

HOST = os.environ.get("PGHOST", "localhost")
PORT = os.environ.get("PGPORT", "5432")
DB = os.environ.get("PGDATABASE", "Seoul_Transit")
USER = os.environ.get("PGUSER", "postgres")
PWD = os.environ.get("PGPASSWORD")

if not PWD:
    sys.exit("PGPASSWORD is not set.  Run:  set PGPASSWORD=...")

engine = create_engine(f"postgresql+psycopg2://{USER}:{PWD}@{HOST}:{PORT}/{DB}")

# Rows per batch. 5.58M rows will not fit comfortably in memory at once.
CHUNK = 500_000

# service_ym (INT64) is the partition key, not service_month.
#
# The sandbox forces a 60-day partition expiration that cannot be disabled,
# and partition expiry is measured from the partition's own date. Day-based
# partitioning on 2025 data therefore expires on arrival: the load job
# reports DONE and the table holds zero rows, with no error anywhere.
# RANGE_BUCKET on an integer sidesteps that rule entirely. Pruning and
# require_partition_filter behave the same, and since the source is a
# monthly aggregate a day-grain partition was never the right key.
#
# service_month is kept as a readable DATE for analysts. It is not the
# partition key.
FACT_SQL = """
SELECT
    use_ym::int                           AS service_ym,
    to_date(use_ym || '01', 'YYYYMMDD')  AS service_month,
    hour                                  AS hour,
    route_no                              AS route_no,
    stop_ars                              AS stop_key,
    boarding_passengers                   AS boardings,
    alighting_passengers                  AS alightings
FROM hourly_bus_stop_passenger
"""

FACT_SCHEMA = pa.schema([
    ("service_ym",    pa.int64()),
    ("service_month", pa.date32()),
    ("hour",          pa.int64()),
    ("route_no",      pa.string()),
    ("stop_key",      pa.string()),
    ("boardings",     pa.int64()),
    ("alightings",    pa.int64()),
])

# route_name is not unique per route_no in the source, so collapse to one
# row per route. NULLS LAST keeps a real name over a missing one.
ROUTE_SQL = """
SELECT DISTINCT ON (route_no)
    route_no,
    route_name
FROM hourly_bus_stop_passenger
ORDER BY route_no, route_name NULLS LAST
"""

ROUTE_SCHEMA = pa.schema([
    ("route_no",   pa.string()),
    ("route_name", pa.string()),
])

# dim_stop is keyed on stop_ars to match the fact table's stop_key.
#
# Three populations, recorded in match_method rather than filtered away:
#   ars_exact          -> matched to the Seoul stop master
#   gyeonggi_unmatched -> Gyeonggi-do stops on cross-boundary routes,
#                         outside the Seoul master's scope
#   virtual_terminal   -> route origin/terminus markers, ARS is literally '~'
#
# The unmatched rows are 4.5% of total boardings. They are kept so aggregate
# totals reconcile against the source, and so an analyst doing spatial work
# excludes them explicitly rather than inheriting a filtered universe.
#
# Stop names embed a five-digit ordinal along the route -- 한림아파트(00020).
# It is parsed into terminal_seq as a fallback key for Phase 2.
DIM_STOP_SQL = """
SELECT DISTINCT ON (h.stop_ars)
    h.stop_ars                                   AS stop_key,
    h.stop_ars                                   AS stop_ars,
    h.stop_id                                    AS stop_id,
    l."\ub178\ub4dcid"                              AS node_id,
    h.stop_name                                  AS stop_name,
    CASE WHEN h.stop_ars = '~'                  THEN 'virtual_terminal'
         WHEN l."\uc815\ub958\uc7a5\ubc88\ud638" IS NOT NULL THEN 'ars_exact'
         ELSE 'gyeonggi_unmatched' END           AS match_method,
    nullif(substring(h.stop_name from '\\((\\d{5})\\)$'), '')::int AS terminal_seq,
    l."\uacbd\ub3c4"                                 AS lon,
    l."\uc704\ub3c4"                                 AS lat,
    t.elev_m                                     AS elev_m,
    t.slope_pct                                  AS slope_pct,
    t.dataset_id                                 AS dataset_id
FROM hourly_bus_stop_passenger h
LEFT JOIN bus_stop_location      l ON l."\uc815\ub958\uc7a5\ubc88\ud638" = h.stop_ars
LEFT JOIN bus_stop_terrain_stats t ON t.node_id = l."\ub178\ub4dcid"
ORDER BY h.stop_ars, l."\uc815\ub958\uc7a5\ubc88\ud638" NULLS LAST
"""

DIM_STOP_SCHEMA = pa.schema([
    ("stop_key",     pa.string()),
    ("stop_ars",     pa.string()),
    ("stop_id",      pa.string()),
    ("node_id",      pa.string()),
    ("stop_name",    pa.string()),
    ("match_method", pa.string()),
    ("terminal_seq", pa.int64()),
    ("lon",          pa.float64()),
    ("lat",          pa.float64()),
    ("elev_m",       pa.float64()),
    ("slope_pct",    pa.float64()),
    ("dataset_id",   pa.string()),
])


def cast_dim_stop(df: pd.DataFrame) -> pd.DataFrame:
    """terminal_seq comes back as float when any row is null. Pin it to
    nullable Int64 so the Arrow schema cast does not silently drop it."""
    df["terminal_seq"] = df["terminal_seq"].astype("Int64")
    return df


def cast_fact(df: pd.DataFrame) -> pd.DataFrame:
    """Passenger counts are stored as double precision but hold no fractional
    part (verified: zero rows where value <> floor(value)). Narrow them to
    nullable Int64 so BigQuery gets INT64 and scans less."""
    for col in ("boardings", "alightings"):
        df[col] = df[col].round().astype("Int64")
    df["hour"] = df["hour"].astype("Int64")
    df["service_ym"] = df["service_ym"].astype("Int64")
    return df


def extract_chunked(sql, schema, path, transform=None):
    """Stream a large query to Parquet batch by batch.

    The schema is pinned explicitly rather than inferred per batch: a batch
    where a column happens to be all-null would otherwise infer a different
    type and the writer would reject it."""
    writer = None
    total = 0
    t0 = time.time()
    try:
        for chunk in pd.read_sql(sql, engine, chunksize=CHUNK):
            if transform is not None:
                chunk = transform(chunk)
            table = pa.Table.from_pandas(chunk, schema=schema, preserve_index=False)
            if writer is None:
                writer = pq.ParquetWriter(path, schema, compression="snappy")
            writer.write_table(table)
            total += len(chunk)
            print(f"  {total:,} rows", flush=True)
    finally:
        if writer is not None:
            writer.close()
    print(f"{path}: {total:,} rows in {time.time() - t0:.1f}s")
    return total


def extract_small(sql, schema, path, transform=None):
    """Single-shot extract for dimension tables that fit in memory."""
    df = pd.read_sql(sql, engine)
    if transform is not None:
        df = transform(df)
    table = pa.Table.from_pandas(df, schema=schema, preserve_index=False)
    pq.write_table(table, path, compression="snappy")
    print(f"{path}: {len(df):,} rows")
    return len(df)


if __name__ == "__main__":
    print("fact_boarding ...")
    n_fact = extract_chunked(
        FACT_SQL, FACT_SCHEMA, "fact_boarding.parquet", transform=cast_fact
    )

    print("dim_route ...")
    n_route = extract_small(ROUTE_SQL, ROUTE_SCHEMA, "dim_route.parquet")

    print("dim_stop ...")
    n_stop = extract_small(
        DIM_STOP_SQL, DIM_STOP_SCHEMA, "dim_stop.parquet", transform=cast_dim_stop
    )

    # Reconcile against the source before loading. Expected values come from
    # the monthly counts in Postgres (2025-01 .. 2025-06).
    print()
    print("Reconciliation")
    print(f"  fact_boarding  {n_fact:,}   (expected 5,581,104)")
    print(f"  dim_route      {n_route:,}")
    print(f"  dim_stop       {n_stop:,}   (expected 12,529)")
