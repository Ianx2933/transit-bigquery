"""Load corrected OD days from Cloud Storage into BigQuery staging.

SeoulTransitPlatform's od-correct job writes one folder per day to
gs://seoul-transit-prod-pipeline/od-curated/YYYYMMDD/: curated.csv.gz,
anomaly.csv.gz and report.json, the report last. This script loads each day
into one partition of

    transit_staging.od_route_stop           (curated rows)
    transit_staging.od_route_stop_anomaly   (rows the correction set aside)

Both tables are partitioned by day on ingestion time, and a day is written to
its own partition (table$YYYYMMDD) with WRITE_TRUNCATE, so loading a day again
replaces it instead of adding to it. BigQuery reads the files from Cloud Storage
itself; nothing is downloaded but the reports.

A day is skipped when its partition already holds the row count and passenger
total its report gives. After loading, every partition is checked again
against its report, and every row's source date against the partition; any
difference stops the run.

Columns are renamed to English, as bus_stop_location is in transit_bigquery.py.
An empty CSV field is NULL, matching how the correction treated it.

    python od_load.py                       # every day not yet in BigQuery
    python od_load.py --only-day 20260915   # one day, reloaded
    python od_load.py --dry-run             # show the plan
"""
import argparse
import hashlib
import json
import os
import re
import sys
import uuid

PROJECT = 'project-9c77d932-f0d2-47bb-947'
DATASET = 'transit_staging'
LOCATION = 'asia-northeast3'
CURATED_URI = os.environ.get('OD_CURATED_URI', 'gs://seoul-transit-prod-pipeline/od-curated')
GCS_PROJECT = os.environ.get('GCS_PROJECT', 'seoul-transit-prod')
MAX_BYTES = 4 * 1024 ** 3

# CSV column order is od_structural_correction.COLUMNS.
COLUMNS = [
    ('service_date_raw', 'STRING'),   # 기준일자, YYMMDD
    ('route_name', 'STRING'),         # 노선명
    ('route_id', 'STRING'),           # 전환_노선ID
    ('board_seq', 'INT64'),           # 승차_정류장순번
    ('board_ars', 'STRING'),          # 승차_정류장ARS
    ('board_code', 'STRING'),         # 승차_정류장표준코드
    ('board_name', 'STRING'),         # 승차_정류장명
    ('alight_seq', 'INT64'),          # 하차_정류장순번
    ('alight_ars', 'STRING'),         # 하차_정류장ARS
    ('alight_code', 'STRING'),        # 하차_정류장표준코드
    ('alight_name', 'STRING'),        # 하차_정류장명
    ('passengers', 'INT64'),          # 승객수
]
TABLES = {
    'od_route_stop': ('curated.csv.gz', 'output'),
    'od_route_stop_anomaly': ('anomaly.csv.gz', 'anomaly'),
}
REPORT_NAME = re.compile(r'^(\d{8})/report\.json$')
# Staging tables are partitioned by day and NOT clustered. Writing a day with a
# $YYYYMMDD decorator into a clustered table fails with "Incompatible table
# partitioning specification" (2026-11-12), whether or not the load states the
# clustering. Clustering belongs on the dbt models that queries read.


def bigquery_module():
    from google.cloud import bigquery
    return bigquery


def split_uri(uri):
    if not uri.startswith('gs://'):
        raise ValueError('Expected gs://bucket/prefix, got ' + uri)
    bucket, _, prefix = uri[5:].partition('/')
    return bucket, prefix.strip('/')


def read_reports(storage_client, uri, only_day=None):
    """{day: (report dict, sha256 of its bytes)} for every finished day."""
    bucket, prefix = split_uri(uri)
    head = prefix + '/' if prefix else ''
    reports = {}
    for blob in storage_client.list_blobs(bucket, prefix=head):
        match = REPORT_NAME.match(blob.name[len(head):])
        if not match or (only_day and match.group(1) != only_day):
            continue
        data = blob.download_as_bytes()
        reports[match.group(1)] = (json.loads(data), hashlib.sha256(data).hexdigest())
    return reports


def expected(report, kind):
    s = report['structural']
    return int(s['rows'][kind]), int(s['passengers'][kind])


def ensure_tables(client, bq, project, dataset):
    """Create missing tables. A table with other settings is recreated only if empty."""
    schema = [bq.SchemaField(name, typ) for name, typ in COLUMNS]
    for table in TABLES:
        table_id = f'{project}.{dataset}.{table}'
        try:
            existing = client.get_table(table_id)
        except Exception as error:
            if type(error).__name__ != 'NotFound':
                raise
            existing = None
        if existing is not None:
            partitioned = existing.time_partitioning is not None and existing.time_partitioning.field is None
            if partitioned and not existing.clustering_fields:
                continue
            if existing.num_rows:
                raise ValueError(f'{table_id} has other partitioning or clustering and {existing.num_rows:,} rows; '
                                 'fix it by hand, it is not dropped automatically')
            client.delete_table(table_id)
            print(f'recreated empty {table_id} (partitioned by day, not clustered)', flush=True)
        t = bq.Table(table_id, schema=schema)
        t.time_partitioning = bq.TimePartitioning(type_=bq.TimePartitioningType.DAY)
        client.create_table(t)


def partition_stats(client, bq, project, dataset, table):
    """{YYYYMMDD: (rows, passengers, min source date, max source date)}"""
    sql = (f"SELECT FORMAT_DATE('%Y%m%d', _PARTITIONDATE) AS day, COUNT(*) AS n, "
           f"IFNULL(SUM(passengers), 0) AS p, MIN(service_date_raw) AS lo, MAX(service_date_raw) AS hi "
           f"FROM `{project}.{dataset}.{table}` GROUP BY day")
    cfg = bq.QueryJobConfig(maximum_bytes_billed=MAX_BYTES, use_legacy_sql=False)
    return {r['day']: (int(r['n']), int(r['p']), r['lo'], r['hi'])
            for r in (dict(x.items()) for x in client.query(sql, job_config=cfg).result())}


def load_day(client, bq, project, dataset, table, day, uri, sha):
    from_conflict = False
    # The table exists (ensure_tables); the $YYYYMMDD decorator picks the partition.
    cfg = bq.LoadJobConfig(
        source_format=bq.SourceFormat.CSV, skip_leading_rows=1, field_delimiter='|',
        encoding='UTF-8', schema=[bq.SchemaField(n, t) for n, t in COLUMNS],
        write_disposition=bq.WriteDisposition.WRITE_TRUNCATE)
    # One job ID per (table, day, report): a retry attaches to the first job
    # instead of loading twice. A first job that FAILED must not be reused: its
    # error would come back on every run (2026-11-12), so that case gets a new ID.
    target = f'{project}.{dataset}.{table}${day}'
    job_id = f'od_{table}_{day}_{sha[:16]}'
    try:
        job = client.load_table_from_uri(uri, target, job_id=job_id, job_config=cfg, location=LOCATION)
    except Exception as error:
        if type(error).__name__ != 'Conflict':
            raise
        job = client.get_job(job_id, location=LOCATION)
        if job.state == 'DONE' and job.error_result:
            job = client.load_table_from_uri(uri, target, job_id=f'{job_id}_{uuid.uuid4().hex[:8]}',
                                             job_config=cfg, location=LOCATION)
        else:
            from_conflict = True
    job.result()
    return from_conflict


def partition_ok(stats, day, report, kind):
    """Rows and passengers match the report and every row carries the partition's date.
    A day with no rows has no partition, so "missing" means 0 rows / 0 passengers.
    Counts alone are not enough: days loaded with an empty 기준일자 (2026-11-12) had
    the right counts and still had to be reloaded once od-correct was fixed."""
    n, p, lo, hi = stats.get(day, (0, 0, None, None))
    return (n, p) == expected(report, kind) and (not n or (lo, hi) == (day[2:], day[2:]))


def check(stats, reports, table, kind):
    problems = []
    for day, (report, _) in sorted(reports.items()):
        want_n, want_p = expected(report, kind)
        got = stats.get(day)
        if got is None:
            if want_n:
                problems.append(f'{table} {day}: missing, expected {want_n:,} rows')
            continue
        n, p, lo, hi = got
        if (n, p) != (want_n, want_p):
            problems.append(f'{table} {day}: {n:,} rows / {p:,} passengers, report says {want_n:,} / {want_p:,}')
        if n and (lo, hi) != (day[2:], day[2:]):
            problems.append(f'{table} {day}: source dates {lo}..{hi} in partition {day}')
    return problems


def run(storage_client, client, bq, args):
    reports = read_reports(storage_client, args.curated_uri, args.only_day)
    if not reports:
        raise ValueError('No report.json found under ' + args.curated_uri)
    bucket, prefix = split_uri(args.curated_uri)
    base = f'gs://{bucket}/{prefix}/' if prefix else f'gs://{bucket}/'
    if not args.dry_run:
        ensure_tables(client, bq, args.project, args.dataset)

    loaded = 0
    for table, (file_name, kind) in TABLES.items():
        stats = {} if args.dry_run and not args.check_existing else \
            partition_stats(client, bq, args.project, args.dataset, table)
        todo = [d for d in sorted(reports) if args.only_day or not partition_ok(stats, d, reports[d][0], kind)]
        print(f'{table}: {len(reports)} days reported, {len(reports) - len(todo)} already match, '
              f'{len(todo)} to load', flush=True)
        if args.dry_run:
            continue
        for day in todo:
            attached = load_day(client, bq, args.project, args.dataset, table, day,
                                base + f'{day}/{file_name}', reports[day][1])
            loaded += 1
            print(f'  LOAD {table}${day}' + (' (attached to earlier job)' if attached else ''), flush=True)
        problems = check(partition_stats(client, bq, args.project, args.dataset, table), reports, table, kind)
        if problems:
            raise ValueError('BigQuery does not match the reports:\n  ' + '\n  '.join(problems[:20]))
    if not args.dry_run:
        print(f'VERIFIED: {len(reports)} days in {", ".join(TABLES)} match their reports '
              f'(rows, passengers, source date); {loaded} partitions loaded this run.', flush=True)


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument('--curated-uri', default=CURATED_URI)
    p.add_argument('--project', default=os.environ.get('BQ_PROJECT', PROJECT))
    p.add_argument('--dataset', default=DATASET)
    p.add_argument('--only-day', default=None, help='YYYYMMDD; loaded even if it already matches')
    p.add_argument('--dry-run', action='store_true')
    p.add_argument('--check-existing', action='store_true', help='with --dry-run, query BigQuery for the plan')
    args = p.parse_args(argv)
    if args.only_day and not re.fullmatch(r'\d{8}', args.only_day):
        p.error('--only-day must be YYYYMMDD')

    from google.cloud import storage
    bq = bigquery_module()
    run(storage.Client(project=GCS_PROJECT), bq.Client(project=args.project, location=LOCATION), bq, args)


if __name__ == '__main__':
    try:
        main()
    except Exception as exc:
        print(f'STOP: {exc}', file=sys.stderr)
        sys.exit(1)
