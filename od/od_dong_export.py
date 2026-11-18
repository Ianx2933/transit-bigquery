"""Export the typical-day district OD for Cloud SQL from BigQuery to Cloud Storage.

dbt builds fct_od_dong_mode_typical (TOPIS district OD by mode, mean per day
type) and stg_stop_dong (gu and dong names to admin_dong_boundary codes). This
script attaches the codes, folds districts outside Seoul into 'outside', and
writes

    gs://seoul-transit-prod-pipeline/od-dong/<YYYYMMDD, export day>/part-*.csv.gz
    gs://seoul-transit-prod-pipeline/od-dong/<YYYYMMDD>/manifest.json   (last)

for SeoulTransitPlatform's pipelines/od_dong/load_od_dong.py (Cloud Run Job
transit-od-dong-load). The manifest holds rows and rides per day type and is
written last, so the loader can tell a complete export from an interrupted one.
When the newest complete export already holds the same numbers nothing is
written. A run stops before writing when fewer than MIN_SEOUL_ORIGINS Seoul
districts got a code (a renamed district would otherwise fall into 'outside').

    python od_dong_export.py
    python od_dong_export.py --dry-run
"""
import argparse
import datetime as dt
import json
import os
import re
import sys

PROJECT = 'project-9c77d932-f0d2-47bb-947'
DATASET = 'transit_marts'
LOCATION = 'asia-northeast3'
OUT_URI = os.environ.get('OD_DONG_URI', 'gs://seoul-transit-prod-pipeline/od-dong')
GCS_PROJECT = os.environ.get('GCS_PROJECT', 'seoul-transit-prod')
MAX_BYTES = 2 * 1024 ** 3
DAY_TYPES = ('weekday', 'saturday', 'sunday_holiday')
MIN_SEOUL_ORIGINS = 420          # 426 Seoul districts on 2026-11-17
COLUMNS = ['day_type', 'origin_cd', 'origin_name', 'dest_cd', 'dest_name',
           'subway_rides', 'bus_rides', 'total_rides']
FOLDER = re.compile(r'^(\d{8})/manifest\.json$')


def export_sql(project, dataset):
    """One row per (day type, origin code, destination code); ordered so exports compare."""
    ds = f'`{project}.{dataset}`'
    return f"""
WITH codes AS (
  SELECT gu, dong, ANY_VALUE(adm_cd) AS adm_cd
  FROM {ds}.stg_stop_dong
  GROUP BY gu, dong
)
SELECT
  f.day_type,
  COALESCE(o.adm_cd, 'outside') AS origin_cd,
  ANY_VALUE(IF(o.adm_cd IS NULL, 'outside', f.origin_dong)) AS origin_name,
  COALESCE(d.adm_cd, 'outside') AS dest_cd,
  ANY_VALUE(IF(d.adm_cd IS NULL, 'outside', f.dest_dong)) AS dest_name,
  SUM(IFNULL(f.subway_rides, 0)) AS subway_rides,
  SUM(IFNULL(f.bus_rides, 0)) AS bus_rides,
  SUM(IFNULL(f.total_rides, 0)) AS total_rides
FROM {ds}.fct_od_dong_mode_typical f
LEFT JOIN codes o ON o.gu = f.origin_gu AND o.dong = f.origin_dong
LEFT JOIN codes d ON d.gu = f.dest_gu AND d.dong = f.dest_dong
GROUP BY f.day_type, origin_cd, dest_cd
"""


def stats_sql(select_sql):
    return (f"SELECT day_type, COUNT(*) AS n, SUM(total_rides) AS r, "
            f"COUNT(DISTINCT IF(origin_cd = 'outside', NULL, origin_cd)) AS origins "
            f"FROM ({select_sql}) GROUP BY day_type ORDER BY day_type")


def check_stats(rows):
    """Manifest day types from stats rows; raises when a day type is missing or codes are short."""
    stats = {r['day_type']: {'rows': int(r['n']), 'total_rides': round(float(r['r']), 6),
                             'seoul_origins': int(r['origins'])} for r in rows}
    missing = [d for d in DAY_TYPES if d not in stats]
    if missing:
        raise ValueError(f'no {", ".join(missing)} in the district OD')
    short = {d: v['seoul_origins'] for d, v in stats.items() if v['seoul_origins'] < MIN_SEOUL_ORIGINS}
    if short:
        raise ValueError(f'too few Seoul districts got a code {short} (expected >= {MIN_SEOUL_ORIGINS}); '
                         f'check stg_stop_dong against the district names')
    return stats


def same_numbers(old_manifest, day_types):
    return old_manifest is not None and old_manifest.get('day_types') == day_types


def newest_manifest(storage_client, bucket_name, prefix):
    """(folder, manifest) of the newest complete export, or (None, None)."""
    p = f'{prefix}/' if prefix else ''
    folders = sorted(m.group(1) for m in (FOLDER.match(b.name[len(p):])
                                          for b in storage_client.list_blobs(bucket_name, prefix=p or None)) if m)
    if not folders:
        return None, None
    blob = storage_client.bucket(bucket_name).blob(f'{p}{folders[-1]}/manifest.json')
    return folders[-1], json.loads(blob.download_as_bytes())


def split_uri(uri):
    if not uri.startswith('gs://'):
        raise ValueError('Expected gs://bucket/prefix, got ' + uri)
    bucket, _, prefix = uri[5:].partition('/')
    return bucket, prefix.strip('/')


def run(storage_client, client, bq, args, today=None):
    select_sql = export_sql(args.project, args.dataset)
    cfg = bq.QueryJobConfig(maximum_bytes_billed=MAX_BYTES, use_legacy_sql=False)
    day_types = check_stats([dict(r.items()) for r in client.query(stats_sql(select_sql), job_config=cfg).result()])
    summary = ', '.join(f'{k} {v["rows"]:,} rows / {v["total_rides"]:,.1f} ({v["seoul_origins"]} districts)'
                        for k, v in day_types.items())
    print(f'{args.project}.{args.dataset}.fct_od_dong_mode_typical: {summary}', flush=True)

    bucket_name, prefix = split_uri(args.out_uri)
    last_folder, last = newest_manifest(storage_client, bucket_name, prefix)
    if same_numbers(last, day_types):
        print(f'gs://{bucket_name}/{prefix}/{last_folder}/ already holds these numbers; nothing to do.', flush=True)
        return
    if args.dry_run:
        return
    day = (today or dt.date.today()).strftime('%Y%m%d')
    folder = f'{prefix}/{day}' if prefix else day
    for blob in storage_client.list_blobs(bucket_name, prefix=f'{folder}/'):
        blob.delete()
    export = (f"EXPORT DATA OPTIONS(uri='gs://{bucket_name}/{folder}/part-*.csv.gz', format='CSV', "
              f"compression='GZIP', header=true, overwrite=true) AS "
              f"SELECT {', '.join(COLUMNS)} FROM ({select_sql}) ORDER BY day_type, origin_cd, dest_cd")
    client.query(export, job_config=cfg, location=LOCATION).result()
    parts = sorted(b.name.rsplit('/', 1)[1] for b in storage_client.list_blobs(bucket_name, prefix=f'{folder}/part-'))
    if not parts:
        raise ValueError('The export wrote no files')
    manifest = {'day_types': day_types, 'columns': COLUMNS, 'parts': parts,
                'source': f'{args.project}.{args.dataset}.fct_od_dong_mode_typical',
                'exported': day}
    storage_client.bucket(bucket_name).blob(f'{folder}/manifest.json').upload_from_string(
        json.dumps(manifest, ensure_ascii=False, indent=2), content_type='application/json')
    print(f'EXPORTED: gs://{bucket_name}/{folder}/ ({len(parts)} files, manifest last)', flush=True)


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument('--project', default=os.environ.get('BQ_PROJECT', PROJECT))
    p.add_argument('--dataset', default=os.environ.get('OD_DONG_DATASET', DATASET))
    p.add_argument('--out-uri', default=OUT_URI)
    p.add_argument('--dry-run', action='store_true')
    args = p.parse_args(argv)
    from google.cloud import bigquery, storage
    try:
        run(storage.Client(project=GCS_PROJECT), bigquery.Client(project=args.project), bigquery, args)
    except Exception as error:
        print(f'STOP: {error}', file=sys.stderr)
        return 1
    return 0


if __name__ == '__main__':
    sys.exit(main())
