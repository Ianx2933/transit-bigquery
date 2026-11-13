"""Export the typical-day OD for Cloud SQL from BigQuery to Cloud Storage.

dbt builds transit_marts.od_typical_route_stop_serving. This script extracts it to

    gs://seoul-transit-prod-pipeline/od-typical/<window_end YYYYMMDD>/part-*.csv.gz
    gs://seoul-transit-prod-pipeline/od-typical/<window_end YYYYMMDD>/manifest.json

The manifest is written last and holds rows and passengers per day type, so the
loader in SeoulTransitPlatform (pipelines/od_typical/load_typical_od.py) can tell
a complete export from an interrupted one and check what it loaded. A folder that
already has a manifest with the same numbers is left alone.

    python od_typical_export.py
    python od_typical_export.py --dry-run
"""
import argparse
import json
import os
import sys
import uuid

PROJECT = 'project-9c77d932-f0d2-47bb-947'
DATASET = 'transit_marts'
TABLE = 'od_typical_route_stop_serving'
LOCATION = 'asia-northeast3'
OUT_URI = os.environ.get('OD_TYPICAL_URI', 'gs://seoul-transit-prod-pipeline/od-typical')
GCS_PROJECT = os.environ.get('GCS_PROJECT', 'seoul-transit-prod')
MAX_BYTES = 2 * 1024 ** 3


def split_uri(uri):
    if not uri.startswith('gs://'):
        raise ValueError('Expected gs://bucket/prefix, got ' + uri)
    bucket, _, prefix = uri[5:].partition('/')
    return bucket, prefix.strip('/')


def table_stats(client, bq, table_id):
    sql = (f"SELECT day_type, COUNT(*) AS n, SUM(passengers) AS p, "
           f"FORMAT_DATE('%Y%m%d', MAX(window_end)) AS end_day, "
           f"FORMAT_DATE('%Y%m%d', MIN(window_start)) AS start_day "
           f"FROM `{table_id}` GROUP BY day_type ORDER BY day_type")
    cfg = bq.QueryJobConfig(maximum_bytes_billed=MAX_BYTES, use_legacy_sql=False)
    rows = [dict(r.items()) for r in client.query(sql, job_config=cfg).result()]
    if not rows:
        raise ValueError(f'{table_id} is empty')
    ends = {r['end_day'] for r in rows}
    starts = {r['start_day'] for r in rows}
    if len(ends) != 1 or len(starts) != 1:
        raise ValueError(f'{table_id}: day types disagree on the window {sorted(starts)} .. {sorted(ends)}')
    return {
        'window_start': starts.pop(),
        'window_end': ends.pop(),
        'day_types': {r['day_type']: {'rows': int(r['n']), 'passengers': round(float(r['p']), 6)} for r in rows},
    }


def run(storage_client, client, bq, args):
    table_id = f'{args.project}.{args.dataset}.{TABLE}'
    stats = table_stats(client, bq, table_id)
    bucket_name, prefix = split_uri(args.out_uri)
    folder = f'{prefix}/{stats["window_end"]}' if prefix else stats['window_end']
    bucket = storage_client.bucket(bucket_name)
    manifest_blob = bucket.blob(f'{folder}/manifest.json')
    summary = ', '.join(f'{k} {v["rows"]:,} rows / {v["passengers"]:,.1f}' for k, v in stats['day_types'].items())
    print(f'{table_id}: window {stats["window_start"]}..{stats["window_end"]}; {summary}', flush=True)

    if manifest_blob.exists():
        old = json.loads(manifest_blob.download_as_bytes())
        if old.get('day_types') == stats['day_types'] and old.get('window_start') == stats['window_start']:
            print(f'gs://{bucket_name}/{folder}/ already holds this export; nothing to do.', flush=True)
            return
        if not args.replace:
            raise ValueError(f'gs://{bucket_name}/{folder}/manifest.json exists with other numbers; '
                             f'use --replace to export again')
    if args.dry_run:
        return
    if manifest_blob.exists():
        manifest_blob.delete()
    for blob in storage_client.list_blobs(bucket_name, prefix=f'{folder}/part-'):
        blob.delete()

    job_config = bq.ExtractJobConfig(destination_format=bq.DestinationFormat.CSV,
                                     compression=bq.Compression.GZIP, print_header=True)
    job = client.extract_table(table_id, f'gs://{bucket_name}/{folder}/part-*.csv.gz',
                               job_id=f'od_typical_export_{stats["window_end"]}_{uuid.uuid4().hex[:8]}',
                               job_config=job_config, location=LOCATION)
    job.result()
    parts = sorted(b.name.rsplit('/', 1)[1] for b in storage_client.list_blobs(bucket_name, prefix=f'{folder}/part-'))
    if not parts:
        raise ValueError('The extract job wrote no files')
    manifest = dict(stats, source=table_id, parts=parts,
                    columns=['day_type', 'label', 'mode', 'route_name', 'route_id', 'board_seq', 'board_key',
                             'board_code', 'board_name', 'alight_seq', 'alight_key', 'alight_code',
                             'alight_name', 'passengers', 'n_days', 'days_seen', 'window_start', 'window_end'])
    manifest_blob.upload_from_string(json.dumps(manifest, ensure_ascii=False, indent=2),
                                     content_type='application/json')
    print(f'EXPORTED: gs://{bucket_name}/{folder}/ ({len(parts)} files, manifest last)', flush=True)


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument('--project', default=os.environ.get('BQ_PROJECT', PROJECT))
    p.add_argument('--dataset', default=os.environ.get('OD_TYPICAL_DATASET', DATASET))
    p.add_argument('--out-uri', default=OUT_URI)
    p.add_argument('--replace', action='store_true', help='export again over a different manifest')
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
