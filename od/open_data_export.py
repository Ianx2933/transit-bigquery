"""Publish the open data files: BigQuery tables and gzip CSV in a public bucket.

After dbt builds transit_marts, this script writes five tables to the dataset
transit_open (overwritten each week) and extracts each to one gzip CSV:

    gs://seoul-transit-open-data/releases/<window_end YYYYMMDD>/<name>.csv.gz
    gs://seoul-transit-open-data/releases/<window_end YYYYMMDD>/manifest.json
    gs://seoul-transit-open-data/latest/...   (a copy of the newest release)

The manifest is written last and lists, per file, rows, bytes, columns and their
meaning; the website reads latest/manifest.json for its download page. A
release whose manifest already holds the same row counts is left alone, so the
weekly run does nothing until the representative OD changes.

Sources are public data under KOGL Type 1 (공공누리 제1유형: attribution); the
manifest carries the attribution text.

    python open_data_export.py
    python open_data_export.py --dry-run   # checks each SQL (BigQuery dry run, no cost), writes nothing
"""
import argparse
import gzip
import json
import os
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import od_dong_export  # noqa: E402
import od_typical_export  # noqa: E402

PROJECT = 'project-9c77d932-f0d2-47bb-947'
SOURCE_DATASET = 'transit_marts'
OPEN_DATASET = os.environ.get('OPEN_DATASET', 'transit_open')
LOCATION = 'asia-northeast3'
OUT_URI = os.environ.get('OPEN_DATA_URI', 'gs://seoul-transit-open-data')
GCS_PROJECT = os.environ.get('GCS_PROJECT', 'seoul-transit-prod')
MAX_BYTES = 5 * 1024 ** 3

ATTRIBUTION = {
    'ko': '원천: 서울특별시 대중교통 이용 자료(서울 열린데이터광장, 공공데이터포털; 공공누리 제1유형). '
          '이 파일은 Seoul Transit Platform이 서울특별시 자료를 가공한 것이며 서울특별시 공식 자료가 아닙니다.',
    'en': 'Source: Seoul Metropolitan Government public transport data (Seoul Open Data Plaza, data.go.kr; '
          'KOGL Type 1, attribution). Processed by Seoul Transit Platform from Seoul Metropolitan '
          'Government data; not an official Seoul Metropolitan Government release.',
}


def datasets(project, source):
    """(name, SQL, description, column notes) for each published table, in page order."""
    ds = f'`{project}.{source}`'
    return [
        ('route_stop_od_typical', f"""
SELECT day_type, mode, route_name,
       board_seq, board_key, board_name,
       alight_seq, alight_key, alight_name,
       ROUND(passengers, 3) AS passengers, n_days, window_start, window_end
FROM {ds}.od_typical_route_stop_serving""",
         {'ko': '대표일 노선별 정류소 OD: 요일 유형마다 최근 12개월 같은 유형의 날 평균 승객(하루)',
          'en': 'Representative-day route stop OD: mean riders a day over the days of each type in the last twelve months'},
         {'day_type': 'weekday | saturday | sunday_holiday', 'mode': 'bus | ferry (Han River ferry)',
          'board_seq': 'stop order on the route', 'passengers': 'mean riders a day',
          'n_days': 'days averaged', 'window_start': 'first day of the window', 'window_end': 'last day'}),
        ('district_od_typical', od_dong_export.export_sql(project, source),
         {'ko': '대표일 행정동 OD: 출발·도착 행정동별 지하철·버스 하루 승객(설·추석 제외 12개월 평균)',
          'en': 'Representative-day district (행정동) OD: rail and bus riders a day by origin and destination'},
         {'origin_cd': '8-digit 행정동 code, or outside (outside Seoul)',
          'subway_rides': 'mean riders a day by rail', 'bus_rides': 'mean riders a day by bus'}),
        ('stop_hourly_profile', f"""
SELECT day_type, route_no AS route_name, stop_key, hour, ROUND(ratio, 6) AS ratio, method
FROM {ds}.hourly_profile""",
         {'ko': '정류소 시간대 승차 비율: 노선·정류소·요일 유형마다 24시간 비율(합 1), 34개월 승차 자료로 추정',
          'en': 'Hourly boarding profile: share of a day\'s boardings in each hour by route, stop and day type (sums to 1)'},
         {'ratio': 'share of the day boarding in this hour', 'method': 'how the profile was obtained'}),
        ('stops', f"""
SELECT stop_key, stop_name,
       ROUND(ST_Y(geog), 6) AS lat, ROUND(ST_X(geog), 6) AS lng, match_method
FROM {ds}.dim_stop""",
         {'ko': '정류소 표: 정류소 키, 이름, 좌표(WGS84)',
          'en': 'Stops: stop key, name and position (WGS84)'},
         {'match_method': 'how the position was matched; empty lat/lng when none'}),
        ('typical_accuracy', f"""
SELECT * FROM {ds}.od_typical_accuracy""",
         {'ko': '대표일 정확도: 하루씩 빼고 나머지 날 평균으로 그날을 맞춰 본 오차(WAPE)',
          'en': 'Representative-day accuracy: leave-one-day-out WAPE by day, mode and level'},
         {'wape': 'weighted absolute percentage error', 'level': 'od_pair | route_stop | stop | route'}),
    ]


def split_uri(uri):
    if not uri.startswith('gs://'):
        raise ValueError('Expected gs://bucket[/prefix], got ' + uri)
    bucket, _, prefix = uri[5:].partition('/')
    return bucket, prefix.strip('/')


def same_rows(old_manifest, rows_by_name):
    return {f['name']: f['rows'] for f in old_manifest.get('files', [])} == rows_by_name


def compose_all(bucket, sources, target):
    """Join objects in order into target (at most 32 per compose call)."""
    while len(sources) > 32:
        joined = []
        for i in range(0, len(sources), 32):
            part = bucket.blob(f'{target}.join{i // 32:04d}')
            part.compose(sources[i:i + 32])
            joined.append(part)
        for blob in sources:
            if blob.name != target:
                blob.delete()
        sources = joined
    out = bucket.blob(target)
    out.compose(sources)
    for blob in sources:
        if blob.name.startswith(f'{target}.join'):
            blob.delete()
    return out


def write_file(storage_client, bucket, client, bq, bucket_name, folder, table, release):
    """One gzip CSV per table: a gzip header plus the extract shards (gzip members concatenate)."""
    name = table['name']
    tmp = f'{folder}/_tmp/{name}'
    for blob in storage_client.list_blobs(bucket_name, prefix=f'{tmp}/'):
        blob.delete()
    header = bucket.blob(f'{tmp}/000-header.csv.gz')
    header.upload_from_string(gzip.compress((','.join(table['columns']) + '\n').encode('utf-8')),
                              content_type='application/gzip')
    cfg = bq.ExtractJobConfig(destination_format=bq.DestinationFormat.CSV,
                              compression=bq.Compression.GZIP, print_header=False)
    client.extract_table(table['table'], f'gs://{bucket_name}/{tmp}/part-*.csv.gz',
                         job_id=f'open_data_{name}_{release}_{uuid.uuid4().hex[:8]}',
                         job_config=cfg, location=LOCATION).result()
    shards = sorted(storage_client.list_blobs(bucket_name, prefix=f'{tmp}/part-'), key=lambda b: b.name)
    out = compose_all(bucket, [header] + shards, f'{folder}/{name}.csv.gz')
    out.content_type = 'application/gzip'
    out.content_disposition = f'attachment; filename="{name}_{release}.csv.gz"'
    out.patch()
    for blob in storage_client.list_blobs(bucket_name, prefix=f'{tmp}/'):
        blob.delete()
    out.reload()
    return int(out.size) if out.size is not None else None


def run(storage_client, client, bq, args, now=None):
    now = now or datetime.now(timezone.utc)
    window = od_typical_export.table_stats(
        client, bq, f'{args.project}.{args.source}.{od_typical_export.TABLE}')
    release = window['window_end']
    bucket_name, prefix = split_uri(args.out_uri)
    base = f'{prefix}/' if prefix else ''
    folder = f'{base}releases/{release}'
    bucket = storage_client.bucket(bucket_name)
    print(f'release {release} (window {window["window_start"]}..{window["window_end"]})', flush=True)

    # 1. Tables in the open dataset.
    tables = []
    for name, sql, description, notes in datasets(args.project, args.source):
        table_id = f'{args.project}.{args.open_dataset}.{name}'
        if args.dry_run:
            cfg = bq.QueryJobConfig(dry_run=True, use_query_cache=False, use_legacy_sql=False)
            job = client.query(sql, job_config=cfg, location=LOCATION)
            print(f'would write {table_id}: SQL ok, {int(job.total_bytes_processed or 0) / 1e6:,.1f} MB to scan',
                  flush=True)
            continue
        cfg = bq.QueryJobConfig(destination=table_id, write_disposition='WRITE_TRUNCATE',
                                maximum_bytes_billed=MAX_BYTES, use_legacy_sql=False)
        client.query(sql, job_config=cfg, location=LOCATION).result()
        table = client.get_table(table_id)
        columns = [f.name for f in table.schema]
        tables.append({'name': name, 'table': table_id, 'rows': int(table.num_rows), 'columns': columns,
                       'description': description, 'notes': notes})
        print(f'{table_id}: {table.num_rows:,} rows', flush=True)
    if args.dry_run:
        return

    # 2. Files, unless this release already holds the same rows. When only the
    #    text (attribution, descriptions) changed, the manifest alone is rewritten.
    manifest_blob = bucket.blob(f'{folder}/manifest.json')
    rows_by_name = {t['name']: t['rows'] for t in tables}
    old = json.loads(manifest_blob.download_as_bytes()) if manifest_blob.exists() else None

    def manifest_for(sizes, generated_at):
        files = [{k: t[k] for k in ('name', 'rows', 'columns', 'description', 'notes')}
                 | {'file': f'{t["name"]}.csv.gz', 'bytes': sizes.get(t['name'])} for t in tables]
        return {
            'release': release,
            'window_start': window['window_start'],
            'window_end': window['window_end'],
            'generated_at': generated_at,
            'license': 'KOGL Type 1 (공공누리 제1유형): attribution',
            'attribution': ATTRIBUTION,
            'bigquery_dataset': f'{args.project}.{args.open_dataset}',
            'files': files,
        }

    def upload(manifest):
        manifest_blob.upload_from_string(json.dumps(manifest, ensure_ascii=False, indent=2),
                                         content_type='application/json')

    if old is not None and same_rows(old, rows_by_name):
        sizes = {f['name']: f.get('bytes') for f in old.get('files', [])}
        manifest = manifest_for(sizes, old.get('generated_at'))
        if manifest == old:
            print(f'gs://{bucket_name}/{folder}/ already holds these rows; files left as they are.', flush=True)
        else:
            upload(manifest)
            print(f'gs://{bucket_name}/{folder}/ same rows; manifest text updated, files left as they are.',
                  flush=True)
    else:
        if old is not None:
            manifest_blob.delete()
        sizes = {t['name']: write_file(storage_client, bucket, client, bq, bucket_name, folder, t, release)
                 for t in tables}
        upload(manifest_for(sizes, now.strftime('%Y-%m-%dT%H:%M:%SZ')))
        print(f'EXPORTED: gs://{bucket_name}/{folder}/ ({len(tables)} files, manifest last)', flush=True)

    # 3. latest/ = this release (manifest last, stale files removed).
    latest = f'{base}latest'
    names = [b.name for b in storage_client.list_blobs(bucket_name, prefix=f'{folder}/')]
    keep = set()
    for name in sorted(names, key=lambda n: n.endswith('manifest.json')):
        target = f'{latest}/{name.rsplit("/", 1)[1]}'
        bucket.copy_blob(bucket.blob(name), bucket, target)
        keep.add(target)
    for blob in storage_client.list_blobs(bucket_name, prefix=f'{latest}/'):
        if blob.name not in keep:
            blob.delete()
    print(f'gs://{bucket_name}/{latest}/ = release {release}', flush=True)


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument('--project', default=os.environ.get('BQ_PROJECT', PROJECT))
    p.add_argument('--source', default=os.environ.get('OPEN_SOURCE_DATASET', SOURCE_DATASET))
    p.add_argument('--open-dataset', default=OPEN_DATASET)
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
