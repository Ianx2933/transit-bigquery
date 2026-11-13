"""Load TOPIS daily district mode OD (동별_수단OD) into BigQuery staging.

Each monthly TOPIS archive that SeoulTransitPlatform's od-collect job stored
(gs://seoul-transit-prod-pipeline/od-raw/YYYY-MM/<zip> + manifest.json) holds
one 동별/수단OD/동별_수단OD_YYYYMMDD.csv per day of the month: dong to dong,
with total, subway and bus passengers counted per ride (a transfer is two
rides), cp949. The bus column is what the stop OD summed to dongs should give.

This script reads each month's zip, decodes the files to UTF-8 and loads the
month into one partition of

    transit_staging.od_dong_mode    partitioned by month on service_date

with WRITE_TRUNCATE (table$YYYYMM), so loading a month again replaces it.
BigQuery only reads UTF-8 or Latin-1, so the bytes go through this script.

The script counts rows and sums the three passenger columns per day while
decoding. A month whose partition already holds those numbers is skipped; after
loading, every loaded month is checked again and any difference stops the run.

    python od_dong_load.py                    # every stored month
    python od_dong_load.py --only-ym 202609   # one month, reloaded
    python od_dong_load.py --dry-run          # compare only, load nothing
"""
import argparse
import csv
import gzip
import io
import json
import os
import re
import sys
import uuid
import zipfile

PROJECT = 'project-9c77d932-f0d2-47bb-947'
DATASET = 'transit_staging'
TABLE = 'od_dong_mode'
LOCATION = 'asia-northeast3'
RAW_URI = os.environ.get('OD_RAW_URI', 'gs://seoul-transit-prod-pipeline/od-raw')
GCS_PROJECT = os.environ.get('GCS_PROJECT', 'seoul-transit-prod')
MAX_BYTES = 2 * 1024 ** 3

HEADER = ['기준일자', '출발_구', '출발_동', '도착_구', '도착_동', '총_승객수', '지하철_승객수', '버스_승객수']
COLUMNS = [
    ('service_date', 'DATE'),
    ('origin_gu', 'STRING'),
    ('origin_dong', 'STRING'),
    ('dest_gu', 'STRING'),
    ('dest_dong', 'STRING'),
    ('total_passengers', 'INT64'),
    ('subway_passengers', 'INT64'),
    ('bus_passengers', 'INT64'),
]
MEMBER = re.compile(r'^동별/수단OD/동별_수단OD_(\d{8})\.csv$')
MONTH_FOLDER = re.compile(r'^(\d{4})-(\d{2})/manifest\.json$')


def split_uri(uri):
    if not uri.startswith('gs://'):
        raise ValueError('Expected gs://bucket/prefix, got ' + uri)
    bucket, _, prefix = uri[5:].partition('/')
    return bucket, prefix.strip('/')


def member_name(info):
    """Zip names without the UTF-8 flag are cp949 bytes read as cp437."""
    if info.flag_bits & 0x800:
        return info.filename
    try:
        return info.filename.encode('cp437').decode('cp949')
    except UnicodeError:
        return info.filename


def read_manifests(storage_client, raw_uri, only_ym=None):
    """{YYYYMM: manifest} for every month with a manifest (= complete upload)."""
    bucket, prefix = split_uri(raw_uri)
    out = {}
    for blob in storage_client.list_blobs(bucket, prefix=f'{prefix}/' if prefix else None):
        rel = blob.name[len(prefix) + 1:] if prefix else blob.name
        m = MONTH_FOLDER.match(rel)
        if not m:
            continue
        ym = m.group(1) + m.group(2)
        if only_ym and ym != only_ym:
            continue
        out[ym] = json.loads(blob.download_as_bytes())
    return out


def convert_month(zip_bytes, ym):
    """Return (gzipped UTF-8 CSV bytes, {YYYY-MM-DD: (rows, total, subway, bus)})."""
    stats = {}
    buf = io.BytesIO()
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf, \
            gzip.GzipFile(fileobj=buf, mode='wb', mtime=0) as gz:
        text_out = io.TextIOWrapper(gz, encoding='utf-8', newline='')
        writer = csv.writer(text_out, lineterminator='\n')
        writer.writerow([name for name, _ in COLUMNS])
        members = []
        for info in zf.infolist():
            match = MEMBER.match(member_name(info))
            if match:
                members.append((match.group(1), info))
        for day, info in sorted(members, key=lambda m: m[0]):
            if day[:6] != ym:
                raise ValueError(f'{info.filename}: day {day} outside month {ym}')
            iso = f'{day[:4]}-{day[4:6]}-{day[6:]}'
            text = zf.read(info).decode('cp949')
            reader = csv.reader(io.StringIO(text))
            header = next(reader)
            if [h.strip() for h in header] != HEADER:
                raise ValueError(f'{info.filename}: unexpected header {header}')
            n = tot = sub = bus = 0
            for row in reader:
                if not row:
                    continue
                if row[0] != day:
                    raise ValueError(f'{info.filename}: row dated {row[0]}')
                t, s, b = int(row[5]), int(row[6]), int(row[7])
                writer.writerow([iso, row[1], row[2], row[3], row[4], t, s, b])
                n, tot, sub, bus = n + 1, tot + t, sub + s, bus + b
            stats[iso] = (n, tot, sub, bus)
        text_out.flush()
        text_out.detach()
    if not stats:
        raise ValueError(f'{ym}: no 동별_수단OD files in the archive')
    return buf.getvalue(), stats


def ensure_table(client, bq, project, dataset):
    table_id = f'{project}.{dataset}.{TABLE}'
    try:
        client.get_table(table_id)
        return
    except Exception as error:
        if type(error).__name__ != 'NotFound':
            raise
    table = bq.Table(table_id, schema=[bq.SchemaField(n, t) for n, t in COLUMNS])
    table.time_partitioning = bq.TimePartitioning(type_=bq.TimePartitioningType.MONTH, field='service_date')
    client.create_table(table)
    print(f'created {table_id}', flush=True)


def month_stats(client, bq, project, dataset, ym):
    sql = (f"SELECT CAST(service_date AS STRING) AS day, COUNT(*) AS n, SUM(total_passengers) AS t, "
           f"SUM(subway_passengers) AS s, SUM(bus_passengers) AS b "
           f"FROM `{project}.{dataset}.{TABLE}` "
           f"WHERE service_date BETWEEN DATE '{ym[:4]}-{ym[4:]}-01' "
           f"AND LAST_DAY(DATE '{ym[:4]}-{ym[4:]}-01') GROUP BY day")
    cfg = bq.QueryJobConfig(maximum_bytes_billed=MAX_BYTES, use_legacy_sql=False)
    rows = client.query(sql, job_config=cfg).result()
    return {r['day']: (int(r['n']), int(r['t']), int(r['s']), int(r['b']))
            for r in (dict(x.items()) for x in rows)}


def load_month(client, bq, project, dataset, ym, data):
    cfg = bq.LoadJobConfig(
        source_format=bq.SourceFormat.CSV, skip_leading_rows=1, encoding='UTF-8',
        schema=[bq.SchemaField(n, t) for n, t in COLUMNS],
        write_disposition=bq.WriteDisposition.WRITE_TRUNCATE)
    job = client.load_table_from_file(
        io.BytesIO(data), f'{project}.{dataset}.{TABLE}${ym}',
        job_id=f'od_dong_mode_{ym}_{uuid.uuid4().hex[:8]}', job_config=cfg, location=LOCATION)
    job.result()


def run(storage_client, client, bq, args):
    manifests = read_manifests(storage_client, args.raw_uri, args.only_ym)
    if not manifests:
        raise ValueError('No manifest.json found under ' + args.raw_uri)
    if not args.dry_run:
        ensure_table(client, bq, args.project, args.dataset)
    loaded, checked, problems = 0, 0, []
    for ym in sorted(manifests):
        bucket, name = split_uri(manifests[ym]['object'])
        zip_bytes = storage_client.bucket(bucket).blob(name).download_as_bytes()
        data, want = convert_month(zip_bytes, ym)
        have = {} if args.dry_run and not args.check_existing else \
            month_stats(client, bq, args.project, args.dataset, ym)
        same = have == want
        print(f'{ym}: {len(want)} days, {sum(v[0] for v in want.values()):,} rows, bus '
              f'{sum(v[3] for v in want.values()):,}' + (' (already loaded)' if same else ''), flush=True)
        if args.dry_run or (same and not args.only_ym):
            continue
        load_month(client, bq, args.project, args.dataset, ym, data)
        loaded += 1
        got = month_stats(client, bq, args.project, args.dataset, ym)
        checked += 1
        for day in sorted(set(want) | set(got)):
            if want.get(day) != got.get(day):
                problems.append(f'{day}: file {want.get(day)}, BigQuery {got.get(day)}')
    if problems:
        raise ValueError('BigQuery does not match the files:\n  ' + '\n  '.join(problems[:20]))
    if not args.dry_run:
        print(f'VERIFIED: {len(manifests)} months; {loaded} loaded and checked per day '
              f'(rows, total, subway, bus).', flush=True)


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument('--raw-uri', default=RAW_URI)
    p.add_argument('--project', default=os.environ.get('BQ_PROJECT', PROJECT))
    p.add_argument('--dataset', default=DATASET)
    p.add_argument('--only-ym', help='YYYYMM, reloaded even if it matches')
    p.add_argument('--dry-run', action='store_true')
    p.add_argument('--check-existing', action='store_true', help='with --dry-run, compare against BigQuery')
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
