import gzip
import io
import json
import sys
import unittest
import zipfile
from pathlib import Path
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parent))
import od_dong_load as app  # noqa: E402

HEAD = '"기준일자","출발_구","출발_동","도착_구","도착_동","총_승객수","지하철_승객수","버스_승객수"\n'


def month_zip(ym, days, rows_per_day=2):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, 'w') as zf:
        for d in days:
            day = f'{ym}{d:02d}'
            body = HEAD + ''.join(f'"{day}","강남구","개포1동","강남구","대치{i}동","{10 + i}","{i}","{10}"\n'
                                  for i in range(rows_per_day))
            zf.writestr(f'동별/수단OD/동별_수단OD_{day}.csv', body.encode('cp949'))
        zf.writestr('노선별/노선별OD/x.csv', 'ignored')
    return buf.getvalue()


class Blob:
    def __init__(self, name, data):
        self.name, self.data = name, data

    def download_as_bytes(self):
        return self.data


class FakeStorage:
    def __init__(self, months):
        self.blobs = {}
        for ym, data in months.items():
            folder = f'od-raw/{ym[:4]}-{ym[4:]}'
            obj = f'{folder}/archive.zip'
            self.blobs[obj] = Blob(obj, data)
            self.blobs[f'{folder}/manifest.json'] = Blob(
                f'{folder}/manifest.json', json.dumps({'object': f'gs://b/{obj}'}).encode())

    def list_blobs(self, bucket, prefix=None):
        return [b for n, b in self.blobs.items() if not prefix or n.startswith(prefix)]

    def bucket(self, name):
        return SimpleNamespace(blob=lambda n: self.blobs[n])


class Rows(list):
    def result(self):
        return self


class FakeBigQuery:
    def __init__(self, drop_bus_on=None):
        self.rows = {}           # ym -> list of tuples
        self.loads = []
        self.tables = set()
        self.drop_bus_on = drop_bus_on

    def get_table(self, table_id):
        if table_id not in self.tables:
            raise type('NotFound', (Exception,), {})(table_id)

    def create_table(self, table):
        self.tables.add(table.table_id)

    def load_table_from_file(self, fileobj, target, job_id, job_config, location):
        ym = target.split('$')[1]
        assert job_config.write_disposition == 'WRITE_TRUNCATE'
        text = gzip.decompress(fileobj.read()).decode('utf-8').splitlines()[1:]
        rows = [line.split(',') for line in text]
        if self.drop_bus_on:
            rows = [r[:7] + ['0'] if r[0] == self.drop_bus_on else r for r in rows]
        self.rows[ym] = rows
        self.loads.append(ym)
        return SimpleNamespace(result=lambda: None)

    def query(self, sql, job_config):
        ym = sql.split("DATE '")[1][:7].replace('-', '')
        out = {}
        for r in self.rows.get(ym, []):
            n, t, s, b = out.get(r[0], (0, 0, 0, 0))
            out[r[0]] = (n + 1, t + int(r[5]), s + int(r[6]), b + int(r[7]))
        return Rows(SimpleNamespace(items=lambda d=d, v=v: {'day': d, 'n': v[0], 't': v[1], 's': v[2], 'b': v[3]}.items())
                    for d, v in out.items())


FAKE_BQ = SimpleNamespace(
    Table=lambda table_id, schema: SimpleNamespace(table_id=table_id, schema=schema),
    SchemaField=lambda n, t: (n, t),
    TimePartitioning=lambda type_, field: (type_, field),
    TimePartitioningType=SimpleNamespace(MONTH='MONTH'),
    LoadJobConfig=lambda **kw: SimpleNamespace(**kw),
    QueryJobConfig=lambda **kw: SimpleNamespace(**kw),
    SourceFormat=SimpleNamespace(CSV='CSV'),
    WriteDisposition=SimpleNamespace(WRITE_TRUNCATE='WRITE_TRUNCATE'),
)


def args(**kw):
    base = dict(raw_uri='gs://b/od-raw', project='p', dataset='d', only_ym=None, dry_run=False, check_existing=False)
    base.update(kw)
    return SimpleNamespace(**base)


class OdDongLoadTests(unittest.TestCase):
    def setUp(self):
        self.storage = FakeStorage({'202609': month_zip('202609', [15, 16]), '202608': month_zip('202608', [1])})

    def test_every_month_is_loaded_into_its_partition_and_checked(self):
        bq = FakeBigQuery()
        app.run(self.storage, bq, FAKE_BQ, args())
        self.assertEqual(sorted(bq.loads), ['202608', '202609'])
        self.assertEqual(sorted(r[0] for r in bq.rows['202609']), ['2026-09-15'] * 2 + ['2026-09-16'] * 2)
        self.assertEqual(bq.rows['202609'][0][1:5], ['강남구', '개포1동', '강남구', '대치0동'])

    def test_second_run_loads_nothing(self):
        bq = FakeBigQuery()
        app.run(self.storage, bq, FAKE_BQ, args())
        bq.loads.clear()
        app.run(self.storage, bq, FAKE_BQ, args())
        self.assertEqual(bq.loads, [])

    def test_only_ym_reloads_one_month(self):
        bq = FakeBigQuery()
        app.run(self.storage, bq, FAKE_BQ, args())
        bq.loads.clear()
        app.run(self.storage, bq, FAKE_BQ, args(only_ym='202609'))
        self.assertEqual(bq.loads, ['202609'])

    def test_a_difference_after_loading_stops_the_run(self):
        bq = FakeBigQuery(drop_bus_on='2026-09-16')
        with self.assertRaises(ValueError):
            app.run(self.storage, bq, FAKE_BQ, args())

    def test_dry_run_loads_nothing(self):
        bq = FakeBigQuery()
        app.run(self.storage, bq, FAKE_BQ, args(dry_run=True))
        self.assertEqual(bq.loads, [])

    def test_cp949_member_names_are_read(self):
        raw = '동별/수단OD/동별_수단OD_20260915.csv'.encode('cp949').decode('cp437')
        info = SimpleNamespace(flag_bits=0, filename=raw)
        self.assertEqual(app.member_name(info), '동별/수단OD/동별_수단OD_20260915.csv')

    def test_unexpected_header_stops(self):
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, 'w') as zf:
            zf.writestr('동별/수단OD/동별_수단OD_20260915.csv', 'a,b\n')
        with self.assertRaises(ValueError):
            app.convert_month(buf.getvalue(), '202609')


if __name__ == '__main__':
    unittest.main()
