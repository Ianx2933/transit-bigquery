import json
import sys
import unittest
from pathlib import Path
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parent))
import od_typical_export as app  # noqa: E402


class Blob:
    def __init__(self, store, name):
        self.store, self.name = store, name

    def exists(self):
        return self.name in self.store.objects

    def download_as_bytes(self):
        return self.store.objects[self.name]

    def upload_from_string(self, data, content_type=None):
        self.store.objects[self.name] = data.encode() if isinstance(data, str) else data
        self.store.order.append(self.name)

    def delete(self):
        del self.store.objects[self.name]


class Storage:
    def __init__(self):
        self.objects, self.order = {}, []

    def bucket(self, name):
        return SimpleNamespace(blob=lambda n: Blob(self, n))

    def list_blobs(self, bucket, prefix=None):
        return [Blob(self, n) for n in sorted(self.objects) if n.startswith(prefix or '')]


class Rows(list):
    def result(self):
        return self


class BigQuery:
    def __init__(self, storage, rows=None):
        self.storage = storage
        self.rows = rows or [('weekday', 10, 123.5, '20260915', '20251014')]
        self.extracts = []

    def query(self, sql, job_config):
        return Rows(SimpleNamespace(items=lambda r=r: {'day_type': r[0], 'n': r[1], 'p': r[2],
                                                       'end_day': r[3], 'start_day': r[4]}.items())
                    for r in self.rows)

    def extract_table(self, table_id, dest, job_id, job_config, location):
        self.extracts.append(dest)
        name = dest[len('gs://b/'):].replace('*', '000000000000')
        self.storage.objects[name] = b'gz'
        self.storage.order.append(name)
        return SimpleNamespace(result=lambda: None)


BQ = SimpleNamespace(QueryJobConfig=lambda **kw: kw, ExtractJobConfig=lambda **kw: kw,
                     DestinationFormat=SimpleNamespace(CSV='CSV'), Compression=SimpleNamespace(GZIP='GZIP'))


def args(**kw):
    base = dict(project='p', dataset='d', out_uri='gs://b/od-typical', replace=False, dry_run=False)
    base.update(kw)
    return SimpleNamespace(**base)


class ExportTests(unittest.TestCase):
    def test_parts_then_manifest_in_the_window_end_folder(self):
        st = Storage()
        app.run(st, BigQuery(st), BQ, args())
        self.assertEqual(st.order[-1], 'od-typical/20260915/manifest.json')
        m = json.loads(st.objects['od-typical/20260915/manifest.json'])
        self.assertEqual(m['day_types'], {'weekday': {'rows': 10, 'passengers': 123.5}})
        self.assertEqual(m['parts'], ['part-000000000000.csv.gz'])

    def test_same_export_twice_does_nothing(self):
        st = Storage()
        bq = BigQuery(st)
        app.run(st, bq, BQ, args())
        app.run(st, bq, BQ, args())
        self.assertEqual(len(bq.extracts), 1)

    def test_different_numbers_need_replace(self):
        st = Storage()
        app.run(st, BigQuery(st), BQ, args())
        changed = BigQuery(st, [('weekday', 11, 130.0, '20260915', '20251014')])
        with self.assertRaises(ValueError):
            app.run(st, changed, BQ, args())
        app.run(st, changed, BQ, args(replace=True))
        m = json.loads(st.objects['od-typical/20260915/manifest.json'])
        self.assertEqual(m['day_types']['weekday']['rows'], 11)

    def test_day_types_must_share_one_window(self):
        st = Storage()
        bq = BigQuery(st, [('weekday', 1, 1.0, '20260915', '20251014'), ('saturday', 1, 1.0, '20260919', '20251014')])
        with self.assertRaises(ValueError):
            app.run(st, bq, BQ, args())

    def test_dry_run_writes_nothing(self):
        st = Storage()
        bq = BigQuery(st)
        app.run(st, bq, BQ, args(dry_run=True))
        self.assertEqual(st.objects, {})


if __name__ == '__main__':
    unittest.main()
